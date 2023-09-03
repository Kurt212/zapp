package zapp

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/Kurt212/zapp/blob"
)

const (
	segmentFileLayoutVerion1 = 1

	segmentFileMagicNumbersSize   = 3 // bytes
	segmentFileLayoutSize         = 1 // byte
	segmentFileLayoutReservedSize = 12

	segmentFileHeaderSize = segmentFileMagicNumbersSize + segmentFileLayoutSize + segmentFileLayoutReservedSize
)

var (
	segmentFileBeginMagicNumbers = []byte{212, 211, 212}
)

type segment struct {
	file          *os.File
	fileSizeBytes int64

	mtx                sync.RWMutex
	hashToOffsetMap    map[uint32][]offsetMetaInfo
	emptySizeToOffsets map[int][]int64
	closedChan         chan struct{}
}

type offsetMetaInfo struct {
	offset int64 // at which offset in segment's file data is located
	size   int   // the length of data in current offset in bytes
}

func newSegment(
	path string,
	collectExpiredItemsPeriod time.Duration,
) (*segment, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("can not open file %s: %w", path, err)
	}

	seg := &segment{
		file:               file,
		mtx:                sync.RWMutex{},
		hashToOffsetMap:    make(map[uint32][]offsetMetaInfo),
		emptySizeToOffsets: make(map[int][]int64),
		closedChan:         make(chan struct{}),
	}

	// read whole file and make fill hash to offset map and empty size to offset map
	err = seg.loadDataFromDisk()
	if err != nil {
		return nil, fmt.Errorf("can not load data from disk: %w", err)
	}

	go seg.collectExpiredItemsLoop(collectExpiredItemsPeriod)

	return seg, nil
}

func (seg *segment) loadDataFromDisk() error {
	file := seg.file

	// move file cursor to the beginning of the file
	fileBeginOffset, err := file.Seek(0, OriginWhence)
	if err != nil {
		return err
	}

	// read segment file header and validate it
	fileHeaderBuffer := make([]byte, segmentFileHeaderSize)

	_, err = file.Read(fileHeaderBuffer)
	// this is okay because this may be an new file without any header at all
	// write header to the disk and stop loading
	if err == io.EOF {
		// reuse same buffer to write file header to the new created file
		copy(fileHeaderBuffer, segmentFileBeginMagicNumbers)
		fileHeaderBuffer[segmentFileMagicNumbersSize] = segmentFileLayoutVerion1

		_, err = file.WriteAt(fileHeaderBuffer, fileBeginOffset)
		if err != nil {
			return err
		}

		seg.fileSizeBytes = segmentFileHeaderSize

		return nil
	}
	// if this is not EOF, then trigger error
	if err != nil {
		return err
	}

	// parse header from the buffer
	fileMagicNumbers := fileHeaderBuffer[0:segmentFileMagicNumbersSize]

	if !bytes.Equal(fileMagicNumbers, segmentFileBeginMagicNumbers) {
		return ErrSegmentMagicNumbersDoNotMatch
	}

	fileVersion := fileHeaderBuffer[segmentFileMagicNumbersSize]
	if fileVersion != segmentFileLayoutVerion1 {
		return ErrSegmentUnknownVersionNumber
	}

	now := time.Now() // to check the expire fields of the items

	visitorFunc := func(file *os.File, currentOffset int64, blobHeader blob.Header) error {
		blobSize := blobHeader.Size()

		switch {
		// If meet an expired blob, then treat it as a deleted blob.
		// On disk it will remain expired until someone overwrites it
		case blobHeader.IsExpired(now):
			fallthrough
		case blobHeader.Status == blob.StatusDeleted:
			// this is an empty blob, so just save it to empty sizes map
			offsetsSlice := seg.emptySizeToOffsets[blobSize]

			offsetsSlice = append(offsetsSlice, currentOffset)

			seg.emptySizeToOffsets[blobSize] = offsetsSlice

		case blobHeader.Status == blob.StatusOK:
			// blob size is sum of header size and body size
			bodySize := blobSize - blob.HeaderSize

			// read blob's body from disk
			blobBodyBuffer := make([]byte, bodySize)

			_, err := file.ReadAt(blobBodyBuffer, currentOffset+blob.HeaderSize)
			if err != nil {
				return err
			}

			kve := blob.UnmarshalBody(blobBodyBuffer, blobHeader)

			// calculate hash from key and store data about this blob in hash to offset map
			keyHash := hash(kve.Key)

			hashOffsets := seg.hashToOffsetMap[keyHash]

			hashOffsets = append(hashOffsets, offsetMetaInfo{
				offset: currentOffset,
				size:   blobSize,
			})

			seg.hashToOffsetMap[keyHash] = hashOffsets
		default:
			panic(ErrUnknownBlobStatus)
		}
		return nil
	}

	var lastOffset int64
	lastOffset, err = seg.visitOnDiskItems(visitorFunc)
	if err != nil {
		return fmt.Errorf("got error when restoring state from disk: %w", err)
	}

	seg.fileSizeBytes = lastOffset

	return nil
}

func (seg *segment) Set(hash uint32, key []byte, value []byte, ttl time.Duration) error {
	seg.mtx.Lock()
	defer seg.mtx.Unlock()

	// transfer duration to timestamp only if it's not empty
	expire := uint32(0)
	if ttl.Milliseconds() > 0 {
		expire = uint32(time.Now().Add(ttl).Unix())
	}

	kve := blob.KVE{
		Key:    key,
		Value:  value,
		Expire: expire,
	}

	// first try to find if there is this key already set
	// if found same existing key, delete old one and mark its disk space as empty
	offsetsWithCurrentHash, ok := seg.hashToOffsetMap[hash]
	if ok {
		for _, offsetInfo := range offsetsWithCurrentHash {
			// TODO redo read key more effectively
			// Don't know how for now. Not sure that two disk reads are better than on big read
			dataBuffer := make([]byte, offsetInfo.size)

			_, err := seg.file.ReadAt(dataBuffer, offsetInfo.offset)
			if err != nil {
				return err // TODO wrap
			}

			kveOnDisk := blob.Unmarshal(dataBuffer)
			onDiskKey := kveOnDisk.Key

			// if found previous blob of current key
			// then mark if as deleted
			// because we are replacing it with a new value now
			if bytes.Equal(key, onDiskKey) {
				// write on disk that data is deleted
				deletedStatusByte := []byte{blob.StatusDeleted}

				// TODO make sure that if something is broken during this write then nothing bad will happen
				_, err := seg.file.WriteAt(deletedStatusByte, offsetInfo.offset+blob.StatusOffset)
				if err != nil {
					return err // TODO wrap
				}

				// Add this offset to list of free empty offsets and delete from hash to offset map
				seg.rawDeleteOffsetFromMemory(hash, offsetInfo)

				// now as duplicate index is found it's ok to stop the for-loop
				break
			}
		}
	}

	// now try to find suitable offset for new data
	// first check the empty offsets and try to find offset with the same size
	// if can not find empty offset, then append at the end of the file

	// marshal the data into one solid binary blob
	binaryBlob, sizeOfBlob := kve.Marshal()

	var offset int64 = 0

	emptyOffsets, ok := seg.emptySizeToOffsets[sizeOfBlob]
	if ok && len(emptyOffsets) > 0 {
		offset = emptyOffsets[0]

		// Swap the first value with the last value. And decrement slice size by 1.
		// This is a cheap way to delete item from slice without O(N) operation
		// TODO also make it more understandable
		emptyOffsets[0] = emptyOffsets[len(emptyOffsets)-1]

		emptyOffsets = emptyOffsets[:len(emptyOffsets)-1]

		seg.emptySizeToOffsets[sizeOfBlob] = emptyOffsets
	}

	if offset == 0 {
		offset = seg.fileSizeBytes
	}

	_, err := seg.file.WriteAt(binaryBlob, offset)
	if err != nil {
		return err // TODO wrap
	}

	seg.fileSizeBytes += int64(sizeOfBlob)

	// modify seg.hashToOffsetMap map and save new offset for current hash
	offsetsWithCurrentHash = append(offsetsWithCurrentHash, offsetMetaInfo{
		offset: offset,
		size:   sizeOfBlob,
	})

	seg.hashToOffsetMap[hash] = offsetsWithCurrentHash

	return nil
}

func (seg *segment) Get(hash uint32, key []byte) ([]byte, error) {
	// read lock here to increate Get speed. There's no option to modify any data here, only read it
	// for example can not delete expired item here and add it to empty map. Adding to emty map requires
	// releasing read lock and then taking write lock. Because of the data race between those two operations
	// need to revalidate if the item still exists and it's still expired
	seg.mtx.RLock()
	defer seg.mtx.RUnlock()

	offsetsWithCurrentHash, ok := seg.hashToOffsetMap[hash]
	if !ok {
		return nil, ErrNotFound
	}

	now := time.Now()

	for _, offsetInfo := range offsetsWithCurrentHash {
		// TODO redo read key more effectively
		dataBuffer := make([]byte, offsetInfo.size)

		_, err := seg.file.ReadAt(dataBuffer, offsetInfo.offset)
		if err != nil {
			return nil, err // TODO wrap
		}

		kveOnDisk := blob.Unmarshal(dataBuffer)
		onDiskKey := kveOnDisk.Key

		// if met the same key, then this is the value, which should be returned
		// the only problem is that the item may be expired, but it's still on disk
		if bytes.Equal(key, onDiskKey) {
			// must check if key is expired now. Then pretend that we didn't see it and return NotFound
			// Later backgroud routine, which deletes all expired keys will clean it and add to empty map
			if kveOnDisk.IsExpired(now) {
				return nil, ErrNotFound
			}

			return kveOnDisk.Value, nil
		}
	}

	return nil, ErrNotFound
}

func (seg *segment) Delete(hash uint32, key []byte) error {
	seg.mtx.Lock()
	defer seg.mtx.Unlock()

	offsetsWithCurrentHash, ok := seg.hashToOffsetMap[hash]

	if !ok {
		return ErrNotFound
	}

	itemIdx := -1
	var itemOffsetInfo offsetMetaInfo

	for idx, offsetInfo := range offsetsWithCurrentHash {
		// TODO redo read key more effectively
		// Don't know how for now. Not sure that two disk reads are better than on big read
		dataBuffer := make([]byte, offsetInfo.size)

		_, err := seg.file.ReadAt(dataBuffer, offsetInfo.offset)
		if err != nil {
			return err // TODO wrap
		}

		kveOnDisk := blob.Unmarshal(dataBuffer)
		onDiskKey := kveOnDisk.Key

		// if found previous blob of current key
		// then mark if as deleted
		// because we are replacing it with a new value now
		if bytes.Equal(key, onDiskKey) {
			itemIdx = idx
			itemOffsetInfo = offsetInfo

			// now as duplicate index is found it's ok to stop the for-loop
			break
		}
	}

	if itemIdx == -1 {
		return ErrNotFound
	}

	// write on disk that data is deleted
	deletedStatusByte := []byte{blob.StatusDeleted}

	// TODO make sure that if something is broken during this write then norhing bad will happen
	_, err := seg.file.WriteAt(deletedStatusByte, itemOffsetInfo.offset+blob.StatusOffset)
	if err != nil {
		return err // TODO wrap
	}

	seg.rawDeleteOffsetFromMemory(hash, itemOffsetInfo)

	return nil
}

// rawDeleteOffsetFromMemory removes offset from offset map and adds this offset to empty map
func (seg *segment) rawDeleteOffsetFromMemory(
	hash uint32, offsetInfo offsetMetaInfo,
) {
	// first find this offset in hashToOffsetMap
	offsetsWithCurrentHash, ok := seg.hashToOffsetMap[hash]
	// if there's no any offset with such hash, then do nothing
	if !ok {
		return
	}

	itemIdx := -1
	for idx, otherOffsetInfo := range offsetsWithCurrentHash {
		if otherOffsetInfo.offset == offsetInfo.offset && otherOffsetInfo.size == offsetInfo.size {
			itemIdx = idx
		}
	}

	// if didn't find this offset with such size in the list of hash offsets, then do nothing
	if itemIdx == -1 {
		return
	}

	// Add this offset to list of free empty offsets
	emptyOffsets := seg.emptySizeToOffsets[offsetInfo.size]

	emptyOffsets = append(emptyOffsets, offsetInfo.offset)

	seg.emptySizeToOffsets[offsetInfo.size] = emptyOffsets

	// Just replace duplicate with the last item and then crop the slice
	currentLength := len(offsetsWithCurrentHash)

	offsetsWithCurrentHash[itemIdx] = offsetsWithCurrentHash[currentLength-1]

	offsetsWithCurrentHash = offsetsWithCurrentHash[:currentLength-1]

	seg.hashToOffsetMap[hash] = offsetsWithCurrentHash
}

func (seg *segment) Close() error {
	seg.mtx.Lock()
	defer seg.mtx.Unlock()

	close(seg.closedChan)

	err := seg.file.Sync()
	if err != nil {
		return err
	}

	err = seg.file.Close()
	if err != nil {
		return err
	}

	return err
}
