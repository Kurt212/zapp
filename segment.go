package zapp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/Kurt212/zapp/blob"
	"github.com/Kurt212/zapp/constants"
	"github.com/Kurt212/zapp/wal"
)

const (
	segmentFileLayoutVerion1       = 1
	segmentFileDefaultLastKnownLSN = 0

	segmentFileMagicNumbersSize   = 3  // bytes
	segmentFileLayoutSize         = 1  // byte
	segmentFileLayoutReservedSize = 12 // bytes
	segmentFileLastKnownLSNSize   = 8  // bytes

	segmentFileLastKnownLSNOffset = segmentFileMagicNumbersSize + segmentFileLayoutSize + segmentFileLayoutReservedSize

	segmentFileHeaderSize = segmentFileMagicNumbersSize + segmentFileLayoutSize + segmentFileLayoutReservedSize + segmentFileLastKnownLSNSize
)

var (
	segmentFileBeginMagicNumbers = []byte{212, 211, 212}
)

type segment struct {
	file          *os.File // used to store segment's items data on disk
	fileSizeBytes int64    // internally count file's size to generate a valid offset for new item if there's no empty offset already existing

	mtx                sync.RWMutex              // mutex is used globally to access this segment. Each operation on segment needs locking. Read operations acquire read lock, write operation acquire write lock
	hashToOffsetMap    map[uint32][]itemMetaInfo // this is a list of items with the same hash value. Hash collisions sometimes happen and it's needed to deal with them. Although collisions happen quite not often
	emptySizeToOffsets map[int][]int64           // this is a list of known empty offset of certain sizes. When key is deleted or expired, its offset will be reused later to store new data. That's why segment tracks all empty offsets
	closedChan         chan struct{}             // this is a generic technic to notify each subprocess assosiated with this segment, that it must be terminated gracefully, because segment is closed and is no longer serving requests

	wal          *wal.W // optional. wal is an object to work with write ahead log, generate new log entries and get log sequence numbers (LSNs). User may not want to work with WAL and increase write-operations throughput.
	lastKnownLSN uint64 // lastKnownLSN is the last known wal's LSN appliend to this segment
}

type itemMetaInfo struct {
	// TODO make meta info more compact. Try to store offset, size and expire time in a single int64 or so.
	// Storing inmemory meta data about on-disk items is necessary.
	// But segment should try to waste as little RAM as possible so that it can store more keys inside
	offset     int64  // at which offset in segment's file data is located
	size       int    // the length of data in current offset in bytes
	expireTime uint32 // the time at which this offset no longer must be considered valid. now >= expireTime => item is invalid
}

func (i itemMetaInfo) IsExpired(now time.Time) bool {
	if i.expireTime == 0 {
		return false
	}
	return now.Unix() >= int64(i.expireTime)
}

func newSegment(
	dataFile *os.File,
	walFile *os.File,
	collectExpiredItemsPeriod time.Duration,
	syncFileDuration time.Duration,
) (*segment, error) {
	seg := &segment{
		file:               dataFile,
		mtx:                sync.RWMutex{},
		hashToOffsetMap:    make(map[uint32][]itemMetaInfo),
		emptySizeToOffsets: make(map[int][]int64),
		closedChan:         make(chan struct{}),
		wal:                nil, // wal will be initiated after reading file from disk
	}

	// read whole file and make fill hash to offset map and empty size to offset map
	// also reads lastKnownLSN from file
	err := seg.loadDataFromDisk()
	if err != nil {
		return nil, fmt.Errorf("can not load data from disk: %w", err)
	}

	if walFile != nil {
		walManager, unaplliedActions, err := wal.CreateWalAndReturnNotAppliedActions(walFile, seg.lastKnownLSN)
		if err != nil {
			return nil, err
		}

		seg.wal = walManager

		if len(unaplliedActions) > 0 {
			err = seg.performUnappliedWALActions(unaplliedActions)
			if err != nil {
				return nil, fmt.Errorf("got error when performing all unapplied actions from wal file: %w", err)
			}

			// after re applying actions from wal,
			// need to run fsync to make a new checkpoint
			// and sync segment's dirty file to disk
			seg.rawFsync()
		}
	}

	if syncFileDuration > 0 {
		go seg.fsyncLoop(syncFileDuration)
	}

	if collectExpiredItemsPeriod > 0 {
		go seg.collectExpiredItemsLoop(collectExpiredItemsPeriod)
	}

	return seg, nil
}

// loadDataFromDisk reads whole on disk file and restores in memory state
func (seg *segment) loadDataFromDisk() error {
	file := seg.file

	// move file cursor to the beginning of the file
	fileBeginOffset, err := file.Seek(0, constants.OriginWhence)
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

		lastLSNBuffer := make([]byte, 0, segmentFileLastKnownLSNSize)
		lastLSNBuffer = binary.BigEndian.AppendUint64(lastLSNBuffer, segmentFileDefaultLastKnownLSN)

		copy(fileHeaderBuffer[segmentFileLastKnownLSNOffset:], lastLSNBuffer)

		_, err = file.WriteAt(fileHeaderBuffer, fileBeginOffset)
		if err != nil {
			return err
		}

		seg.fileSizeBytes = segmentFileHeaderSize
		seg.lastKnownLSN = segmentFileDefaultLastKnownLSN

		return nil
	}
	// if this is not EOF, then trigger error
	if err != nil {
		return err
	}

	// parse header from the buffer
	fileMagicNumbers := fileHeaderBuffer[:segmentFileMagicNumbersSize]

	if !bytes.Equal(fileMagicNumbers, segmentFileBeginMagicNumbers) {
		return ErrSegmentMagicNumbersDoNotMatch
	}

	fileVersion := fileHeaderBuffer[segmentFileMagicNumbersSize]
	if fileVersion != segmentFileLayoutVerion1 {
		return ErrSegmentUnknownVersionNumber
	}
	// here may be some other reads for data from reserved bytes in header

	// initialize lastKnownLSN from header
	lastKnownLSNBuffer := fileHeaderBuffer[segmentFileLastKnownLSNOffset:]
	seg.lastKnownLSN = binary.BigEndian.Uint64(lastKnownLSNBuffer)

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

			hashOffsets = append(hashOffsets, itemMetaInfo{
				offset:     currentOffset,
				size:       blobSize,
				expireTime: blobHeader.Expire,
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

func (seg *segment) Set(hash uint32, key []byte, value []byte, expire uint32) error {
	seg.mtx.Lock()
	defer seg.mtx.Unlock()

	// if wal manager field is nil, then do nothing with the WAL logic and work without it
	// this increases performace dramatically
	if seg.wal != nil {
		lsn, err := seg.wal.AppendSet(key, value, expire)
		if err != nil {
			panic(fmt.Errorf("got error when append set action to WAL: %w", err))
		}

		err = seg.rawWriteLastKnownLSN(lsn)
		if err != nil {
			panic(fmt.Errorf("got error when trying to write last known LSN %d to segment: %w", lsn, err))
		}
	}

	return seg.rawSet(hash, key, value, expire)
}
func (seg *segment) rawSet(hash uint32, key []byte, value []byte, expire uint32) error {
	// convert duration to timestamp only if it's not empty
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
				panic(fmt.Errorf(
					"tried to read item's data at offset %d but got error: %w",
					offsetInfo.offset, err,
				))
			}

			kveOnDisk := blob.Unmarshal(dataBuffer)
			onDiskKey := kveOnDisk.Key

			// if found previous blob of current key
			// then mark if as deleted
			// because we are replacing it with a new value now
			if bytes.Equal(key, onDiskKey) {
				// write on disk that data is deleted
				deletedStatusByte := []byte{blob.StatusDeleted}

				_, err := seg.file.WriteAt(deletedStatusByte, offsetInfo.offset+blob.StatusOffset)
				if err != nil {
					panic(fmt.Errorf(
						"tried to write delete status at offset %d but got error: %w",
						offsetInfo.offset+blob.StatusOffset,
						err,
					))
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
		if len(emptyOffsets) > 1 {
			emptyOffsets[0] = emptyOffsets[len(emptyOffsets)-1]
			emptyOffsets = emptyOffsets[:len(emptyOffsets)-1]
		} else {
			emptyOffsets = emptyOffsets[:0]
		}

		seg.emptySizeToOffsets[sizeOfBlob] = emptyOffsets
	}

	appendAtTheEnd := false
	if offset == 0 {
		offset = seg.fileSizeBytes
		appendAtTheEnd = true
	}

	_, err := seg.file.WriteAt(binaryBlob, offset)
	if err != nil {
		panic(fmt.Errorf(
			"tried to write new item's blob at offset %d but got error: %w",
			offset,
			err,
		))
	}

	if appendAtTheEnd {
		seg.fileSizeBytes += int64(sizeOfBlob)
	}

	// could be modified since last retrieval so obtain it one more time
	offsetsWithCurrentHash, ok = seg.hashToOffsetMap[hash]
	if !ok {
		offsetsWithCurrentHash = nil
	}

	// modify seg.hashToOffsetMap map and save new offset for current hash
	offsetsWithCurrentHash = append(offsetsWithCurrentHash, itemMetaInfo{
		offset:     offset,
		size:       sizeOfBlob,
		expireTime: expire,
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

	return seg.rawGet(hash, key)
}

func (seg *segment) rawGet(hash uint32, key []byte) ([]byte, error) {
	offsetsWithCurrentHash, ok := seg.hashToOffsetMap[hash]
	if !ok {
		return nil, ErrNotFound
	}

	now := time.Now()

	for _, offsetInfo := range offsetsWithCurrentHash {
		// if expired then do not try to read it from disk
		if offsetInfo.IsExpired(now) {
			continue
		}

		// TODO redo read key more effectively
		dataBuffer := make([]byte, offsetInfo.size)

		_, err := seg.file.ReadAt(dataBuffer, offsetInfo.offset)
		if err != nil {
			panic(fmt.Errorf(
				"tried to read item's data at offset %d but got error: %w",
				offsetInfo.offset,
				err,
			))
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

	// if wal manager field is nil, then do nothing with the WAL logic and work without it
	// this increases performace dramatically
	if seg.wal != nil {
		lsn, err := seg.wal.AppendDel(key)
		if err != nil {
			panic(fmt.Errorf("got error when append del action to WAL: %w", err))
		}

		err = seg.rawWriteLastKnownLSN(lsn)
		if err != nil {
			panic(fmt.Errorf("got error when trying to write last known LSN %d to segment: %w", lsn, err))
		}
	}

	return seg.rawDelete(hash, key)
}

func (seg *segment) rawDelete(hash uint32, key []byte) error {
	offsetsWithCurrentHash, ok := seg.hashToOffsetMap[hash]
	if !ok {
		return ErrNotFound
	}

	itemIdx := -1
	var itemOffsetInfo itemMetaInfo

	now := time.Now()

	for idx, offsetInfo := range offsetsWithCurrentHash {
		// if expired then do not try to read it from disk
		if offsetInfo.IsExpired(now) {
			continue
		}

		// TODO redo read key more effectively
		// Don't know how for now. Not sure that two disk reads are better than on big read
		dataBuffer := make([]byte, offsetInfo.size)

		_, err := seg.file.ReadAt(dataBuffer, offsetInfo.offset)
		if err != nil {
			panic(fmt.Errorf(
				"tried to read item's data at offset %d but got error: %w",
				offsetInfo.offset,
				err,
			))
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

	_, err := seg.file.WriteAt(deletedStatusByte, itemOffsetInfo.offset+blob.StatusOffset)
	if err != nil {
		panic(fmt.Errorf(
			"tried to write deleted status at offset %d but got error: %w",
			itemOffsetInfo.offset+blob.StatusOffset,
			err,
		))
	}

	seg.rawDeleteOffsetFromMemory(hash, itemOffsetInfo)

	return nil
}

// rawDeleteOffsetFromMemory removes offset from offset map and adds this offset to empty map
func (seg *segment) rawDeleteOffsetFromMemory(
	hash uint32, offsetInfo itemMetaInfo,
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

	if len(offsetsWithCurrentHash) > 1 {
		offsetsWithCurrentHash[itemIdx] = offsetsWithCurrentHash[currentLength-1]
		offsetsWithCurrentHash = offsetsWithCurrentHash[:currentLength-1]
	} else {
		offsetsWithCurrentHash = offsetsWithCurrentHash[:0]
	}

	seg.hashToOffsetMap[hash] = offsetsWithCurrentHash
}

func (seg *segment) Close() error {
	seg.mtx.Lock()
	defer seg.mtx.Unlock()

	seg.rawFsync()

	close(seg.closedChan)

	err := seg.file.Close()
	if err != nil {
		panic(fmt.Errorf(
			"tried to close segment's file when closing segment, but got error: %w",
			err,
		))
	}

	return nil
}

func (seg *segment) rawWriteLastKnownLSN(lastKnownLSN uint64) error {
	var buffer []byte
	buffer = binary.BigEndian.AppendUint64(buffer, lastKnownLSN)

	_, err := seg.file.WriteAt(buffer, segmentFileLastKnownLSNOffset)
	if err != nil {
		return fmt.Errorf("got error when writing last known lasn to segment's file: %w", err)
	}

	seg.lastKnownLSN = lastKnownLSN

	return nil
}
