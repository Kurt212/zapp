package zapp

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Kurt212/zapp/blob"
)

type segment struct {
	file               *os.File
	mtx                sync.Mutex
	hashToOffsetMap    map[uint32][]offsetMetaInfo
	emptySizeToOffsets map[int][]int64
}

type offsetMetaInfo struct {
	offset int64 // at which offset in segment's file data is located
	size   int   // the length of data in current offset in bytes
}

func newSegment(path string) (*segment, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("can not create segment %s: %w", path, err)
	}

	seg := &segment{
		file:               file,
		mtx:                sync.Mutex{},
		hashToOffsetMap:    make(map[uint32][]offsetMetaInfo),
		emptySizeToOffsets: make(map[int][]int64),
	}

	return seg, nil
}

func (seg *segment) Set(hash uint32, key []byte, value []byte) error {
	return seg.SetExpire(hash, key, value, 0)
}

func (seg *segment) SetExpire(hash uint32, key []byte, value []byte, ttl time.Duration) error {
	seg.mtx.Lock()
	defer seg.mtx.Unlock()

	// transfer duration to timestamp only if it's not empty
	expire := uint32(0)
	if ttl.Milliseconds() > 0 {
		expire = uint32(time.Now().Add(ttl).UnixMilli())
	}

	kve := blob.KVE{
		Key:    key,
		Value:  value,
		Expire: expire,
	}

	// first try to find if there is this key already set
	// if found same existing key, delete old one and mark its disk space as empty

	duplicateKeyIdx := -1

	offsetsWithCurrentHash, ok := seg.hashToOffsetMap[hash]
	if ok {
		for idx, offsetInfo := range offsetsWithCurrentHash {
			// TODO redo read key more effectively
			dataBuffer := make([]byte, offsetInfo.size)

			_, err := seg.file.ReadAt(dataBuffer, offsetInfo.offset)
			if err != nil {
				return err // TODO wrap
			}

			kveOnDisk := blob.Unmarshal(dataBuffer)
			onDiskKey := kveOnDisk.Key

			// if found previous blob of current key
			// then mark if as deleted
			if bytes.Equal(key, onDiskKey) {
				duplicateKeyIdx = idx

				// TODO write on disk that data is deleted
				// ...

				// Add this offset to list of free empty offsets
				emptyOffsets, _ := seg.emptySizeToOffsets[offsetInfo.size]

				emptyOffsets = append(emptyOffsets, offsetInfo.offset)

				seg.emptySizeToOffsets[offsetInfo.size] = emptyOffsets
				// now as duplicate index is found it's ok to stop the for-loop
				break
			}
		}

		// if duplicate was found, then delete it from list of offsets for the same hash
		if duplicateKeyIdx > 0 {
			// TODO move to a function or smth. Make it more clear

			// Just replace duplicate with the last item and then crop the slice
			currentLength := len(offsetsWithCurrentHash)

			offsetsWithCurrentHash[duplicateKeyIdx] = offsetsWithCurrentHash[currentLength-1]

			offsetsWithCurrentHash = offsetsWithCurrentHash[:currentLength-1]

			seg.hashToOffsetMap[hash] = offsetsWithCurrentHash
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

		// TODO also make if more understandable
		emptyOffsets[0] = emptyOffsets[len(emptyOffsets)-1]

		emptyOffsets = emptyOffsets[:len(emptyOffsets)-1]

		seg.emptySizeToOffsets[sizeOfBlob] = emptyOffsets
	}

	if offset == 0 {
		var err error
		offset, err = seg.file.Seek(0, EndWhence)
		if err != nil {
			return err // TODO wrap
		}
	}

	_, err := seg.file.WriteAt(binaryBlob, offset)
	if err != nil {
		return err // TODO wrap
	}

	// modify seg.hashToOffsetMap map and save new offset for current hash
	offsetsWithCurrentHash = append(offsetsWithCurrentHash, offsetMetaInfo{
		offset: offset,
		size:   sizeOfBlob,
	})

	seg.hashToOffsetMap[hash] = offsetsWithCurrentHash

	return nil
}

func (seg *segment) Get(hash uint32, key []byte) ([]byte, error) {
	seg.mtx.Lock()
	defer seg.mtx.Unlock()

	offsetsWithCurrentHash, ok := seg.hashToOffsetMap[hash]
	if ok {
		for _, offsetInfo := range offsetsWithCurrentHash {
			// TODO redo read key more effectively
			dataBuffer := make([]byte, offsetInfo.size)

			_, err := seg.file.ReadAt(dataBuffer, offsetInfo.offset)
			if err != nil {
				return nil, err // TODO wrap
			}

			kveOnDisk := blob.Unmarshal(dataBuffer)
			onDiskKey := kveOnDisk.Key

			// if found previous blob of current key
			// then mark if as deleted
			if bytes.Equal(key, onDiskKey) {
				// TODO check expire
				// ...
				return kveOnDisk.Value, nil
			}
		}
	}

	return nil, ErrNotFound
}

func (seg *segment) Delete(hash uint32, key []byte) error {
	return errors.New("not implemented")
}