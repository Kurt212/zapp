package zapp

import (
	"os"
	"time"

	"github.com/Kurt212/zapp/blob"
)

func (seg *segment) collectExpiredItemsLoop(
	tickDelay time.Duration,
) {
	ticker := time.NewTicker(tickDelay)

	for {
		select {
		case <-ticker.C:
			seg.collectExpiredItems() // TODO add logging of error
		case <-seg.closedChan:
			ticker.Stop()
			return
		}
	}
}

func (seg *segment) collectExpiredItems() error {
	// here visit all items on disk
	// check their TTL
	// in case an item is expired mark delete it from inmemory state

	seg.mtx.Lock()
	defer seg.mtx.Unlock()

	now := time.Now()

	visitorFunc := func(file *os.File, currentOffset int64, blobHeader blob.Header) error {
		if !blobHeader.IsExpired(now) {
			return nil
		}

		// In case item is expired, need to read key from disk to calculate hash
		// and find item in inmemory segment's state

		blobSize := blobHeader.Size()

		// blob size is sum of header size and body size
		bodySize := blobSize - blob.HeaderSize

		// read blob's body from disk
		blobBodyBuffer := make([]byte, bodySize)

		_, err := file.ReadAt(blobBodyBuffer, currentOffset+blob.HeaderSize)
		if err != nil {
			return err
		}

		kve := blob.UnmarshalBody(blobBodyBuffer, blobHeader)

		keyHash := hash(kve.Key)

		offsetInfo := offsetMetaInfo{
			offset: currentOffset,
			size:   blobSize,
		}

		// find the item by hash in inmemory state and mark it as empty offset
		seg.rawDeleteOffsetFromMemory(keyHash, offsetInfo)

		return nil
	}

	_, err := seg.visitOnDiskItems(visitorFunc)
	if err != nil {
		return err
	}

	return nil
}
