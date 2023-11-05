package zapp

import (
	"time"
)

func (seg *segment) collectExpiredItemsLoop(
	tickDelay time.Duration,
) {
	ticker := time.NewTicker(tickDelay)

	for {
		select {
		case <-ticker.C:
			seg.collectExpiredItems()
		case <-seg.closedChan:
			ticker.Stop()
			return
		}
	}
}

func (seg *segment) collectExpiredItems() {
	// here visit all items on disk
	// check their TTL
	// in case an item is expired mark delete it from inmemory state

	seg.mtx.Lock()
	defer seg.mtx.Unlock()

	now := time.Now()

	for hash, offsets := range seg.hashToOffsetMap {
		for _, offsetInfo := range offsets {
			if !offsetInfo.IsExpired(now) {
				continue
			}

			// find the item by hash in inmemory state and mark it as empty offset
			seg.rawDeleteOffsetFromMemory(hash, offsetInfo)
		}
	}
}
