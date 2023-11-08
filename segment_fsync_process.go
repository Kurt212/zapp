package zapp

import (
	"fmt"
	"time"
)

func (s *segment) fsyncLoop(syncInterval time.Duration) {
	// zero interval means user wants not to have any sync process at all
	if syncInterval == 0 {
		return
	}

	ticker := time.NewTicker(syncInterval)

	for {
		select {
		case <-ticker.C:
			s.fsync()
		case <-s.closedChan:
			return
		}
	}
}

// fsync runs the process of persisting segment's file to disk
// The generic problem of all databases is that raw writing to underlying hardware is too expensive.
// OS buffers file changes implicitly and asynchronosly transfers the buffer to the hardware.
// Zapp uses Write Ahead Logging (WAL) to achieve consistency and durability.
// Each Write to data generates a new entry, which is appended to WAL and persisted to real disk hardware synchronosly.
// Segment's file contains the Recent Log Sequence Number (LSN) at the beginning header, which refers to one of the real existing WAL entries.
// Periodically segments file needs to be persisted to the hardware explicitly so that it is guaranteed, that a new checkpoint in WAL file can be created.
// Once the segment's file is persisted, the WAL file may be truncated because it's safe to loose actios, which are persisted to disk in segments.
// The process of safe truncation of the WAL file is called "checkpoint creation".
func (s *segment) fsync() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.rawFsync()
}

func (s *segment) rawFsync() {
	err := s.file.Sync()
	if err != nil {
		panic(fmt.Errorf("tried to fsync segment's file, but got error: %w", err))
	}

	// we support working without WAL at all, so this is okay
	if s.wal != nil {
		err = s.wal.Checkpoint()
		if err != nil {
			panic(fmt.Errorf("can not create new checkpoint in WAL: %w", err))
		}
	}
}
