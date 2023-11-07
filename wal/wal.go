package wal

import (
	"fmt"
	"os"
	"sync"
)

type W struct {
	file    *os.File // represent the persistent file used to store wal data
	lastLSN uint64   // last known LSN in this log file. Used to generate next LSN

	lock sync.Mutex // needed to work with WAL file, to avoid LSN generation and file appending data races and
}

type Action struct {
	Type   ActionType
	LSN    uint64
	Key    []byte
	Value  []byte // optional
	Expire uint32 // optional. 0 is default and means no expire time
}

type ActionType byte

const (
	ActionTypeUnknown ActionType = iota
	ActionTypeSet
	ActionTypeDel
)

func CreateWalAndReturnNotAppliedActions(file *os.File, lastAppliedLSN uint64) (*W, []Action, error) {
	w := &W{
		file: file,
		// real unknown yet. But we will read whole WAL file and find out.
		// For now use lastAppliedLSN as the lowerbound.
		// WAL file may be fully empty and in this case it means, that programm was closed right after checkpoint.
		// Or this is the firt time creating WAL file and segment and there's no existing previous recent LSN
		lastLSN: lastAppliedLSN,
	}

	actions, lastLSN, err := initialRead(file, lastAppliedLSN)
	if err != nil {
		return nil, nil, fmt.Errorf("got error when initial reading wal file: %w", err)
	}

	w.lastLSN = lastLSN

	return w, actions, nil
}

// Checkpoint for now checkpointing WAL means it's safe to delete all entries from the file
// Checkpoint mush be called only after the segment's file is persisted to the disk fully
// Otherwise some data changes can be lost in case of software or hardware faults
func (w *W) Checkpoint() error {
	err := w.file.Truncate(0)
	if err != nil {
		return err
	}

	return nil
}

func (w *W) AppendSet(key []byte, value []byte, expire uint32) (uint64, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.lastLSN++

	lsn := w.lastLSN

	action := Action{
		LSN:    lsn,
		Type:   ActionTypeSet,
		Key:    key,
		Value:  value,
		Expire: expire,
	}

	err := AppendAction(w.file, action)
	if err != nil {
		return 0, err
	}

	return lsn, nil
}

func (w *W) AppendDel(key []byte) (uint64, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.lastLSN++

	lsn := w.lastLSN

	action := Action{
		LSN:  lsn,
		Type: ActionTypeDel,
		Key:  key,
	}

	err := AppendAction(w.file, action)
	if err != nil {
		return 0, err
	}

	return lsn, nil
}
