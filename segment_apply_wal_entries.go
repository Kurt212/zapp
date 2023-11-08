package zapp

import (
	"errors"
	"fmt"

	"github.com/Kurt212/zapp/wal"
)

// performUnappliedWALActions performs actions obtained from WAL file,
// which was not found in current segment
// Only called on segment creation
func (seg *segment) performUnappliedWALActions(actions []wal.Action) error {
	for _, action := range actions {
		lsn := action.LSN

	SWITCH:
		switch action.Type {
		case wal.ActionTypeSet:
			key := action.Key
			value := action.Value
			expire := action.Expire

			keyHash := hash(key)

			err := seg.rawSet(keyHash, key, value, expire)
			if err != nil {
				return fmt.Errorf("got error when performing SET action from wal with lsn %d: %w", lsn, err)
			}
		case wal.ActionTypeDel:
			key := action.Key

			keyHash := hash(key)

			err := seg.rawDelete(keyHash, key)
			// if error is not found, then this is okay cause
			// delete was appended to WAL before checking if this key really existed
			if errors.Is(err, ErrNotFound) {
				break SWITCH
			}
			if err != nil {
				return fmt.Errorf("got error when performing DEL action from wal with lsn %d: %w", lsn, err)
			}
		default:
			return fmt.Errorf("unknown action type %d", action.Type)
		}

		seg.lastKnownLSN = lsn
	}

	err := seg.rawWriteLastKnownLSN(seg.lastKnownLSN)
	if err != nil {
		return err
	}

	return nil
}
