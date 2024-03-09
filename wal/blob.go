package wal

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/Kurt212/zapp/constants"
)

const (
	lsnSize    = 8 // bytes
	typeSize   = 1 // byte
	expireSize = 4 // bytes
	keylenSize = 2 // bytes
	vallenSize = 4 // bytes
)

func initialRead(file io.ReadSeeker, lastAppliedLSN uint64) (_ []Action, lastSeenLSN uint64, _ error) {
	_, err := file.Seek(0, constants.OriginWhence)
	if err != nil {
		return nil, 0, fmt.Errorf("got error when moving wal file's cursor: %w", err)
	}

	lastLSN := uint64(0)

	var unappliedActions []Action

	for {
		lsnAndTypeBuffer := make([]byte, lsnSize+typeSize)
		_, err := file.Read(lsnAndTypeBuffer)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, 0, err
		}

		lsn := binary.BigEndian.Uint64(lsnAndTypeBuffer[:lsnSize])
		actonType := ActionType(lsnAndTypeBuffer[lsnSize])

		lastLSN = lsn // update global last seen LSN

		switch actonType {
		case ActionTypeSet:
			// can read expire + keylen + vallen and then check lsn to determine if need to skip this entry or append it to result
			expireAndKeylenAndVallenBuffer := make([]byte, expireSize+keylenSize+vallenSize)
			n, err := file.Read(expireAndKeylenAndVallenBuffer)
			if err != nil {
				return nil, 0, fmt.Errorf("got error when reading set action wal's entry payload: %w", err)
			}
			if n < len(expireAndKeylenAndVallenBuffer) {
				return nil, 0, fmt.Errorf("expected %d bytes to read expire, keylen and vallen, but got %d bytes", len(expireAndKeylenAndVallenBuffer), n)
			}
			expire := binary.BigEndian.Uint32(expireAndKeylenAndVallenBuffer[:expireSize])
			keylen := binary.BigEndian.Uint16(expireAndKeylenAndVallenBuffer[expireSize : expireSize+keylenSize])
			vallen := binary.BigEndian.Uint32(expireAndKeylenAndVallenBuffer[expireSize+keylenSize:])

			// if lastAppliedLSN is greater than this wal entry LSN, then it means that this entry was already appliend
			// now need to move file cursor to next entry and skip keylen + vallen bytes
			if lastAppliedLSN >= lsn {
				_, err := file.Seek(int64(keylen)+int64(vallen), constants.CurrentPositionWhence)
				if err != nil {
					return nil, 0, fmt.Errorf("got error when moved cursor to next wal entry: %w", err)
				}
				// now when moved cursor go to next loop itteration
				continue
			}

			keyPayloadAndValPayloadBuffer := make([]byte, int(keylen)+int(vallen))
			n, err = file.Read(keyPayloadAndValPayloadBuffer)
			if err != nil {
				return nil, 0, fmt.Errorf("got error when reading set action wal's entry payload: %w", err)
			}
			if n < len(keyPayloadAndValPayloadBuffer) {
				return nil, 0, fmt.Errorf("expected %d bytes to read key and val, but got %d bytes", len(keyPayloadAndValPayloadBuffer), n)
			}

			key := keyPayloadAndValPayloadBuffer[:keylen]
			val := keyPayloadAndValPayloadBuffer[keylen:]

			action := Action{
				Type:   ActionTypeSet,
				LSN:    lsn,
				Key:    key,
				Value:  val,
				Expire: expire,
			}

			unappliedActions = append(unappliedActions, action)

		case ActionTypeDel:
			// can read expire + keylen + vallen and then check lsn to determine if need to skip this entry or append it to result
			keylenBuffer := make([]byte, keylenSize)
			n, err := file.Read(keylenBuffer)
			if err != nil {
				return nil, 0, fmt.Errorf("got error when reading del action wal's entry payload: %w", err)
			}
			if n < len(keylenBuffer) {
				return nil, 0, fmt.Errorf("expected %d bytes to read keylen, but got %d bytes", len(keylenBuffer), n)
			}
			keylen := binary.BigEndian.Uint16(keylenBuffer)

			// if lastAppliedLSN is greater than this wal entry LSN, then it means that this entry was already appliend
			// now need to move file cursor to next entry and skip keylen + vallen bytes
			if lastAppliedLSN >= lsn {
				_, err := file.Seek(int64(keylen), constants.CurrentPositionWhence)
				if err != nil {
					return nil, 0, fmt.Errorf("got error when moved cursor to next wal entry: %w", err)
				}
				// now when moved cursor go to next loop itteration
				continue
			}

			keyPayload := make([]byte, int(keylen))
			n, err = file.Read(keyPayload)
			if err != nil {
				return nil, 0, fmt.Errorf("got error when reading set action wal's entry payload: %w", err)
			}
			if n < len(keyPayload) {
				return nil, 0, fmt.Errorf("expected %d bytes to read key and val, but got %d bytes", len(keyPayload), n)
			}

			action := Action{
				Type: ActionTypeDel,
				LSN:  lsn,
				Key:  keyPayload,
			}

			unappliedActions = append(unappliedActions, action)

		default:
			return nil, 0, fmt.Errorf("unknown wal's action type %d met, when reading wal", actonType)
		}
	}

	return unappliedActions, lastLSN, nil
}

func AppendAction(file io.Writer, action Action) error {
	var buffer []byte

	buffer = binary.BigEndian.AppendUint64(buffer, action.LSN)

	switch action.Type {
	case ActionTypeSet:
		buffer = append(buffer, byte(action.Type))

		buffer = binary.BigEndian.AppendUint32(buffer, action.Expire)
		buffer = binary.BigEndian.AppendUint16(buffer, uint16(len(action.Key)))
		buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(action.Value)))
		buffer = append(buffer, action.Key...)
		buffer = append(buffer, action.Value...)

	case ActionTypeDel:
		buffer = append(buffer, byte(action.Type))

		buffer = binary.BigEndian.AppendUint16(buffer, uint16(len(action.Key)))
		buffer = append(buffer, action.Key...)

	default:
		return fmt.Errorf("trying to append to wal unknown action type %d", action.Type)
	}

	n, err := file.Write(buffer)
	if err != nil {
		return fmt.Errorf("got error when trying to write to wal's file: %w", err)
	}
	if n != len(buffer) {
		return fmt.Errorf("appended only %d bytes to WAL file, wanted %d bytes", n, len(buffer))
	}

	return nil
}
