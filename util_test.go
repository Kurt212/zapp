package zapp

import (
	"encoding/binary"
	"io"

	"github.com/Kurt212/zapp/blob"
	"github.com/Kurt212/zapp/wal"
)

type v struct {
	value  []byte
	expire uint32
	lsn    uint64 // optional. is you don't use WAL, then it means nothing
}

type kv struct {
	key []byte
	v
}

func makeSegment(writer io.Writer, data map[string]v) error {
	var orderedData []kv
	for k, v := range data {
		orderedData = append(orderedData, kv{
			key: []byte(k),
			v:   v,
		})
	}

	return makeSegmentOrdered(writer, orderedData)
}

func makeSegmentOrdered(writer io.Writer, orderedData []kv) error {
	lastLSN := uint64(0)

	for _, itm := range orderedData {
		if itm.lsn > lastLSN {
			lastLSN = itm.lsn
		}
	}

	var headerBuffer []byte

	headerBuffer = append(headerBuffer, segmentFileBeginMagicNumbers...)
	headerBuffer = append(headerBuffer, segmentFileLayoutVerion1)

	// reserved bytes
	for i := 0; i < segmentFileLayoutReservedSize; i++ {
		headerBuffer = append(headerBuffer, 0x0)
	}

	headerBuffer = binary.BigEndian.AppendUint64(headerBuffer, lastLSN)

	_, err := writer.Write(headerBuffer)
	if err != nil {
		return nil
	}

	for _, item := range orderedData {
		kve := blob.KVE{
			Key:    item.key,
			Value:  item.value,
			Expire: item.expire,
		}

		buffer, _ := kve.Marshal()

		_, err := writer.Write(buffer)
		if err != nil {
			return err
		}
	}

	return nil
}

func makeWAL(writer io.Writer, actions []wal.Action) error {
	for _, action := range actions {
		err := wal.AppendAction(writer, action)
		if err != nil {
			return err
		}
	}
	return nil
}
