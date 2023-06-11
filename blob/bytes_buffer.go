package blob

import (
	"bytes"
	"encoding/binary"
)

type Buffer struct {
	size   int
	buffer *bytes.Buffer
}

func NewBuffer(size int) *Buffer {
	return &Buffer{
		size:   size,
		buffer: bytes.NewBuffer(make([]byte, 0, size)),
	}
}

func (b *Buffer) WriteHeader(h Header) {
	data := make([]byte, 0, HeaderSize)

	data = append(data, h.SizePower)
	data = append(data, byte(h.Status))

	data = binary.BigEndian.AppendUint16(data, h.KeyLen)
	data = binary.BigEndian.AppendUint32(data, h.ValLen)

	data = binary.BigEndian.AppendUint32(data, h.Expire)

	b.buffer.Write(data)
}

func (b *Buffer) WriteValue(val []byte) {
	b.buffer.Write(val)
}

func (b *Buffer) WriteKey(key []byte) {
	b.buffer.Write(key)
}

func (b *Buffer) Bytes() []byte {
	result := make([]byte, b.size)

	original := b.buffer.Bytes()

	copy(result, original)

	return result
}
