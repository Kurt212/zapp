package blob

import (
	"encoding/binary"
	"time"
)

const (
	SizePowerSize = 1 // byte
	StatusSize    = 1 // byte
	KeyLenSize    = 2 // bytes
	ValLenSize    = 4 // bytes
	ExpireSize    = 4 // bytes

	HeaderSize = SizePowerSize + StatusSize + KeyLenSize + ValLenSize + ExpireSize // bytes

	StatusOffset = SizePowerSize
)

const (
	StatusOK      = 212
	StatusDeleted = 106
)

type Status byte

type Header struct {
	SizePower byte
	Status    Status
	KeyLen    uint16
	ValLen    uint32
	Expire    uint32
}

func (h Header) Size() int {
	return 1 << h.SizePower
}

func (h Header) IsExpired(now time.Time) bool {
	if h.Expire == 0 {
		return false
	}

	expireTime := time.Unix(int64(h.Expire), 0)

	return expireTime.Before(now)
}

// KVE stands for Key Value Expire
type KVE struct {
	Key    []byte
	Value  []byte
	Expire uint32
}

func (kve KVE) Marshal() (_ []byte, nextPowerOfTwo int) {
	currenRawSize := len(kve.Key) + len(kve.Value) + HeaderSize
	powerNumber, paddedSize := NextNumberOfPowerOfTwo(currenRawSize)

	buffer := NewBuffer(paddedSize)

	header := Header{
		SizePower: powerNumber,
		Status:    StatusOK,
		KeyLen:    uint16(len(kve.Key)),
		ValLen:    uint32(len(kve.Value)),
		Expire:    kve.Expire,
	}

	buffer.WriteHeader(header)
	buffer.WriteValue(kve.Value)
	buffer.WriteKey(kve.Key)

	return buffer.Bytes(), paddedSize
}

func (kve KVE) IsExpired(now time.Time) bool {
	if kve.Expire == 0 {
		return false
	}

	expireTime := time.Unix(int64(kve.Expire), 0)

	return expireTime.Before(now)
}

func Unmarshal(buffer []byte) KVE {
	header := UnmarshalHeader(buffer[:HeaderSize])

	return UnmarshalBody(buffer[HeaderSize:], header)
}

func UnmarshalHeader(buffer []byte) Header {
	header := Header{}

	// TODO checks for bad buffer lengths

	var offset = 0

	header.SizePower = buffer[offset]
	offset += SizePowerSize

	header.Status = Status(buffer[offset])
	offset += StatusSize

	header.KeyLen = binary.BigEndian.Uint16(buffer[offset : offset+KeyLenSize])
	offset += KeyLenSize

	header.ValLen = binary.BigEndian.Uint32(buffer[offset : offset+ValLenSize])
	offset += ValLenSize

	header.Expire = binary.BigEndian.Uint32(buffer[offset : offset+ExpireSize])
	offset += ExpireSize

	return header
}

func UnmarshalBody(buffer []byte, header Header) KVE {
	// TODO checks for bad buffer lengths
	kve := KVE{
		Key:    buffer[header.ValLen : header.ValLen+uint32(header.KeyLen)],
		Value:  buffer[:header.ValLen],
		Expire: header.Expire,
	}

	return kve
}

func NextPowerOfTwo[V int | int32 | int64](value V) V {
	value--

	value |= value >> 1
	value |= value >> 2
	value |= value >> 4
	value |= value >> 8
	value |= value >> 16

	value++

	return value
}

func NextNumberOfPowerOfTwo[V int | int32 | int64](value V) (byte, V) {
	var power byte = 0
	var nextValue V = 1

	nextPower := NextPowerOfTwo(value)

	for nextValue < nextPower {
		power++
		nextValue <<= 1
	}

	return power, nextValue
}
