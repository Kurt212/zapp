package blob

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuffer(t *testing.T) {
	t.Run("marshal raw", func(t *testing.T) {
		buffer := NewBuffer(32)

		value := []byte{0xCA, 0xFE, 0xBA, 0xBE}
		key := []byte("key") // 0x6B, 0x65, 0x79

		h := Header{
			SizePower: 5,
			Status:    212,
			KeyLen:    uint16(len(key)),
			ValLen:    uint32(len(value)),
			Expire:    0,
		}

		buffer.WriteHeader(h)
		buffer.WriteValue(value)
		buffer.WriteKey(key)

		result := buffer.Bytes()

		expect := []byte{
			5,    // size power
			212,  // status
			0, 3, // key len
			0, 0, 0, 4, // val len
			0, 0, 0, 0, // expire
			0xCA, 0xFE, 0xBA, 0xBE, // value
			0x6B, 0x65, 0x79, // key
			0x00, 0x00, 0x00, 0x00, // padding
			0x00, 0x00, 0x00, 0x00, // padding
			0x00, 0x00, 0x00, // padding
			0x00, 0x00, // padding
		}

		assert.Equal(t, expect, result)
	})

	t.Run("marshal", func(t *testing.T) {
		data := KVE{
			Key:    []byte("key"),
			Value:  []byte{0xCA, 0xFE, 0xBA, 0xBE},
			Expire: 0,
		}

		result, size := data.Marshal()

		expect := []byte{
			5,    // size power
			212,  // status
			0, 3, // key len
			0, 0, 0, 4, // val len
			0, 0, 0, 0, // expire
			0xCA, 0xFE, 0xBA, 0xBE, // value
			0x6B, 0x65, 0x79, // key
			0x00, 0x00, 0x00, 0x00, // padding
			0x00, 0x00, 0x00, 0x00, // padding
			0x00, 0x00, 0x00, // padding
			0x00, 0x00, // padding
		}

		assert.Equal(t, 32, size)
		assert.Equal(t, expect, result)
	})

	t.Run("unmarshal", func(t *testing.T) {
		data := []byte{
			5,    // size power
			0,    // status
			0, 3, // key len
			0, 0, 0, 4, // val len
			0, 0, 0, 0, // expire
			0xCA, 0xFE, 0xBA, 0xBE, // value
			0x6B, 0x65, 0x79, // key
			0x00, 0x00, 0x00, 0x00, // padding
			0x00, 0x00, 0x00, 0x00, // padding
			0x00, 0x00, 0x00, // padding
			0x00, 0x00, // padding
		}

		result := Unmarshal(data)

		expect := KVE{
			Key:    []byte("key"),
			Value:  []byte{0xCA, 0xFE, 0xBA, 0xBE},
			Expire: 0,
		}

		assert.Equal(t, expect, result)
	})
}
