package wal

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitialRead(t *testing.T) {
	t.Run("action set", func(t *testing.T) {
		inputData := []byte{}

		key := []byte("test_key")
		value := []byte("test value")
		lsn := uint64(1)
		expire := uint32(100500)

		inputData = binary.BigEndian.AppendUint64(inputData, lsn)                // lsn
		inputData = append(inputData, byte(ActionTypeSet))                       // type
		inputData = binary.BigEndian.AppendUint32(inputData, expire)             // expire
		inputData = binary.BigEndian.AppendUint16(inputData, uint16(len(key)))   // keylen
		inputData = binary.BigEndian.AppendUint32(inputData, uint32(len(value))) // vallen
		inputData = append(inputData, key...)                                    // key payload
		inputData = append(inputData, value...)                                  // val payload

		lastSeenLSN := 0

		reader := bytes.NewReader(inputData)

		result, lastLSN, err := initialRead(reader, uint64(lastSeenLSN))
		assert.NoError(t, err)

		assert.Equal(t, lsn, lastLSN)

		expected := []Action{
			{
				Type:   ActionTypeSet,
				LSN:    lsn,
				Key:    key,
				Value:  value,
				Expire: expire,
			},
		}

		assert.Equal(t, expected, result)
	})

	t.Run("action del", func(t *testing.T) {
		inputData := []byte{}

		key := []byte("test_key")
		lsn := uint64(1)

		inputData = binary.BigEndian.AppendUint64(inputData, lsn)              // lsn
		inputData = append(inputData, byte(ActionTypeDel))                     // type
		inputData = binary.BigEndian.AppendUint16(inputData, uint16(len(key))) // keylen
		inputData = append(inputData, key...)                                  // key payload

		lastSeenLSN := 0

		reader := bytes.NewReader(inputData)

		result, lastLSN, err := initialRead(reader, uint64(lastSeenLSN))
		assert.NoError(t, err)

		assert.Equal(t, lsn, lastLSN)

		expected := []Action{
			{
				Type: ActionTypeDel,
				LSN:  lsn,
				Key:  key,
			},
		}

		assert.Equal(t, expected, result)
	})

	t.Run("action set and del", func(t *testing.T) {
		inputData := []byte{}

		key := []byte("test_key")
		value := []byte("test value")
		lsn := uint64(1)
		expire := uint32(100500)

		inputData = binary.BigEndian.AppendUint64(inputData, lsn)                // lsn
		inputData = append(inputData, byte(ActionTypeSet))                       // type
		inputData = binary.BigEndian.AppendUint32(inputData, expire)             // expire
		inputData = binary.BigEndian.AppendUint16(inputData, uint16(len(key)))   // keylen
		inputData = binary.BigEndian.AppendUint32(inputData, uint32(len(value))) // vallen
		inputData = append(inputData, key...)                                    // key payload
		inputData = append(inputData, value...)                                  // val payload

		key = []byte("test_key2")
		lsn = uint64(2)

		inputData = binary.BigEndian.AppendUint64(inputData, lsn)              // lsn
		inputData = append(inputData, byte(ActionTypeDel))                     // type
		inputData = binary.BigEndian.AppendUint16(inputData, uint16(len(key))) // keylen
		inputData = append(inputData, key...)                                  // key payload

		lastSeenLSN := 0

		reader := bytes.NewReader(inputData)

		result, lastLSN, err := initialRead(reader, uint64(lastSeenLSN))
		assert.NoError(t, err)

		assert.Equal(t, lsn, lastLSN)

		expected := []Action{
			{
				Type:   ActionTypeSet,
				LSN:    1,
				Key:    []byte("test_key"),
				Value:  []byte("test value"),
				Expire: expire,
			},
			{
				Type: ActionTypeDel,
				LSN:  2,
				Key:  []byte("test_key2"),
			},
		}

		assert.Equal(t, expected, result)
	})

	t.Run("action set and del but set was applied", func(t *testing.T) {
		inputData := []byte{}

		key := []byte("test_key")
		value := []byte("test value")
		lsn := uint64(1)
		expire := uint32(100500)

		inputData = binary.BigEndian.AppendUint64(inputData, lsn)                // lsn
		inputData = append(inputData, byte(ActionTypeSet))                       // type
		inputData = binary.BigEndian.AppendUint32(inputData, expire)             // expire
		inputData = binary.BigEndian.AppendUint16(inputData, uint16(len(key)))   // keylen
		inputData = binary.BigEndian.AppendUint32(inputData, uint32(len(value))) // vallen
		inputData = append(inputData, key...)                                    // key payload
		inputData = append(inputData, value...)                                  // val payload

		key = []byte("test_key2")
		lsn = uint64(2)

		inputData = binary.BigEndian.AppendUint64(inputData, lsn)              // lsn
		inputData = append(inputData, byte(ActionTypeDel))                     // type
		inputData = binary.BigEndian.AppendUint16(inputData, uint16(len(key))) // keylen
		inputData = append(inputData, key...)                                  // key payload

		lastSeenLSN := 1

		reader := bytes.NewReader(inputData)

		result, lastLSN, err := initialRead(reader, uint64(lastSeenLSN))
		assert.NoError(t, err)

		assert.Equal(t, lsn, lastLSN)

		expected := []Action{
			{
				Type: ActionTypeDel,
				LSN:  2,
				Key:  []byte("test_key2"),
			},
		}

		assert.Equal(t, expected, result)
	})

	t.Run("action set and del but both were applied", func(t *testing.T) {
		inputData := []byte{}

		key := []byte("test_key")
		value := []byte("test value")
		lsn := uint64(1)
		expire := uint32(100500)

		inputData = binary.BigEndian.AppendUint64(inputData, lsn)                // lsn
		inputData = append(inputData, byte(ActionTypeSet))                       // type
		inputData = binary.BigEndian.AppendUint32(inputData, expire)             // expire
		inputData = binary.BigEndian.AppendUint16(inputData, uint16(len(key)))   // keylen
		inputData = binary.BigEndian.AppendUint32(inputData, uint32(len(value))) // vallen
		inputData = append(inputData, key...)                                    // key payload
		inputData = append(inputData, value...)                                  // val payload

		key = []byte("test_key2")
		lsn = uint64(2)

		inputData = binary.BigEndian.AppendUint64(inputData, lsn)              // lsn
		inputData = append(inputData, byte(ActionTypeDel))                     // type
		inputData = binary.BigEndian.AppendUint16(inputData, uint16(len(key))) // keylen
		inputData = append(inputData, key...)                                  // key payload

		lastSeenLSN := 2

		reader := bytes.NewReader(inputData)

		result, lastLSN, err := initialRead(reader, uint64(lastSeenLSN))
		assert.NoError(t, err)

		assert.Equal(t, lsn, lastLSN)

		assert.Empty(t, result)
	})

	t.Run("empty", func(t *testing.T) {
		inputData := []byte{}

		lastSeenLSN := uint64(0)

		reader := bytes.NewReader(inputData)

		result, lastLSN, err := initialRead(reader, lastSeenLSN)
		assert.NoError(t, err)

		assert.Equal(t, uint64(0), lastLSN)

		assert.Empty(t, result)
	})

	t.Run("partial action set", func(t *testing.T) {
		inputData := []byte{}

		key := []byte("test_key")
		lsn := uint64(1)
		expire := uint32(100500)

		inputData = binary.BigEndian.AppendUint64(inputData, lsn)              // lsn
		inputData = append(inputData, byte(ActionTypeSet))                     // type
		inputData = binary.BigEndian.AppendUint32(inputData, expire)           // expire
		inputData = binary.BigEndian.AppendUint16(inputData, uint16(len(key))) // keylen

		lastSeenLSN := 0

		reader := bytes.NewReader(inputData)

		_, _, err := initialRead(reader, uint64(lastSeenLSN))
		assert.Error(t, err)
	})

	t.Run("unknown action", func(t *testing.T) {
		inputData := []byte{}

		inputData = binary.BigEndian.AppendUint64(inputData, 1) // lsn
		inputData = append(inputData, byte(200))                // type

		lastSeenLSN := 0

		reader := bytes.NewReader(inputData)

		_, _, err := initialRead(reader, uint64(lastSeenLSN))
		assert.Error(t, err)
	})
}

func TestAppendAction(t *testing.T) {
	t.Run("append set", func(t *testing.T) {
		key := []byte("test_key")
		value := []byte("test value")
		lsn := uint64(1)
		expire := uint32(100500)

		action := Action{
			LSN:    lsn,
			Type:   ActionTypeSet,
			Key:    key,
			Value:  value,
			Expire: expire,
		}

		buffer := bytes.NewBuffer(nil)

		err := AppendAction(buffer, action)

		assert.NoError(t, err)

		expected := []byte{}

		expected = binary.BigEndian.AppendUint64(expected, lsn)                // lsn
		expected = append(expected, byte(ActionTypeSet))                       // type
		expected = binary.BigEndian.AppendUint32(expected, expire)             // expire
		expected = binary.BigEndian.AppendUint16(expected, uint16(len(key)))   // keylen
		expected = binary.BigEndian.AppendUint32(expected, uint32(len(value))) // vallen
		expected = append(expected, key...)                                    // key payload
		expected = append(expected, value...)                                  // value payload

		assert.Equal(t, expected, buffer.Bytes())
	})

	t.Run("append del", func(t *testing.T) {
		key := []byte("test_key")
		lsn := uint64(1)

		action := Action{
			LSN:  lsn,
			Type: ActionTypeDel,
			Key:  key,
		}

		buffer := bytes.NewBuffer(nil)

		err := AppendAction(buffer, action)

		assert.NoError(t, err)

		expected := []byte{}

		expected = binary.BigEndian.AppendUint64(expected, lsn)              // lsn
		expected = append(expected, byte(ActionTypeDel))                     // type
		expected = binary.BigEndian.AppendUint16(expected, uint16(len(key))) // keylen
		expected = append(expected, key...)                                  // key payload

		assert.Equal(t, expected, buffer.Bytes())
	})
}
