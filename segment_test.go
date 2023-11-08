package zapp

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"

	"github.com/Kurt212/zapp/constants"
	"github.com/Kurt212/zapp/wal"
	"github.com/stretchr/testify/require"
)

func TestOpenFromExistingSegmentFiles(t *testing.T) {
	t.Run("restore from wal 2 sets", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		require.NoError(t, err)

		defer os.Remove(dataFile.Name())
		defer dataFile.Close()

		walFile, err := os.CreateTemp(dir, "*")

		require.NoError(t, err)

		defer os.Remove(walFile.Name())
		defer walFile.Close()

		segmentData := map[string]v{
			"key1": {
				value: []byte("value1"),
				lsn:   1,
			},
			"key2": {
				value: []byte("value2"),
				lsn:   2,
			},
		}

		err = makeSegment(dataFile, segmentData)
		require.NoError(t, err)

		walActions := []wal.Action{
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key1"),
				Value: []byte("value1"),
				LSN:   1,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key2"),
				Value: []byte("value2"),
				LSN:   2,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key3"),
				Value: []byte("value3"),
				LSN:   3,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key4"),
				Value: []byte("value4"),
				LSN:   4,
			},
		}

		err = makeWAL(walFile, walActions)
		require.NoError(t, err)

		segment, err := newSegment(
			dataFile,
			walFile,
			time.Hour,
			time.Hour,
		)
		require.NoError(t, err)

		require.Equal(t, uint64(4), segment.lastKnownLSN)
		require.Equal(t, uint64(4), segment.wal.LastLSN())

		key := []byte("key3")
		value, err := segment.Get(hash(key), key)

		require.NoError(t, err)

		require.Equal(t, []byte("value3"), value)

		key = []byte("key4")
		value, err = segment.Get(hash(key), key)

		require.NoError(t, err)

		require.Equal(t, []byte("value4"), value)
	})

	t.Run("restore from wal 2 sets with empty datafile", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		require.NoError(t, err)

		defer os.Remove(dataFile.Name())
		defer dataFile.Close()

		walFile, err := os.CreateTemp(dir, "*")

		require.NoError(t, err)

		defer os.Remove(walFile.Name())
		defer walFile.Close()

		walActions := []wal.Action{
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key1"),
				Value: []byte("value1"),
				LSN:   1,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key2"),
				Value: []byte("value2"),
				LSN:   2,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key3"),
				Value: []byte("value3"),
				LSN:   3,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key4"),
				Value: []byte("value4"),
				LSN:   4,
			},
		}

		err = makeWAL(walFile, walActions)
		require.NoError(t, err)

		segment, err := newSegment(
			dataFile,
			walFile,
			time.Hour,
			time.Hour,
		)
		require.NoError(t, err)

		require.Equal(t, uint64(4), segment.lastKnownLSN)
		require.Equal(t, uint64(4), segment.wal.LastLSN())

		key := []byte("key3")
		value, err := segment.Get(hash(key), key)

		require.NoError(t, err)

		require.Equal(t, []byte("value3"), value)

		key = []byte("key4")
		value, err = segment.Get(hash(key), key)

		require.NoError(t, err)

		require.Equal(t, []byte("value4"), value)
	})

	t.Run("restore from wal 2 sets with empty datafile but one is expired", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		require.NoError(t, err)

		defer os.Remove(dataFile.Name())
		defer dataFile.Close()

		walFile, err := os.CreateTemp(dir, "*")

		require.NoError(t, err)

		defer os.Remove(walFile.Name())
		defer walFile.Close()

		now := time.Now()
		expiredTime := now.Add(-time.Second) // expired for sure
		notExpiredTime := now.Add(time.Hour) // not expired for sure

		walActions := []wal.Action{
			{
				Type:   wal.ActionTypeSet,
				Key:    []byte("key1"),
				Value:  []byte("value1"),
				Expire: uint32(expiredTime.Unix()),
				LSN:    1,
			},
			{
				Type:   wal.ActionTypeSet,
				Key:    []byte("key2"),
				Value:  []byte("value2"),
				Expire: uint32(notExpiredTime.Unix()),
				LSN:    2,
			},
		}

		err = makeWAL(walFile, walActions)
		require.NoError(t, err)

		segment, err := newSegment(
			dataFile,
			walFile,
			time.Hour,
			time.Hour,
		)
		require.NoError(t, err)

		require.Equal(t, uint64(2), segment.lastKnownLSN)
		require.Equal(t, uint64(2), segment.wal.LastLSN())

		key := []byte("key1")
		_, err = segment.Get(hash(key), key)
		require.ErrorIs(t, err, ErrNotFound)

		key = []byte("key2")
		_, err = segment.Get(hash(key), key)
		require.NoError(t, err)
	})

	t.Run("restore from wal 2 sets then 1 del", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		require.NoError(t, err)

		defer os.Remove(dataFile.Name())
		defer dataFile.Close()

		walFile, err := os.CreateTemp(dir, "*")

		require.NoError(t, err)

		defer os.Remove(walFile.Name())
		defer walFile.Close()

		segmentData := map[string]v{
			"key1": {
				value: []byte("value1"),
				lsn:   1,
			},
			"key2": {
				value: []byte("value2"),
				lsn:   2,
			},
		}

		err = makeSegment(dataFile, segmentData)
		require.NoError(t, err)

		walActions := []wal.Action{
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key1"),
				Value: []byte("value1"),
				LSN:   1,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key2"),
				Value: []byte("value2"),
				LSN:   2,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key3"),
				Value: []byte("value3"),
				LSN:   3,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key4"),
				Value: []byte("value4"),
				LSN:   4,
			},
			{
				Type: wal.ActionTypeDel,
				Key:  []byte("key3"),
				LSN:  5,
			},
		}

		err = makeWAL(walFile, walActions)
		require.NoError(t, err)

		segment, err := newSegment(
			dataFile,
			walFile,
			time.Hour,
			time.Hour,
		)
		require.NoError(t, err)

		require.Equal(t, uint64(5), segment.lastKnownLSN)
		require.Equal(t, uint64(5), segment.wal.LastLSN())

		key := []byte("key3")
		_, err = segment.Get(hash(key), key)

		require.ErrorIs(t, err, ErrNotFound)

		key = []byte("key4")
		value, err := segment.Get(hash(key), key)

		require.NoError(t, err)

		require.Equal(t, []byte("value4"), value)
	})

	t.Run("restore from wal 2 sets then 1 del then del non existing key", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		require.NoError(t, err)

		defer os.Remove(dataFile.Name())
		defer dataFile.Close()

		walFile, err := os.CreateTemp(dir, "*")

		require.NoError(t, err)

		defer os.Remove(walFile.Name())
		defer walFile.Close()

		segmentData := map[string]v{
			"key1": {
				value: []byte("value1"),
				lsn:   1,
			},
			"key2": {
				value: []byte("value2"),
				lsn:   2,
			},
		}

		err = makeSegment(dataFile, segmentData)
		require.NoError(t, err)

		walActions := []wal.Action{
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key1"),
				Value: []byte("value1"),
				LSN:   1,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key2"),
				Value: []byte("value2"),
				LSN:   2,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key3"),
				Value: []byte("value3"),
				LSN:   3,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key4"),
				Value: []byte("value4"),
				LSN:   4,
			},
			{
				Type: wal.ActionTypeDel,
				Key:  []byte("key3"),
				LSN:  5,
			},
			{
				Type: wal.ActionTypeDel,
				Key:  []byte("key100500"),
				LSN:  6,
			},
		}

		err = makeWAL(walFile, walActions)
		require.NoError(t, err)

		segment, err := newSegment(
			dataFile,
			walFile,
			time.Hour,
			time.Hour,
		)
		require.NoError(t, err)

		require.Equal(t, uint64(6), segment.lastKnownLSN)
		require.Equal(t, uint64(6), segment.wal.LastLSN())

		key := []byte("key3")
		_, err = segment.Get(hash(key), key)

		require.ErrorIs(t, err, ErrNotFound)

		key = []byte("key4")
		value, err := segment.Get(hash(key), key)

		require.NoError(t, err)

		require.Equal(t, []byte("value4"), value)
	})

	t.Run("restore from wal 2 sets then 1 del then del non existing key then one more set", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		require.NoError(t, err)

		defer os.Remove(dataFile.Name())
		defer dataFile.Close()

		walFile, err := os.CreateTemp(dir, "*")

		require.NoError(t, err)

		defer os.Remove(walFile.Name())
		defer walFile.Close()

		segmentData := map[string]v{
			"key1": {
				value: []byte("value1"),
				lsn:   1,
			},
			"key2": {
				value: []byte("value2"),
				lsn:   2,
			},
		}

		err = makeSegment(dataFile, segmentData)
		require.NoError(t, err)

		walActions := []wal.Action{
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key1"),
				Value: []byte("value1"),
				LSN:   1,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key2"),
				Value: []byte("value2"),
				LSN:   2,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key3"),
				Value: []byte("value3"),
				LSN:   3,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key4"),
				Value: []byte("value4"),
				LSN:   4,
			},
			{
				Type: wal.ActionTypeDel,
				Key:  []byte("key3"),
				LSN:  5,
			},
			{
				Type: wal.ActionTypeDel,
				Key:  []byte("key100500"),
				LSN:  6,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key4"),
				Value: []byte("value4 new"),
				LSN:   7,
			},
		}

		err = makeWAL(walFile, walActions)
		require.NoError(t, err)

		segment, err := newSegment(
			dataFile,
			walFile,
			time.Hour,
			time.Hour,
		)
		require.NoError(t, err)

		require.Equal(t, uint64(7), segment.lastKnownLSN)
		require.Equal(t, uint64(7), segment.wal.LastLSN())

		key := []byte("key3")
		_, err = segment.Get(hash(key), key)

		require.ErrorIs(t, err, ErrNotFound)

		key = []byte("key4")
		value, err := segment.Get(hash(key), key)

		require.NoError(t, err)

		require.Equal(t, []byte("value4 new"), value)
	})

	t.Run("restore from wal 2 sets then 1 del then del non existing key then one more set then del", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		require.NoError(t, err)

		defer os.Remove(dataFile.Name())
		defer dataFile.Close()

		walFile, err := os.CreateTemp(dir, "*")

		require.NoError(t, err)

		defer os.Remove(walFile.Name())
		defer walFile.Close()

		segmentData := map[string]v{
			"key1": {
				value: []byte("value1"),
				lsn:   1,
			},
			"key2": {
				value: []byte("value2"),
				lsn:   2,
			},
		}

		err = makeSegment(dataFile, segmentData)
		require.NoError(t, err)

		walActions := []wal.Action{
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key1"),
				Value: []byte("value1"),
				LSN:   1,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key2"),
				Value: []byte("value2"),
				LSN:   2,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key3"),
				Value: []byte("value3"),
				LSN:   3,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key4"),
				Value: []byte("value4"),
				LSN:   4,
			},
			{
				Type: wal.ActionTypeDel,
				Key:  []byte("key3"),
				LSN:  5,
			},
			{
				Type: wal.ActionTypeDel,
				Key:  []byte("key100500"),
				LSN:  6,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key4"),
				Value: []byte("value4 new"),
				LSN:   7,
			},
			{
				Type: wal.ActionTypeDel,
				Key:  []byte("key4"),
				LSN:  8,
			},
		}

		err = makeWAL(walFile, walActions)
		require.NoError(t, err)

		segment, err := newSegment(
			dataFile,
			walFile,
			time.Hour,
			time.Hour,
		)
		require.NoError(t, err)

		require.Equal(t, uint64(8), segment.lastKnownLSN)
		require.Equal(t, uint64(8), segment.wal.LastLSN())

		key := []byte("key3")
		_, err = segment.Get(hash(key), key)

		require.ErrorIs(t, err, ErrNotFound)

		key = []byte("key4")
		_, err = segment.Get(hash(key), key)

		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("empty wal no restore", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		require.NoError(t, err)

		defer os.Remove(dataFile.Name())
		defer dataFile.Close()

		walFile, err := os.CreateTemp(dir, "*")

		require.NoError(t, err)

		defer os.Remove(walFile.Name())
		defer walFile.Close()

		segmentData := map[string]v{
			"key1": {
				value: []byte("value1"),
				lsn:   1,
			},
			"key2": {
				value: []byte("value2"),
				lsn:   2,
			},
			"key3": {
				value: []byte("value3"),
				lsn:   3,
			},
		}

		err = makeSegment(dataFile, segmentData)
		require.NoError(t, err)

		segment, err := newSegment(
			dataFile,
			walFile,
			time.Hour,
			time.Hour,
		)
		require.NoError(t, err)

		require.Equal(t, uint64(3), segment.lastKnownLSN)
		require.Equal(t, uint64(3), segment.wal.LastLSN())

		key := []byte("key1")
		value, err := segment.Get(hash(key), key)

		require.NoError(t, err)

		require.Equal(t, []byte("value1"), value)

		key = []byte("key2")
		value, err = segment.Get(hash(key), key)

		require.NoError(t, err)

		require.Equal(t, []byte("value2"), value)

		key = []byte("key3")
		value, err = segment.Get(hash(key), key)

		require.NoError(t, err)

		require.Equal(t, []byte("value3"), value)
	})

	t.Run("open data file, close and check data file on disk and nothing changes", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		dataFileName := dataFile.Name()

		require.NoError(t, err)

		defer os.Remove(dataFile.Name())
		defer dataFile.Close()

		walFile, err := os.CreateTemp(dir, "*")

		require.NoError(t, err)

		defer os.Remove(walFile.Name())
		defer walFile.Close()

		segmentDataOrdered := []kv{
			{
				key: []byte("key1"),
				v: v{
					value: []byte("value1"),
					lsn:   1,
				},
			},
			{
				key: []byte("key2"),
				v: v{
					value: []byte("value2"),
					lsn:   2,
				},
			},
			{
				key: []byte("key3"),
				v: v{
					value: []byte("value3"),
					lsn:   3,
				},
			},
		}

		err = makeSegmentOrdered(dataFile, segmentDataOrdered)
		require.NoError(t, err)

		segment, err := newSegment(
			dataFile,
			walFile,
			time.Hour,
			time.Hour,
		)
		require.NoError(t, err)

		err = segment.Close()
		require.NoError(t, err)

		expectedDataFileBuffer := bytes.NewBuffer(nil)
		err = makeSegmentOrdered(expectedDataFileBuffer, segmentDataOrdered)
		require.NoError(t, err)

		dataFile, err = os.Open(dataFileName)
		require.NoError(t, err)

		offset, err := dataFile.Seek(0, constants.OriginWhence)
		require.NoError(t, err)
		require.Equal(t, int64(0), offset)

		onDiskDataFileBytes, err := io.ReadAll(dataFile)
		require.NoError(t, err)

		require.Equal(t, expectedDataFileBuffer.Bytes(), onDiskDataFileBytes)
	})

	t.Run("open data file, apply wal, close and check data file durability", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		dataFileName := dataFile.Name()

		require.NoError(t, err)

		defer os.Remove(dataFile.Name())
		defer dataFile.Close()

		walFile, err := os.CreateTemp(dir, "*")

		walFileName := walFile.Name()

		require.NoError(t, err)

		defer os.Remove(walFile.Name())
		defer walFile.Close()

		segmentData := map[string]v{
			"key1": {
				value: []byte("value1"),
				lsn:   1,
			},
			"key2": {
				value: []byte("value2"),
				lsn:   2,
			},
			"key3": {
				value: []byte("value3"),
				lsn:   3,
			},
		}

		err = makeSegment(dataFile, segmentData)
		require.NoError(t, err)

		walActions := []wal.Action{
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key4"),
				Value: []byte("value4"),
				LSN:   4,
			},
			{
				Type: wal.ActionTypeDel,
				Key:  []byte("key3"),
				LSN:  5,
			},
			{
				Type: wal.ActionTypeDel,
				Key:  []byte("key100500"),
				LSN:  6,
			},
			{
				Type:  wal.ActionTypeSet,
				Key:   []byte("key4"),
				Value: []byte("value4 new"),
				LSN:   7,
			},
		}

		err = makeWAL(walFile, walActions)
		require.NoError(t, err)

		segment, err := newSegment(
			dataFile,
			walFile,
			time.Hour,
			time.Hour,
		)
		require.NoError(t, err)

		err = segment.Close()
		require.NoError(t, err)

		walFile, err = os.Open(walFileName)
		require.NoError(t, err)

		walBuffer, err := io.ReadAll(walFile)
		require.NoError(t, err)
		require.Empty(t, walBuffer)

		walFile.Close()

		dataFile, err = os.OpenFile(dataFileName, os.O_RDWR, 0644)
		require.NoError(t, err)

		walFile, err = os.OpenFile(walFileName, os.O_RDWR|os.O_SYNC|os.O_APPEND, 0644)
		require.NoError(t, err)

		segment, err = newSegment(
			dataFile,
			walFile,
			time.Hour,
			time.Hour,
		)
		require.NoError(t, err)

		require.Equal(t, uint64(7), segment.lastKnownLSN)
		require.Equal(t, uint64(7), segment.wal.LastLSN())

		key := []byte("key1")
		value, err := segment.Get(hash(key), key)
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), value)

		key = []byte("key2")
		value, err = segment.Get(hash(key), key)
		require.NoError(t, err)
		require.Equal(t, []byte("value2"), value)

		key = []byte("key3")
		_, err = segment.Get(hash(key), key)
		require.ErrorIs(t, err, ErrNotFound)

		key = []byte("key4")
		value, err = segment.Get(hash(key), key)

		require.NoError(t, err)

		require.Equal(t, []byte("value4 new"), value)

		key = []byte("key100500")
		_, err = segment.Get(hash(key), key)
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("empty files no errors", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")
		require.NoError(t, err)

		defer os.Remove(dataFile.Name())
		defer dataFile.Close()

		walFile, err := os.CreateTemp(dir, "*")
		require.NoError(t, err)

		defer os.Remove(walFile.Name())
		defer walFile.Close()

		segment, err := newSegment(dataFile, walFile, time.Hour, time.Hour)
		require.NoError(t, err)

		key := []byte("key100500")
		_, err = segment.Get(hash(key), key)
		require.ErrorIs(t, err, ErrNotFound)

		err = segment.Close()
		require.NoError(t, err)
	})
}

func TestWritesAndDeletes(t *testing.T) {
	t.Run("set one key", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		dataFileName := dataFile.Name()

		require.NoError(t, err)

		defer os.Remove(dataFile.Name())

		walFile, err := os.CreateTemp(dir, "*")
		require.NoError(t, err)

		walFileName := walFile.Name()

		defer os.Remove(walFile.Name())

		segment, err := newSegment(dataFile, walFile, time.Hour, time.Hour)
		require.NoError(t, err)

		key := []byte("key1")
		value := []byte("value1")

		err = segment.Set(hash(key), key, value, 0)
		require.NoError(t, err)

		expectedValue := value

		value, err = segment.Get(hash(key), key)
		require.NoError(t, err)

		require.Equal(t, expectedValue, value)

		err = segment.Close()
		require.NoError(t, err)

		dataFile, err = os.OpenFile(dataFileName, os.O_RDWR, 0644)
		require.NoError(t, err)
		walFile, err = os.OpenFile(walFileName, os.O_RDWR|os.O_SYNC|os.O_APPEND, 0644)
		require.NoError(t, err)

		segment, err = newSegment(dataFile, walFile, time.Hour, time.Hour)
		require.NoError(t, err)

		value, err = segment.Get(hash(key), key)
		require.NoError(t, err)

		require.Equal(t, expectedValue, value)

		err = segment.Close()
		require.NoError(t, err)
	})

	t.Run("set one key then del key", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		dataFileName := dataFile.Name()

		require.NoError(t, err)

		defer os.Remove(dataFile.Name())

		walFile, err := os.CreateTemp(dir, "*")
		require.NoError(t, err)

		walFileName := walFile.Name()

		defer os.Remove(walFile.Name())

		segment, err := newSegment(dataFile, walFile, time.Hour, time.Hour)
		require.NoError(t, err)

		key := []byte("key1")
		value := []byte("value1")

		err = segment.Set(hash(key), key, value, 0)
		require.NoError(t, err)

		err = segment.Delete(hash(key), key)
		require.NoError(t, err)

		_, err = segment.Get(hash(key), key)
		require.ErrorIs(t, err, ErrNotFound)

		err = segment.Close()
		require.NoError(t, err)

		dataFile, err = os.OpenFile(dataFileName, os.O_RDWR, 0644)
		require.NoError(t, err)
		walFile, err = os.OpenFile(walFileName, os.O_RDWR|os.O_SYNC|os.O_APPEND, 0644)
		require.NoError(t, err)

		segment, err = newSegment(dataFile, walFile, time.Hour, time.Hour)
		require.NoError(t, err)

		_, err = segment.Get(hash(key), key)
		require.ErrorIs(t, err, ErrNotFound)

		err = segment.Close()
		require.NoError(t, err)
	})

	t.Run("set one key then del key", func(t *testing.T) {
		dir := os.TempDir()

		dataFile, err := os.CreateTemp(dir, "*")
		require.NoError(t, err)

		defer os.Remove(dataFile.Name())

		walFile, err := os.CreateTemp(dir, "*")
		require.NoError(t, err)

		defer os.Remove(walFile.Name())

		segment, err := newSegment(dataFile, walFile, time.Hour, time.Hour)
		require.NoError(t, err)

		key := []byte("key1")

		err = segment.Delete(hash(key), key)
		require.ErrorIs(t, err, ErrNotFound)

		err = segment.Close()
		require.NoError(t, err)
	})

	t.Run("set expired, clear expired, check not found", func(t *testing.T) {
		dir := os.TempDir()

		dataFile, err := os.CreateTemp(dir, "*")
		require.NoError(t, err)

		defer os.Remove(dataFile.Name())

		dataFileName := dataFile.Name()

		walFile, err := os.CreateTemp(dir, "*")
		require.NoError(t, err)

		walFileName := walFile.Name()

		defer os.Remove(walFile.Name())

		segment, err := newSegment(dataFile, walFile, time.Hour, time.Hour)
		require.NoError(t, err)

		key := []byte("key1")
		value := []byte("value1")

		now := time.Now()
		expireTime := now.Add(-time.Second) // already expired

		err = segment.Set(hash(key), key, value, uint32(expireTime.Unix()))
		require.NoError(t, err)

		// run delete expired items
		segment.collectExpiredItems()

		_, err = segment.Get(hash(key), key)
		require.ErrorIs(t, err, ErrNotFound)

		err = segment.Delete(hash(key), key)
		require.ErrorIs(t, err, ErrNotFound)

		err = segment.Close()
		require.NoError(t, err)

		dataFile, err = os.OpenFile(dataFileName, os.O_RDWR, 0644)
		require.NoError(t, err)
		walFile, err = os.OpenFile(walFileName, os.O_RDWR|os.O_SYNC|os.O_APPEND, 0644)
		require.NoError(t, err)

		segment, err = newSegment(dataFile, walFile, time.Hour, time.Hour)
		require.NoError(t, err)

		_, err = segment.Get(hash(key), key)
		require.ErrorIs(t, err, ErrNotFound)

		err = segment.Close()
		require.NoError(t, err)
	})
}

func TestWorkWithoutWAL(t *testing.T) {
	t.Run("set and get and durability check", func(t *testing.T) {
		dir := os.TempDir()

		dataFile, err := os.CreateTemp(dir, "*")
		require.NoError(t, err)

		defer os.Remove(dataFile.Name())

		dataFileName := dataFile.Name()

		segment, err := newSegment(dataFile, nil, time.Hour, time.Hour)
		require.NoError(t, err)

		key := []byte("key1")
		value := []byte("value1")

		err = segment.Set(hash(key), key, value, 0)
		require.NoError(t, err)

		realValue, err := segment.Get(hash(key), key)
		require.NoError(t, err)

		require.Equal(t, value, realValue)

		err = segment.Close()
		require.NoError(t, err)

		// reopen data file and segment and check old key value
		dataFile, err = os.Open(dataFileName)
		require.NoError(t, err)

		segment, err = newSegment(dataFile, nil, time.Hour, time.Hour)
		require.NoError(t, err)

		realValue, err = segment.Get(hash(key), key)
		require.NoError(t, err)

		require.Equal(t, value, realValue)
	})

	t.Run("restore state from existing file and serve get", func(t *testing.T) {
		dir := os.TempDir()

		dataFile, err := os.CreateTemp(dir, "*")
		require.NoError(t, err)

		defer os.Remove(dataFile.Name())

		key := []byte("key1")
		value := []byte("value1")

		data := map[string]v{
			string(key): {
				value:  value,
				expire: 0,
				lsn:    0,
			},
		}

		err = makeSegment(dataFile, data)
		require.NoError(t, err)

		segment, err := newSegment(dataFile, nil, time.Hour, time.Hour)
		require.NoError(t, err)

		realValue, err := segment.Get(hash(key), key)
		require.NoError(t, err)

		require.Equal(t, value, realValue)
	})

	t.Run("set expired key, run expired check and fsync", func(t *testing.T) {
		dir := os.TempDir()

		dataFile, err := os.CreateTemp(dir, "*")
		require.NoError(t, err)

		defer os.Remove(dataFile.Name())

		key := []byte("key1")
		value := []byte("value1")

		segment, err := newSegment(dataFile, nil, time.Hour, time.Hour)
		require.NoError(t, err)

		now := time.Now()
		expiredTime := now.Add(-time.Hour)

		err = segment.Set(hash(key), key, value, uint32(expiredTime.Unix()))
		require.NoError(t, err)

		segment.rawCollectExpiredItems()
		segment.rawFsync()

		_, err = segment.Get(hash(key), key)
		require.ErrorIs(t, err, ErrNotFound)
	})
}
