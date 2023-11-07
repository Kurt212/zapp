package zapp

import (
	"os"
	"testing"
	"time"

	"github.com/Kurt212/zapp/wal"
	"github.com/stretchr/testify/assert"
)

func TestOpenFromExistingSegmentFiles(t *testing.T) {
	t.Run("restore from wal 2 sets", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		assert.NoError(t, err)

		defer os.Remove(dataFile.Name())
		defer dataFile.Close()

		walFile, err := os.CreateTemp(dir, "*")

		assert.NoError(t, err)

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
		assert.NoError(t, err)

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
		assert.NoError(t, err)

		segment, err := newSegment(
			dataFile,
			walFile,
			time.Hour,
			time.Hour,
		)
		assert.NoError(t, err)

		assert.Equal(t, uint64(4), segment.lastKnownLSN)

		key := []byte("key3")
		value, err := segment.Get(hash(key), key)

		assert.NoError(t, err)

		assert.Equal(t, []byte("value3"), value)

		key = []byte("key4")
		value, err = segment.Get(hash(key), key)

		assert.NoError(t, err)

		assert.Equal(t, []byte("value4"), value)
	})

	t.Run("restore from wal 2 sets then 1 del", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		assert.NoError(t, err)

		defer os.Remove(dataFile.Name())
		defer dataFile.Close()

		walFile, err := os.CreateTemp(dir, "*")

		assert.NoError(t, err)

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
		assert.NoError(t, err)

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
		assert.NoError(t, err)

		segment, err := newSegment(
			dataFile,
			walFile,
			time.Hour,
			time.Hour,
		)
		assert.NoError(t, err)

		assert.Equal(t, uint64(5), segment.lastKnownLSN)

		key := []byte("key3")
		_, err = segment.Get(hash(key), key)

		assert.ErrorIs(t, err, ErrNotFound)

		key = []byte("key4")
		value, err := segment.Get(hash(key), key)

		assert.NoError(t, err)

		assert.Equal(t, []byte("value4"), value)
	})

	t.Run("restore from wal 2 sets then 1 del then del non existing key", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		assert.NoError(t, err)

		defer os.Remove(dataFile.Name())
		defer dataFile.Close()

		walFile, err := os.CreateTemp(dir, "*")

		assert.NoError(t, err)

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
		assert.NoError(t, err)

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
		assert.NoError(t, err)

		segment, err := newSegment(
			dataFile,
			walFile,
			time.Hour,
			time.Hour,
		)
		assert.NoError(t, err)

		assert.Equal(t, uint64(6), segment.lastKnownLSN)

		key := []byte("key3")
		_, err = segment.Get(hash(key), key)

		assert.ErrorIs(t, err, ErrNotFound)

		key = []byte("key4")
		value, err := segment.Get(hash(key), key)

		assert.NoError(t, err)

		assert.Equal(t, []byte("value4"), value)
	})

	t.Run("restore from wal 2 sets then 1 del then del non existing key then one more set", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		assert.NoError(t, err)

		defer os.Remove(dataFile.Name())
		defer dataFile.Close()

		walFile, err := os.CreateTemp(dir, "*")

		assert.NoError(t, err)

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
		assert.NoError(t, err)

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
		assert.NoError(t, err)

		segment, err := newSegment(
			dataFile,
			walFile,
			time.Hour,
			time.Hour,
		)
		assert.NoError(t, err)

		assert.Equal(t, uint64(7), segment.lastKnownLSN)

		key := []byte("key3")
		_, err = segment.Get(hash(key), key)

		assert.ErrorIs(t, err, ErrNotFound)

		key = []byte("key4")
		value, err := segment.Get(hash(key), key)

		assert.NoError(t, err)

		assert.Equal(t, []byte("value4 new"), value)
	})

	t.Run("restore from wal 2 sets then 1 del then del non existing key then one more set then del", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		assert.NoError(t, err)

		defer os.Remove(dataFile.Name())
		defer dataFile.Close()

		walFile, err := os.CreateTemp(dir, "*")

		assert.NoError(t, err)

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
		assert.NoError(t, err)

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
		assert.NoError(t, err)

		segment, err := newSegment(
			dataFile,
			walFile,
			time.Hour,
			time.Hour,
		)
		assert.NoError(t, err)

		assert.Equal(t, uint64(8), segment.lastKnownLSN)

		key := []byte("key3")
		_, err = segment.Get(hash(key), key)

		assert.ErrorIs(t, err, ErrNotFound)

		key = []byte("key4")
		_, err = segment.Get(hash(key), key)

		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("empty wal no restore", func(t *testing.T) {
		dir := os.TempDir()
		dataFile, err := os.CreateTemp(dir, "*")

		assert.NoError(t, err)

		defer os.Remove(dataFile.Name())
		defer dataFile.Close()

		walFile, err := os.CreateTemp(dir, "*")

		assert.NoError(t, err)

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
		assert.NoError(t, err)

		segment, err := newSegment(
			dataFile,
			walFile,
			time.Hour,
			time.Hour,
		)
		assert.NoError(t, err)

		assert.Equal(t, uint64(3), segment.lastKnownLSN)

		key := []byte("key1")
		value, err := segment.Get(hash(key), key)

		assert.NoError(t, err)

		assert.Equal(t, []byte("value1"), value)

		key = []byte("key2")
		value, err = segment.Get(hash(key), key)

		assert.NoError(t, err)

		assert.Equal(t, []byte("value2"), value)

		key = []byte("key3")
		value, err = segment.Get(hash(key), key)

		assert.NoError(t, err)

		assert.Equal(t, []byte("value3"), value)
	})
}
