package zapp

import (
	"fmt"
	"os"
	"time"
)

type DB struct {
	segments []*segment
}

func New() (*DB, error) {
	const segmentsCount = 4
	const dirName = "data"
	const syncTime = time.Second

	_, err := os.Stat(dirName)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.Mkdir(dirName, 0755)
			if err != nil {
				return nil, fmt.Errorf("can not create %s dir: %w", dirName, err)
			}
		} else {
			return nil, fmt.Errorf("can not check %s dir existance: %w", dirName, err)
		}
	}

	var segments []*segment
	for i := 0; i < segmentsCount; i++ {
		segPath := fmt.Sprintf("%s/%d.bin", dirName, i)

		seg, err := newSegment(segPath)
		if err != nil {
			return nil, fmt.Errorf("can not create segment %s: %w", segPath, err)
		}

		segments = append(segments, seg)
	}

	db := &DB{
		segments: segments,
	}

	return db, nil
}

func (db *DB) Set(key string, data []byte, ttl time.Duration) error {
	byteKey := []byte(key)

	h := hash(byteKey)
	segment := db.getSegmentForKey(h)

	err := segment.Set(h, byteKey, data, ttl)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) Get(key string) ([]byte, error) {
	byteKey := []byte(key)

	h := hash(byteKey)
	segment := db.getSegmentForKey(h)

	data, err := segment.Get(h, byteKey)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (db *DB) Delete(key string) error {
	byteKey := []byte(key)

	h := hash(byteKey)
	segment := db.getSegmentForKey(h)

	err := segment.Delete(h, byteKey)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) Close() error {
	for idx, segment := range db.segments {
		err := segment.Close()
		if err != nil {
			return fmt.Errorf("close segment %d: %w", idx, err)
		}
	}

	return nil
}

func (db *DB) getSegmentForKey(hash uint32) *segment {
	segmentIdx := getSegmentIndex(hash, len(db.segments))

	segment := db.segments[segmentIdx]

	return segment
}

func getSegmentIndex(hash uint32, segmentsCount int) int {
	return int(hash % uint32(segmentsCount))
}
