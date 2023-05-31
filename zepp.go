package zepp

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

func (db *DB) Set(key string, data []byte) error {
	segment := db.getSegmentForKey(key)

	err := segment.Set(key, data)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) Get(key string) ([]byte, error) {
	segment := db.getSegmentForKey(key)

	data, err := segment.Get(key)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (db *DB) Delete(key string) error {
	segment := db.getSegmentForKey(key)

	data, err := segment.Delete(key)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) getSegmentForKey(key string) *segment {
	segmentIdx := getSegmentIndex(key, len(db.segments))

	segment := db.segments[segmentIdx]

	return segment
}

func getSegmentIndex(key string, segmentsCount int) int {
	h := hash([]byte(key))
	return int(h % uint32(segmentsCount))
}
