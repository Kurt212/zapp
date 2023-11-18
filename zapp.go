package zapp

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

type DB struct {
	segments []*segment
}

func New(params Params) (*DB, error) {
	if err := validateParams(params); err != nil {
		return nil, err
	}

	// first create directory for storing files
	_, err := os.Stat(params.dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.Mkdir(params.dataPath, 0755)
			if err != nil {
				return nil, fmt.Errorf("can not create %s dir: %w", params.dataPath, err)
			}
		} else {
			return nil, fmt.Errorf("can not check %s dir existance: %w", params.dataPath, err)
		}
	}

	// then open existing/create N segment files
	var segments []*segment
	for i := 0; i < params.segmentsNum; i++ {
		segPath := fmt.Sprintf("%s/%d_data.bin", params.dataPath, i)

		// open for read and write
		// create file from scratch if it did not exist
		file, err := os.OpenFile(segPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("can not open file %s: %w", segPath, err)
		}

		var walFile *os.File // nil by default. nil => do not use wal logic
		if params.useWAL {
			walPath := fmt.Sprintf("%s/%d_wal.bin", params.dataPath, i)
			// wal should be readable and writable
			// if wal file doesn't exist, then it will be created
			// wal file is append only
			// writes to wal file should be synchronous! This is extremely important.
			walFile, err = os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND|os.O_SYNC, 0644)
			if err != nil {
				return nil, fmt.Errorf("can not open wal file %s: %w", walPath, err)
			}
		}

		// randomize syncPeriod so that each segment will process expired items with random delay depending on params
		syncPeriod := time.Duration(0)
		if params.removeExpiredPeriod > 0 {
			syncPeriod = generateNewPeriodWithRandomDelta(params.syncPeriod, params.syncPeriodDeltaMax)
		}

		// randomize expiredPeriod so that each segment will process expired items with random delay depending on params
		expiredPeriod := time.Duration(0)
		if params.removeExpiredPeriod > 0 {
			expiredPeriod = generateNewPeriodWithRandomDelta(params.removeExpiredPeriod, params.removeExpiredDeltaMax)
		}

		seg, err := newSegment(file, walFile, expiredPeriod, syncPeriod)
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

	now := time.Now()

	expireTime := uint32(0)
	if ttl.Milliseconds() > 0 {
		expireTime = uint32(now.Add(ttl).Unix())
	}

	err := segment.Set(h, byteKey, data, expireTime)
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

func (db *DB) Close() {
	wg := sync.WaitGroup{}
	for _, s := range db.segments {
		wg.Add(1)
		go func(s *segment) {
			defer wg.Done()
			s.Close()
		}(s)
	}

	wg.Wait()
}

func (db *DB) getSegmentForKey(hash uint32) *segment {
	segmentIdx := getSegmentIndex(hash, len(db.segments))

	segment := db.segments[segmentIdx]

	return segment
}

func getSegmentIndex(hash uint32, segmentsCount int) int {
	return int(hash % uint32(segmentsCount))
}

func generateNewPeriodWithRandomDelta(period time.Duration, maxDelta time.Duration) time.Duration {
	delta := time.Duration(rand.Int63n(int64(maxDelta)))
	return period + delta
}
