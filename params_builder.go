package zapp

import "time"

type Params struct {
	segmentsNum           int
	dataPath              string
	syncPeriod            time.Duration
	syncPeriodDeltaMax    time.Duration
	removeExpiredPeriod   time.Duration
	removeExpiredDeltaMax time.Duration
	useWAL                bool
}

type ParamsBuilder struct {
	params Params
}

func NewParamsBuilder(path string) *ParamsBuilder {
	return &ParamsBuilder{
		params: Params{
			segmentsNum:           4,
			dataPath:              path,
			syncPeriod:            time.Minute,
			syncPeriodDeltaMax:    0,
			removeExpiredPeriod:   time.Minute,
			removeExpiredDeltaMax: 0,
			useWAL:                true,
		},
	}
}

func (pb *ParamsBuilder) SegmentsNum(number int) *ParamsBuilder {
	pb.params.segmentsNum = number
	return pb
}

// SyncPeriod sets the period time for scheduling a background process of
// periodic calling fsync on data file and creating checkpoint in WAL.
// 0 value disables the background process running
func (pb *ParamsBuilder) SyncPeriod(period time.Duration) *ParamsBuilder {
	pb.params.syncPeriod = period
	return pb
}

// SyncPeriodDeltaMax sets maximum random delta added to SyncPeriod to generate
// sync period for each segment. Only works if SyncPeriod > 0
func (pb *ParamsBuilder) SyncPeriodDeltaMax(delta time.Duration) *ParamsBuilder {
	pb.params.syncPeriodDeltaMax = delta
	return pb
}

// RemoveExpiredPeriod sets the period time for scheduling a background process of
// periodic marking expired items as deleted in inmemory state.
// 0 value disables the background process running
func (pb *ParamsBuilder) RemoveExpiredPeriod(period time.Duration) *ParamsBuilder {
	pb.params.removeExpiredPeriod = period
	return pb
}

// RemoveExpiredDeltaMax sets maximum random delta added to ExpiredPeriod to generate
// expired check period for each segment. Only works if ExpiredPeriod > 0
func (pb *ParamsBuilder) RemoveExpiredDeltaMax(delta time.Duration) *ParamsBuilder {
	pb.params.removeExpiredDeltaMax = delta
	return pb
}

func (pb *ParamsBuilder) UseWAL(use bool) *ParamsBuilder {
	pb.params.useWAL = use
	return pb
}

func (pb *ParamsBuilder) Params() Params {
	return pb.params
}

func validateParams(p Params) error {
	if p.dataPath == "" {
		return ErrInvalidPath
	}

	if p.segmentsNum <= 0 {
		return ErrInvalidSegmentsNum
	}

	return nil
}
