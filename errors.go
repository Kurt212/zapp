package zapp

import "errors"

var (
	ErrNotFound = errors.New("key not found")

	ErrSegmentMagicNumbersDoNotMatch = errors.New("file magic numbers do not match")
	ErrSegmentUnknownVersionNumber   = errors.New("unknown version number of segment file")
	ErrUnknownBlobStatus             = errors.New("unkown blob status")

	ErrInvalidPath        = errors.New("invalid path for storing data")
	ErrInvalidSegmentsNum = errors.New("invalid number of segments")

	ErrClosed = errors.New("segment is closed")
)
