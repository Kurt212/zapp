package zapp

import "errors"

var (
	ErrNotFound = errors.New("key not found")

	ErrSegmentMagicNumbersDoNotMatch = errors.New("file magic numbers do not match")
	ErrSegmentUnknownVersionNumber   = errors.New("unknown version number of segment file")
	ErrUnknownBlobStatus             = errors.New("unkown blob status")
)
