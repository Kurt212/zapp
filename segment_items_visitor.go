package zapp

import (
	"io"
	"os"

	"github.com/Kurt212/zapp/blob"
)

// visitOnDiskItems implements visitor pattern
// it's very low level and gives the caller the ability to visit each item on disk and call some visitorFunc
// by default visitor doesn't read item's body, a caller has to read it from file himself using file, current offset and item's header data
// the function returns last offset in file where it stopped. By default the returned offset is the end of the file.
func (s *segment) visitOnDiskItems(
	visitorFunc func(file *os.File, offset int64, header blob.Header) error,
) (lastOffset int64, _ error) {
	// calling function must aquire segment's mutex itself, if needed
	// this function is too low level
	// it provides a generic visitor pattern way to read whole segment's file on disk
	// and do something with each item
	// if you need to access item's value or key, you must read it from file yourself
	// by default only item's header is read and passed to the visitor function

	// the beginning of the first item on dist is at fixed offset after file header bytes
	currentOffset := int64(segmentFileHeaderSize)

	for {
		// read fixed sized header
		blobHeaderBuffer := make([]byte, blob.HeaderSize)

		_, err := s.file.ReadAt(blobHeaderBuffer, currentOffset)
		if err != nil {
			if err == io.EOF {
				break
			}

			return currentOffset, err
		}

		blobHeader := blob.UnmarshalHeader(blobHeaderBuffer)

		blobSize := blobHeader.Size()

		// pass all needed data to visitor function, so it can do whatever it wants with this item
		// if visitor function return an error, finish visiting process and return the error
		// when the error happens, return current item's offset at which the error happend.
		err = visitorFunc(s.file, currentOffset, blobHeader)
		if err != nil {
			return currentOffset, err
		}

		currentOffset += int64(blobSize)
	}

	lastOffset = currentOffset

	return lastOffset, nil
}
