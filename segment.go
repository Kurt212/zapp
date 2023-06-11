package zapp

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
)

type segment struct {
	file *os.File
	mtx  sync.Mutex
}

type fileDataStructure map[string]string

func newSegment(path string) (*segment, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("can not create segment %s: %w", path, err)
	}

	seg := &segment{
		file: file,
		mtx:  sync.Mutex{},
	}

	return seg, nil
}

func (seg *segment) Set(key string, data []byte) error {
	seg.mtx.Lock()
	defer seg.mtx.Unlock()

	// move to beginning of the file
	seg.file.Seek(0, 0)

	rawData, err := ioutil.ReadAll(seg.file)
	if err != nil {
		return err
	}

	err = seg.file.Truncate(0)
	if err != nil {
		return err
	}

	fileData := make(fileDataStructure)

	json.Unmarshal(rawData, &fileData)

	encodedNewData := base64.RawStdEncoding.EncodeToString(data)

	fileData[key] = encodedNewData

	newFileData, err := json.Marshal(fileData)
	if err != nil {
		return err
	}

	// move to beginning of the file
	seg.file.Seek(0, 0)

	_, err = seg.file.Write(newFileData)
	if err != nil {
		return err
	}

	return nil
}

func (seg *segment) Get(key string) ([]byte, error) {
	seg.mtx.Lock()
	defer seg.mtx.Unlock()

	// move to beginning of the file
	seg.file.Seek(0, 0)

	rawData, err := ioutil.ReadAll(seg.file)
	if err != nil {
		return nil, err
	}

	fileData := make(fileDataStructure)

	json.Unmarshal(rawData, &fileData)

	b64Data, ok := fileData[key]
	if !ok {
		return nil, ErrNotFound
	}

	data, err := base64.RawStdEncoding.DecodeString(b64Data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (seg *segment) Delete(key string) error {

}
