package toydb

import (
	"os"
)

const LOCK_FILE = "LOCK"
const DATA_FILE = "DATA"
const MERGE_FILE = "DATA.MER"

type DBFile struct {
	File   *os.File
	Offset int64
}

func newDBDataFile(basePath string) (*DBFile, error) {
	datafile := basePath + string(os.PathSeparator) + DATA_FILE
	return newFileImpl(datafile)
}

func newDBMergeFile(basePath string) (*DBFile, error) {
	datafile := basePath + string(os.PathSeparator) + MERGE_FILE
	return newFileImpl(datafile)
}

func newFileImpl(filepath string) (*DBFile, error) {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(filepath)
	if err != nil {
		return nil, err
	}

	return &DBFile{
		File:   file,
		Offset: stat.Size(),
	}, nil
}

func (df *DBFile) Write(e *DbItem) error {
	encode, err := e.Encode()
	if err != nil {
		return err
	}

	_, err = df.File.WriteAt(encode, df.Offset)
	if err != nil {
		return err
	}

	df.Offset += int64(e.Size())
	return nil
}

func (df *DBFile) ReadItem(offset int64) (*DbItem, error) {
	buf := make([]byte, DbItemHdrSize)
	if _, err := df.File.ReadAt(buf, offset); err != nil {
		return nil, err
	}

	item, err := DecodeDbItemHdr(buf)
	if err != nil {
		return nil, err
	}

	offset += DbItemHdrSize
	if item.KeySize > 0 {
		key := make([]byte, item.KeySize)
		if _, err := df.File.ReadAt(key, offset); err != nil {
			return nil, err
		}
		item.Key = key
	}

	offset += int64(item.KeySize)
	if item.ValSize > 0 {
		value := make([]byte, item.ValSize)
		if _, err := df.File.ReadAt(value, offset); err != nil {
			return nil, err
		}
		item.Val = value
	}
	return item, nil
}
