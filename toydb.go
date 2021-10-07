package toydb

import (
	"errors"
	"io"
	"log"
	"os"
	"sync"
)

var (
	ErrDbLockFileExists        = errors.New("LOCK file already exists")
	ErrKeyEmptyNotAllowed      = errors.New("empty key not allowed")
	ErrVacuumAlreadyInProgress = errors.New("vacuum already in progress")
)

type indexData struct {
	Offset int64
}

type ToyDB struct {
	lock     sync.RWMutex
	dataFile *DBFile
	dirPath  string
	indexes  map[string]indexData
	isVacuum bool
}

func Open(dirPath string) (*ToyDB, error) {
	if _, err := os.Stat(dirPath); err != nil && os.IsNotExist(err) {
		if err := os.Mkdir(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// Try make LOCK file
	// newDBLockFile(dirPath)

	dbDataFile, err := newDBDataFile(dirPath)
	if err != nil {
		return nil, err
	}

	db := &ToyDB{
		dataFile: dbDataFile,
		dirPath:  dirPath,
		indexes:  make(map[string]indexData),
		isVacuum: false,
	}

	log.Print("toydb start to reload indexes")
	err = db.reloadIndexes()
	log.Print("toydb finish reload indexes")
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *ToyDB) reloadIndexes() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if len(db.indexes) > 0 {
		db.indexes = make(map[string]indexData)
	}

	// reload from dbfile
	var offset int64 = 0
	for {
		item, err := db.dataFile.ReadItem(offset)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}

		db.indexes[string(item.Key)] = indexData{Offset: offset}

		if item.Flag == DEL {
			delete(db.indexes, string(item.Key))
		}

		offset += int64(item.Size())
	}
	return nil
}

func (db *ToyDB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmptyNotAllowed
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	offset := db.dataFile.Offset
	item := newDBItem(key, value, PUT)
	if err := db.dataFile.Write(item); err != nil {
		return err
	}

	db.indexes[string(key)] = indexData{Offset: offset}
	return nil
}

func (db *ToyDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyEmptyNotAllowed
	}

	db.lock.RLock()
	defer db.lock.RUnlock()

	index, ok := db.indexes[string(key)]
	if !ok {
		return nil, nil
	}

	// read disk
	item, err := db.dataFile.ReadItem(index.Offset)
	if err != nil {
		return nil, err
	}

	return item.Val, nil
}

func (db *ToyDB) Del(key []byte) error {
	if len(key) == 0 {
		return ErrKeyEmptyNotAllowed
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	_, ok := db.indexes[string(key)]
	if !ok {
		return nil
	}

	item := newDBItem(key, nil, DEL)
	err := db.dataFile.Write(item)
	if err != nil {
		return err
	}

	delete(db.indexes, string(key))
	return nil
}

func (db *ToyDB) Size() int64 {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return int64(len(db.indexes))
}

func (db *ToyDB) Vacuum() error {
	db.lock.Lock()
	defer db.lock.Unlock()
	if db.isVacuum {
		return ErrVacuumAlreadyInProgress
	}

	db.isVacuum = true
	defer func() {
		db.isVacuum = false
	}()

	mergeFile, err := newDBMergeFile(db.dirPath)
	if err != nil {
		return err
	}

	mergeFileName := mergeFile.File.Name()
	defer os.Remove(mergeFileName)

	// 直接使用内存哈希表的索引数据重建
	newIndexes := make(map[string]indexData)

	for key := range db.indexes {
		index := db.indexes[key]
		item, err := db.dataFile.ReadItem(index.Offset)
		if err != nil {
			return err
		}

		// 写入merge文件
		offset := mergeFile.Offset
		err = mergeFile.Write(item)
		if err != nil {
			return err
		}

		// 新的offset放到新索引位置
		newIndexes[key] = indexData{Offset: offset}
	}

	// 删除旧数据文件
	dbFileName := db.dataFile.File.Name()
	_ = db.dataFile.File.Close()
	_ = os.Remove(dbFileName)

	err = mergeFile.File.Close()
	if err != nil {
		return err
	}

	err = os.Rename(mergeFileName, dbFileName)
	if err != nil {
		return err
	}

	// 索引替换
	db.indexes = newIndexes
	// 重新打开文件
	dataFileV2, err := newDBDataFile(db.dirPath)
	if err != nil {
		return err
	}
	db.dataFile = dataFileV2
	return nil
}

func (db *ToyDB) Close() {
	_ = db.Vacuum()
	_ = db.dataFile.File.Close()
	// make gc collectors work here.
	db.indexes = make(map[string]indexData)
}
