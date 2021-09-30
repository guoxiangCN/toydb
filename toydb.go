package toydb

import (
	"errors"
	"io"
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

	err = db.reloadIndexes()
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
	db.lock.Lock()
	defer db.lock.Unlock()
	return int64(len(db.indexes))
}

func (db *ToyDB) Vacuum() error {
	db.lock.RLock()
	if db.isVacuum {
		db.lock.RUnlock()
		return ErrVacuumAlreadyInProgress
	}

	db.lock.RUnlock()
	db.lock.Lock()
	defer db.lock.Unlock()

	// start vacuum processing TODO

	return nil
}

//func (db *ToyDB) StartBgVacuum(ctx context.Context) {
//	go func() {
//		time.Sleep(60 * time.Second)
//		err := db.Vacuum()
//		if err != nil {
//			panic(err)
//		}
//	}()
//}

func (db *ToyDB) Close() {
	_ = db.Vacuum()
	_ = db.dataFile.File.Close()
	// make gc collectors work here.
	db.indexes = make(map[string]indexData)
}
