package toydb

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {
	db, err := Open("D:/toydb")
	if err != nil {
		log.Fatalln("Error to open toydb")
	}
	for i := 0; i < 10000000; i++ {
		var key = fmt.Sprintf("testkey%d", i)
		var val = fmt.Sprintf("redisvalue%d", i)
		err := db.Put([]byte(key), []byte(val))
		if err != nil {
			fmt.Println("put error")
		}
	}

	fmt.Println("DB size: ", db.Size())
	fmt.Println("DB size: ", db.Size())

	for i := 0; i < 1000000; i++ {
		var key = fmt.Sprintf("storemelf_userid_%d", i)
		value, err := db.Get([]byte(key))
		if err != nil {
			log.Fatalln("Get error")
		}
		fmt.Printf("key: %s, value: %s\n", key, string(value))
	}

	defer db.Close()
}

func TestToyDB_Put(t *testing.T) {
	db, err := Open("D:/toydb")
	if err != nil {
		log.Fatalln("Error to open toydb")
	}
	_ = db.Put([]byte("test_index_data"), []byte("aabbccddeeff@1996"))
	value, err := db.Get([]byte("key1"))
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println(string(value))
	}
}

func TestToyDB_Get2(t *testing.T) {
	db, err := Open("D:/toydb")
	if err != nil {
		log.Fatalln("Error to open toydb")
	}

	value, err := db.Get([]byte("test_index_data"))
	if err == nil {
		fmt.Println(string(value))
	}

	db.Close()
	for {
		time.Sleep(1 * time.Second)
	}
}

func TestToyDB_Del(t *testing.T) {
	db, err := Open("D:/toydb")
	if err != nil {
		log.Fatalln("Error to open toydb")
	}
	for i := 0; i < 10000000; i++ {
		var key = fmt.Sprintf("testkey%d", i)
		err := db.Del([]byte(key))
		if err != nil {
			t.Fatal("delete failed", err)
		}
	}
}

func TestToyDB_Get(t *testing.T) {
	startMili := time.Now().UnixMilli()
	db, err := Open("D:/toydb")

	endMili := time.Now().UnixMilli()
	fmt.Println("REBUILD INDEX elapsed ms:", endMili-startMili)

	if err != nil {
		log.Fatalln("Error to open toydb")
	}
	for i := 0; i < 100; i++ {
		var key = fmt.Sprintf("testkey%d", i)
		value, err := db.Get([]byte(key))
		if err != nil {
			log.Fatalln("Get error")
		}
		fmt.Printf("key: %s, value: %s\n", key, value)
	}
}

func TestToyDB_Vacuum(t *testing.T) {
	db, err := Open("D:/toydb_testvacuum")
	if err != nil {
		log.Fatalln("Open DB failed...")
	}

	if err!= nil {
		log.Fatal("Vacuum DB failed...",err)
	}

	for i := 0; i < 10000; i++ {
		var key = fmt.Sprintf("testkey%d", i)
		var val = fmt.Sprintf("redisvalue%d", i)
		err := db.Put([]byte(key), []byte(val))
		if err != nil {
			fmt.Println("put error")
		}
	}

	for i := 0; i < 9999; i++ {
		var key = fmt.Sprintf("testkey%d", i)
		err := db.Del([]byte(key))
		if err != nil {
			log.Fatalln("Get error")
		}
	}

	err = db.Vacuum()
	if err != nil {
		t.Fatal("Vacuum failed...")
	}
	// put 10000  del 10000
	// 理论上vacuum后为空
	get, err := db.Get([]byte("testkey9999"))
	fmt.Println(string(get))
}
