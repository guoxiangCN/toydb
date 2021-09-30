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
		var key = fmt.Sprintf("storemelf_userid_%d", i)
		var val = fmt.Sprintf("newvalue_______%d", i)
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
		if err!=nil {
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
		time.Sleep(1*time.Second)
	}
}

func TestToyDB_Del(t *testing.T) {
	var e []byte
	fmt.Println(len(e))
}

func TestToyDB_Get(t *testing.T) {
	startMili := time.Now().UnixMilli()
	db, err := Open("D:/toydb")

	endMili := time.Now().UnixMilli()
	fmt.Println("REBUILD INDEX elapsed ms:", endMili-startMili)

	if err != nil {
		log.Fatalln("Error to open toydb")
	}
	for i := 0; i < 1000000; i++ {
		var key = fmt.Sprintf("storemelf_userid_%d", i)
		value, err := db.Get([]byte(key))
		if err!=nil {
			log.Fatalln("Get error")
		}
		fmt.Printf("key: %s, value: %s\n", key, value)
	}
}

func TestToyDB_Vacuum(t *testing.T) {

}
