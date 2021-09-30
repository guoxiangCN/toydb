1. Open a DB on a dir path

```go
db, err := Open("D:/toydb")
if err != nil {
	log.Fatalln("Error to open toydb")
}
```

2. Put K-V pairs

   ```go
var key = fmt.Sprintf("storemelf_userid_%d", i)
var val = fmt.Sprintf("newvalue_______%d", i)
err := db.Put([]byte(key), []byte(val))
if err != nil {
	fmt.Println("put error")
}
   ```

3. Get Value by Key

   ```go
for i := 0; i < 1000000; i++ {
	var key = fmt.Sprintf("storemelf_userid_%d", i)
	value, err := db.Get([]byte(key))
	if err!=nil {
		log.Fatalln("Get error")
	}
	fmt.Printf("key: %s, value: %s\n", key, string(value))
}
   ```

4. Delete a Key-Value Pair

   ```go
var key = fmt.Sprintf("storemelf_userid_%d", 1111112334)
err = db.Del([]byte(key))
if err != nil {
	log.Fatalln("Del error")
}
   ```

   

