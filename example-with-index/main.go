package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gagliardetto/listfile"
)

func main() {
	lst, err := listfile.New("./example-with-index.txt")
	if err != nil {
		panic(err)
	}
	defer lst.Close()
	type Object struct {
		ID int
	}

	if true {
		for iPar := 0; iPar < 500000; iPar++ {
			o := &Object{
				ID: iPar,
			}
			res, err := json.Marshal(o)
			if err != nil {
				panic(err)
			}
			err = lst.AppendBytes(res)
			if err != nil {
				panic(err)
			}
		}
	}

	time.Sleep(time.Second)
	start := time.Now()
	lst.IterateLinesAsBytes(func(line []byte) bool {
		var obj Object
		err := json.Unmarshal(line, &obj)
		if err != nil {
			panic(err)
		}
		// WARNING: FreeOSMemory() (i.e. forced garbage collection) takes a REALLY long time
		//rtdebug.FreeOSMemory()
		return true
	})
	fmt.Println("took to iterate over all object and parse them:", time.Now().Sub(start))

	indexName := "index-by-id"
	start = time.Now()
	lst.CreateIndexOnInt(indexName, func(line []byte) int {
		var obj Object
		err := json.Unmarshal(line, &obj)
		if err != nil {
			panic(err)
		}
		return obj.ID
	})
	fmt.Println("took to create index:", time.Now().Sub(start))

	start = time.Now()
	query := 444
	fmt.Println("has", query, lst.HasIntByIndex(indexName, query))
	fmt.Println("took to ask to index:", time.Now().Sub(start))

	start = time.Now()
	query = 600000
	fmt.Println("has", query, lst.HasIntByIndex(indexName, query))
	fmt.Println("took to ask to index:", time.Now().Sub(start))

	time.Sleep(time.Second * 10)

	start = time.Now()
	query = 346346
	fmt.Println("has", query, lst.HasIntByIndex(indexName, query))
	fmt.Println("took to ask to index:", time.Now().Sub(start))

	start = time.Now()
	query = 45363743
	fmt.Println("has", query, lst.HasIntByIndex(indexName, query))
	fmt.Println("took to ask to index:", time.Now().Sub(start))

	time.Sleep(time.Hour)
}
