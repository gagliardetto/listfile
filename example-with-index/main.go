package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gagliardetto/hashsearch"
	"github.com/gagliardetto/listfile"
	"github.com/mitchellh/hashstructure"
	"github.com/petar/GoLLRB/llrb"
	"github.com/pkg/profile"
)

type ItemUint64 uint64

func (oid ItemUint64) Less(than llrb.Item) bool {
	return oid < than.(ItemUint64)
}

func HasByNumericHash(index *llrb.LLRB, s string) bool {
	numericHash, err := hashstructure.Hash(s, nil)
	if err != nil {
		panic(err)
	}

	return index.Has(ItemUint64(numericHash))
}
func main() {
	defer profile.Start(profile.MemProfile).Stop()
	{
		start := time.Now()

		batchWithoutIndex, err := listfile.NewWithoutIndex("./master-sab3ago2019.out.batch.txt")
		if err != nil {
			panic(err)
		}
		defer batchWithoutIndex.Close()

		reg := hashsearch.New()

		batchWithoutIndex.IterateLines(func(line string) bool {
			reg.WarningUnorderedAppend(line)
			return true
		})
		reg.Sort()
		fmt.Println("took:", time.Since(start))

		start = time.Now()

		fmt.Println(reg.Has("TODO"))

		fmt.Println("took:", time.Since(start))

		time.Sleep(time.Minute)
	}
	return
	{
		start := time.Now()
		index := llrb.New()

		batchWithoutIndex, err := listfile.NewWithoutIndex("./master-sab3ago2019.out.batch.txt")
		if err != nil {
			panic(err)
		}
		defer batchWithoutIndex.Close()

		var hashes []uint64

		batchWithoutIndex.IterateLines(func(line string) bool {
			numericHash, err := hashstructure.Hash(line, nil)
			if err != nil {
				panic(err)
			}
			hashes = append(hashes, numericHash)
			index.InsertNoReplace(ItemUint64(numericHash))
			return true
		})
		fmt.Println("took:", time.Since(start))

		start = time.Now()

		fmt.Println(HasByNumericHash(index, "TODO"))

		fmt.Println("took:", time.Since(start))

		time.Sleep(time.Minute)
	}
	return

	{
		start := time.Now()
		batch, err := listfile.NewWithRootIndex("./master-sab3ago2019.out.batch.txt")
		if err != nil {
			panic(err)
		}
		fmt.Println("took:", time.Since(start))
		defer batch.Close()

		start = time.Now()
		fmt.Println(batch.HasStringLine("TODO"))
		fmt.Println("took:", time.Since(start))
		time.Sleep(time.Minute)
	}
	return

	lst, err := listfile.NewWithRootIndex("./example-with-index.txt")
	if err != nil {
		panic(err)
	}
	defer lst.Close()
	type Object struct {
		ID int
	}

	if true {
		for iPar := 0; iPar < 100; iPar++ {
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
	query := 44
	fmt.Println("has", query, lst.HasIntByIndex(indexName, query))
	fmt.Println("took to ask to index:", time.Now().Sub(start))

	start = time.Now()
	query = 60
	fmt.Println("has", query, lst.HasIntByIndex(indexName, query))
	fmt.Println("took to ask to index:", time.Now().Sub(start))

	start = time.Now()
	query = 34
	fmt.Println("has", query, lst.HasIntByIndex(indexName, query))
	fmt.Println("took to ask to index:", time.Now().Sub(start))

	start = time.Now()
	query = 45
	fmt.Println("has", query, lst.HasIntByIndex(indexName, query))
	fmt.Println("took to ask to index:", time.Now().Sub(start))

	start = time.Now()
	query = 102
	fmt.Println("has", query, lst.HasIntByIndex(indexName, query))
	fmt.Println("took to ask to index:", time.Now().Sub(start))

	err = lst.Close()
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Hour)
}
