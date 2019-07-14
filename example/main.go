package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gagliardetto/listfile"
)

func main() {
	lst, err := listfile.New("./example.txt")
	if err != nil {
		panic(err)
	}
	defer lst.Close()

	for iPar := 0; iPar < 5; iPar++ {
		ii := strconv.Itoa(iPar)
		go func() {
			for i := 0; i < 10; i++ {
				err = lst.Append(ii + "item" + strconv.Itoa(i))
				if err != nil {
					panic(err)
				}
			}
		}()
	}

	time.Sleep(time.Second)

	lst.IterateLines(func(line string) bool {
		fmt.Println(line)
		return true
	})

	for i := 990; i < 999; i++ {
		err = lst.Append("hello world after reading")
		if err != nil {
			panic(err)
		}
	}
}
