package main

import (
	"fmt"

	"github.com/gagliardetto/listfile"
)

func main() {
	{
		file, err := listfile.NewIndexed("./file.json")
		if err != nil {
			panic(err)
		}
		defer file.Close()

		value := "TODO"
		fmt.Println(file.HasStringLine(value))
		err = file.Append(value)
		if err != nil {
			panic(err)
		}
		fmt.Println(file.HasStringLine(value))
	}
}
