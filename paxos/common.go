package paxos

import "fmt"

const debug = 1

func Debug(i int, arg ...interface{}) {
	if i == 1 {
		for _, val := range arg {
			fmt.Print(val)
			fmt.Print(",")
		}
		fmt.Println()
	}

}
