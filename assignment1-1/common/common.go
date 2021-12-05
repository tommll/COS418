package common

import (
	"fmt"
)

// Propagate error if it exists
func CheckError(err error, msg string) {
	if err != nil {
		fmt.Println("ERROR: " + msg + err.Error())
		panic(err)
	}
}
