package main

import "fmt"

type A struct {
	X string
	Y string
}

func main() {
	obj := A{Y: "abc"}

	fmt.Println(len(obj.X))
}
