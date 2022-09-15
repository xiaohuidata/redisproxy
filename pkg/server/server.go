package server

import (
	"fmt"
)

func Run() {
	RunValues()
	fmt.Println()
	fmt.Println()

	RunLua()
	fmt.Println()
	fmt.Println()

	RunMutex()
	fmt.Println()
	fmt.Println()

	return
}
