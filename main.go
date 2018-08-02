package main

import (
	"chordNode"
	"fmt"
)

func main_old() {
	var i int
	fmt.Scanf("%d", &i)
	cmd := chordNode.NewNodeConsole(int32(i))
	cmd.Run()
}
