package chord

import (
	"fmt"
)

const (
	helpInfo string = "There is no help info, you are on your own"
)

type NodeConsole struct {
	myHelpInfo string
	node       *ringNode
}

func NewNodeConsole(port int32) *NodeConsole {
	cmd := new(NodeConsole)
	cmd.node = NewNode(port)
	cmd.myHelpInfo = helpInfo
	return cmd
}

func (c *NodeConsole) PrintHelp() {
	fmt.Println(c.myHelpInfo)
}

func Run() {
	cmd := NewNodeConsole(114514)
	cmd.PrintHelp()
	cmd.node.PrintName()
}
