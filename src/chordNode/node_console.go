package chordNode

import (
	"fmt"
)

const (
	helpInfo  string = "There is no help info, you are on your own"
	startInfo string = "Node start on ip: "
)

type NodeConsole struct {
	myHelpInfo string
	node       *ringNode
	ipt        string
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

func (c *NodeConsole) Run() int {
	var ipt string
	go c.node.Run()
	c.PrintLog(startInfo + c.node.ipAddress)
	for {

		c.ipt = ""
		fmt.Print("DHT> ")
		fmt.Scanln(&c.ipt)
		if c.ipt != "" && int32(len(c.node.messageQueue)) != int32(MAX_QUEUE_LEN) {
			c.node.messageQueue <- *NewCtrlMsg(ipt, 0)
		}
	}
	return 0
}
