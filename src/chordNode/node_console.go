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
	ipt        string
	node       *ringNode
	currentMsg ctrlMessage
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
	go c.node.Run()
	for {
		if len(c.node.messageQueueOut) != 0 {
			break
		}
	}
	c.currentMsg = <-c.node.messageQueueOut
	c.PrintLog(c.currentMsg.name)
	//CODE ABOVE PRINT START INFO
	for {
		if c.ipt != "" {
			c.node.messageQueueIn <- *NewCtrlMsg(c.ipt, 0)
			c.currentMsg = <-c.node.messageQueueOut
			c.PrintLog(c.currentMsg.name)
		}
		if len(c.node.ifstop) > 0 {
			return 0
		}
		c.ipt = ""
		fmt.Print("DHT> ")
		fmt.Scanln(&c.ipt)
	}
	return 0
}
