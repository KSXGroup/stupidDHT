package chordNode

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

const (
	helpInfo  string = "There is no help info, you are on your own"
	startInfo string = "Node start on ip: "
)

type NodeConsole struct {
	ipt        []string
	node       *ringNode
	currentMsg ctrlMessage
}

func NewNodeConsole(port int32) *NodeConsole {
	cmd := new(NodeConsole)
	cmd.node = NewNode(port)
	return cmd
}

func (c *NodeConsole) PrintHelp() {
	fmt.Println(helpInfo)
}

func (c *NodeConsole) processNodeInfo() {
	var nodeMsg ctrlMessage
	for len(c.node.ifStop) == 0 {
		nodeMsg = <-c.node.userMessageQueueOut
		PrintLog("[NODE INFO]" + nodeMsg.name[0])
	}
	fmt.Println("PNI EXIT")
}

func (c *NodeConsole) processUserInfo() {
	var nodeMsg ctrlMessage
	for len(c.node.ifStop) == 0 {
		nodeMsg = <-c.node.nodeMessageQueueOut
		PrintLog("[USER INFO]" + nodeMsg.name[0])
	}
	fmt.Println("PUI EXIT")
}

func (c *NodeConsole) ScanUserCommand() *ctrlMessage {
	scanner := bufio.NewScanner(os.Stdin)
	var s []string
	var ipt string
	for len(c.node.ifStop) == 0 {
		scanner.Scan()
		ipt = scanner.Text()
		if ipt != "" {
			break
		}
		if len(c.node.ifStop) > 0 {
			ipt = ""
			break
		}
	}
	s = strings.Fields(ipt)
	//TODO PREPROCESS THE COMMAND OF USER
	return NewCtrlMsg(s, 0)
}

func (c *NodeConsole) Run() int {
	go c.node.Run()
	go c.processNodeInfo()
	go c.processUserInfo()
	var mmsg ctrlMessage
	for len(c.node.ifStop) == 0 {
		mmsg = *c.ScanUserCommand()
		if len(mmsg.name) != 0 && mmsg.name[0] != "" {
			c.node.userMessageQueueIn <- mmsg
		}
	}
	return 0
}
