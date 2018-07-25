package chordNode

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

const (
	helpInfo  string = "There is no help info, you are on your own"
	startInfo string = "Node start on ip: "
)

type NodeConsole struct {
	ipt        []string
	node       *ringNode
	currentMsg ctrlMessage
	stopSigPNI chan uint8
}

func NewNodeConsole(port int32) *NodeConsole {
	cmd := new(NodeConsole)
	cmd.node = NewNode(port)
	return cmd
}

func (c *NodeConsole) PrintHelp() {
	fmt.Println(helpInfo)
}

func (c *NodeConsole) processNodeInfo(wg *sync.WaitGroup) {
	var nodeMsg ctrlMessage
	for {
		nodeMsg = <-c.node.nodeMessageQueueOut
		PrintLog("[NODE INFO]" + nodeMsg.name[0])
		if len(c.node.ifStop) > 0 {
			break
		}
	}
	wg.Done()
}

func (c *NodeConsole) processInput(ipt []string) int {
	mmsg := *NewCtrlMsg(ipt, 1)
	if len(mmsg.name) != 0 && mmsg.name[0] != "" {
		switch mmsg.name[0] {
		case "create":
			c.node.userMessageQueueIn <- mmsg
			return 1
			break
		case "join":
		case "quit":
			c.node.userMessageQueueIn <- mmsg
			return 2
			break
		default:
			return 0
		}
	}
	return 0
}

func (c *NodeConsole) Run() int {
	var wg sync.WaitGroup
	wg.Add(1)
	go c.node.Run(&wg)
	wg.Add(1)
	go c.processNodeInfo(&wg)
	reader := bufio.NewReader(os.Stdin)
	var ipt string
	for {
		if len(c.node.ifStop) != 0 {
			return 0
		}
		ipt, _ = reader.ReadString('\n')
		ipt = strings.TrimSpace(ipt)
		ipt = strings.Replace(ipt, "\n", "", -1)
		s := strings.Fields(ipt)
		switch c.processInput(s) {
		case 0:
			PrintLog("Wrong Command")
			break
		case 2:
			wg.Wait()
			return 0
		default:
			break
		}
	}
	return 0
}
