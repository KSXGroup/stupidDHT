// node
package chordNode

import (
	//"crypto/sha256"
	"fmt"
	//"math/big"
	"net"
)

const (
	MAX_QUEUE_LEN int32 = 1024
	STOP          uint8 = 0
	RUNNING       uint8 = 1
)

type ctrlMessage struct {
	name string
	arg  int32
}

func NewCtrlMsg(_n string, _arg int32) *ctrlMessage {
	tmsg := new(ctrlMessage)
	tmsg.arg = _arg
	tmsg.name = _n
	return tmsg
}

type ringNode struct {
	ipAddress       string
	port            int32
	hashedAddress   [256]byte
	messageQueueIn  chan ctrlMessage
	messageQueueOut chan ctrlMessage
	currentMsg      ctrlMessage
	ifstop          chan uint8
}

func (n *ringNode) PrintName() {
	fmt.Println(n.ipAddress)
}

func (n *ringNode) Hash(ip string, port int32) {}

func NewNode(port int32) *ringNode {
	var ret = new(ringNode)
	ret.getIp()
	ret.port = port
	return ret
}

func (n *ringNode) getIp() {
	addrList, err := net.InterfaceAddrs()
	if err != nil {
		panic("Fail to get IP address")
	}
	for _, a := range addrList {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				n.ipAddress = ipnet.IP.String()
			}
		}
	}
}

func (n *ringNode) handleMsg(msg *ctrlMessage) {
	tmp := NewCtrlMsg("The message "+msg.name+" is handled", 1)
	tmp.arg = 1
	n.messageQueueOut <- *tmp
	switch msg.name {
	case "exit":
		close(n.messageQueueOut)
		close(n.messageQueueIn)
		n.ifstop <- STOP
		fmt.Print("ServerStopped\n")
		break
	}
	//TODO
}

func (n *ringNode) Run() {
	n.messageQueueIn = make(chan ctrlMessage, 1)
	n.messageQueueOut = make(chan ctrlMessage, 1)
	n.ifstop = make(chan uint8, 1)
	for {
		if len(n.messageQueueIn) > 0 {
			n.currentMsg = <-n.messageQueueIn
			n.handleMsg(&n.currentMsg)
		}
	}
}
