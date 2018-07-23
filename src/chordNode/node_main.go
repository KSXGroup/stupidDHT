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
	ipAddress     string
	port          int32
	hashedAddress [256]byte
	messageQueue  chan ctrlMessage
	currentMsg    ctrlMessage
	status        uint8
}

func (n *ringNode) PrintName() {
	fmt.Println(n.ipAddress)
}

func (n *ringNode) Hash(ip string, port int32) {}

func NewNode(port int32) *ringNode {
	var ret = new(ringNode)
	ret.getIp()
	ret.port = port
	ret.status = STOP
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
	fmt.Println(msg.name)
	if msg.name == "exit" {
		close(n.messageQueue)
		n.status = STOP
	}
}

func (n *ringNode) Run() {
	n.messageQueue = make(chan ctrlMessage, int32(MAX_QUEUE_LEN))
	n.status = RUNNING
	for {
		if len(n.messageQueue) > 0 {
			n.currentMsg = <-n.messageQueue
			n.handleMsg(&n.currentMsg)
		}
	}
}
