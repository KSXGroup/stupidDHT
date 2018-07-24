// node
package chordNode

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"net"
	"strconv"
)

const (
	MAX_QUEUE_LEN         int32 = 1024
	HASHED_ADDRESS_LENGTH int32 = 160
	STOP                  uint8 = 0
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
	hashModAddress  big.Int
	currentMsg      ctrlMessage
	messageQueueIn  chan ctrlMessage
	messageQueueOut chan ctrlMessage
	ifstop          chan uint8
}

func (n *ringNode) PrintNodeInfo() {
	fmt.Println(n.ipAddress)
	//TODO PRINT FINGER LIST
}

func (n *ringNode) hashAddress() {
	toHash := n.ipAddress + strconv.Itoa(int(n.port))
	hasher := sha1.New()
	tmp := new(big.Int).SetBytes(hasher.Sum([]byte(toHash)))
	n.hashModAddress = *new(big.Int).Mod(tmp, new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(HASHED_ADDRESS_LENGTH)), nil))
}

func NewNode(port int32) *ringNode {
	var ret = new(ringNode)
	ret.getIp()
	ret.port = port
	ret.hashAddress()
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
	welmsg := NewCtrlMsg("The node on ip "+n.ipAddress+":"+strconv.Itoa(int(n.port)), 0)
	n.messageQueueOut <- *welmsg
	for {
		if len(n.messageQueueIn) > 0 {
			n.currentMsg = <-n.messageQueueIn
			n.handleMsg(&n.currentMsg)
		}
	}
}
