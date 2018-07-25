// node
package chordNode

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"net"
	"strconv"
	"time"
)

const (
	MAX_QUEUE_LEN         int32 = 1024
	HASHED_ADDRESS_LENGTH int32 = 160
	STOP                  uint8 = 0
)

type ctrlMessage struct {
	name []string
	arg  int32
}

func NewCtrlMsg(_n []string, _arg int32) *ctrlMessage {
	tmsg := new(ctrlMessage)
	tmsg.arg = _arg
	tmsg.name = _n
	return tmsg
}

func NewCtrlMsgFromString(_n string, _arg int32) *ctrlMessage {
	tmsg := new(ctrlMessage)
	tmsg.arg = _arg
	tmsg.name = make([]string, 1)
	tmsg.name[0] = _n
	return tmsg
}

type ringNode struct {
	ipAddress           string
	port                int32
	hashModAddress      big.Int
	currentMsg          ctrlMessage
	nodeFingerTable     *fingerTable
	userMessageQueueIn  chan ctrlMessage
	userMessageQueueOut chan ctrlMessage
	nodeMessageQueueOut chan ctrlMessage
	ifStop              chan uint8
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
	ret.userMessageQueueIn = make(chan ctrlMessage, MAX_QUEUE_LEN)
	ret.userMessageQueueOut = make(chan ctrlMessage, MAX_QUEUE_LEN)
	ret.nodeMessageQueueOut = make(chan ctrlMessage, MAX_QUEUE_LEN)
	ret.ifStop = make(chan uint8, 1)
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
	tmp := NewCtrlMsgFromString("The message "+msg.name[0]+" is handled", 0)
	time.Sleep(1000 * time.Millisecond)
	n.userMessageQueueOut <- *tmp
	switch msg.name[0] {
	case "exit":
		close(n.userMessageQueueOut)
		close(n.userMessageQueueIn)
		n.ifStop <- STOP
		fmt.Print("node stopped\n")
		break
	}
	//TODO
}

func (n *ringNode) ProcessUserCommand() {
	welmsg := NewCtrlMsgFromString("The node start on ip "+n.ipAddress+":"+strconv.Itoa(int(n.port))+" with hashed addresses:"+n.hashModAddress.String(), 1)
	n.nodeMessageQueueOut <- *welmsg
	for {
		n.currentMsg = <-n.userMessageQueueIn
		n.handleMsg(&n.currentMsg)
		if len(n.ifStop) > 0 {
			return
		}
	}
}

func (n *ringNode) create() {}

func (n *ringNode) quit() {}

func (n *ringNode) join() {}

func (n *ringNode) Run() {
	go n.ProcessUserCommand()
}
