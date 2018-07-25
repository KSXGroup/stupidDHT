// node
package chordNode

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"net"
	"strconv"
	"sync"
	//"time"
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
	tmsg.name = make([]string, 10)
	tmsg.name[0] = _n
	return tmsg
}

type ringNode struct {
	info                nodeInfo
	currentMsg          ctrlMessage
	nodeFingerTable     *fingerTable
	userMessageQueueIn  chan ctrlMessage
	nodeMessageQueueOut chan ctrlMessage
	ifStop              chan uint8
}

func (n *ringNode) PrintNodeInfo() {
	fmt.Println(n.info.ipAddress)
	//TODO PRINT FINGER LIST
}

func hashAddress(ip string, port int32) big.Int {
	toHash := ip + strconv.Itoa(int(port))
	hasher := sha1.New()
	tmp := new(big.Int).SetBytes(hasher.Sum([]byte(toHash)))
	hashModAddress := *new(big.Int).Mod(tmp, new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(HASHED_ADDRESS_LENGTH)), nil))
	return hashModAddress
}

func NewNode(port int32) *ringNode {
	var ret = new(ringNode)
	ret.info = *NewNodeInfo(ret.getIp(), port)
	ret.userMessageQueueIn = make(chan ctrlMessage, MAX_QUEUE_LEN)
	ret.nodeMessageQueueOut = make(chan ctrlMessage, MAX_QUEUE_LEN)
	ret.nodeFingerTable = NewFingerTable()
	ret.ifStop = make(chan uint8, 1)
	return ret
}

func (n *ringNode) getIp() string {
	var ipAddress string
	addrList, err := net.InterfaceAddrs()
	if err != nil {
		panic("Fail to get IP address")
	}
	for _, a := range addrList {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ipAddress = ipnet.IP.String()
			}
		}
	}
	return ipAddress
}

func (n *ringNode) handleMsg(msg *ctrlMessage) {
	tmp := NewCtrlMsgFromString("The message "+msg.name[0]+" is handled", 0)
	//time.Sleep(1000 * time.Millisecond)
	n.nodeMessageQueueOut <- *tmp
	switch msg.name[0] {
	case "quit":
		n.quit()
		for len(n.nodeMessageQueueOut) > 0 {
		}
		close(n.nodeMessageQueueOut)
		close(n.userMessageQueueIn)
		defer func() {
			n.ifStop <- STOP
			fmt.Print("node stopped\n")
		}()
		break
	case "create":
		n.create()
		n.nodeMessageQueueOut <- *NewCtrlMsgFromString("Create ring and set table[0] and pre to "+n.info.ipAddress+" : "+strconv.Itoa(int(n.info.port)), 1)
		break
	case "join":
		n.join()
		break
	default:
	}
	//TODO
}

func (n *ringNode) ProcessUserCommand(wg *sync.WaitGroup) {
	welmsg := NewCtrlMsgFromString("The node start on ip "+n.info.ipAddress+":"+strconv.Itoa(int(n.info.port))+" with hashed addresses:"+n.info.hashedAddress.String(), 1)
	n.nodeMessageQueueOut <- *welmsg
	for {
		n.currentMsg = <-n.userMessageQueueIn
		n.handleMsg(&n.currentMsg)
		if len(n.ifStop) > 0 {
			break
		}
	}
	wg.Done()
}

func (n *ringNode) create() {
	n.nodeFingerTable.table[0] = n.info
	n.nodeFingerTable.predecessor = n.info
}

func (n *ringNode) quit() {}

func (n *ringNode) join() {}

//func (n *ringNode) closestPrecedingNode(hashedAddress big.Int) *nodeInfo {}

func (n *ringNode) Run(wg *sync.WaitGroup) {
	var wgi sync.WaitGroup
	wgi.Add(1)
	go n.ProcessUserCommand(&wgi)
	wgi.Wait()
	wg.Done()
}
