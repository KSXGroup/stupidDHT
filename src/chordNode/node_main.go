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

type RingNode struct {
	Info                NodeInfo
	currentMsg          ctrlMessage
	nodeFingerTable     *fingerTable
	UserMessageQueueIn  chan ctrlMessage
	NodeMessageQueueOut chan ctrlMessage
	IfStop              chan uint8
}

func (n *RingNode) PrintNodeInfo() {
	fmt.Println(n.Info.IpAddress)
	//TODO PRINT FINGER LIST
}

func hashAddress(ip string, port int32) big.Int {
	toHash := ip + strconv.Itoa(int(port))
	hasher := sha1.New()
	tmp := new(big.Int).SetBytes(hasher.Sum([]byte(toHash)))
	hashModAddress := *new(big.Int).Mod(tmp, new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(HASHED_ADDRESS_LENGTH)), nil))
	return hashModAddress
}

func NewNode(port int32) *RingNode {
	var ret = new(RingNode)
	ret.Info = *NewNodeInfo(ret.getIp(), port)
	ret.UserMessageQueueIn = make(chan ctrlMessage, MAX_QUEUE_LEN)
	ret.NodeMessageQueueOut = make(chan ctrlMessage, MAX_QUEUE_LEN)
	ret.nodeFingerTable = NewFingerTable()
	ret.IfStop = make(chan uint8, 1)
	return ret
}

func (n *RingNode) getIp() string {
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

func (n *RingNode) handleMsg(msg *ctrlMessage) {
	tmp := NewCtrlMsgFromString("The message "+msg.name[0]+" is handled", 0)
	//time.Sleep(1000 * time.Millisecond)
	n.NodeMessageQueueOut <- *tmp
	switch msg.name[0] {
	case "quit":
		n.quit()
		for len(n.NodeMessageQueueOut) > 0 {
		}
		close(n.NodeMessageQueueOut)
		close(n.UserMessageQueueIn)
		defer func() {
			n.IfStop <- STOP
			PrintLog("[STOP INFO]Node Stop")
		}()
		break
	case "create":
		n.create()
		n.NodeMessageQueueOut <- *NewCtrlMsgFromString("Create ring and set table[0] and pre to "+n.Info.IpAddress+" : "+strconv.Itoa(int(n.Info.Port)), 1)
		break
	case "join":
		n.join()
		break
	default:
	}
	//TODO
}

func (n *RingNode) ProcessUserCommand(wg *sync.WaitGroup) {
	welmsg := NewCtrlMsgFromString("The node start on ip "+n.Info.IpAddress+":"+strconv.Itoa(int(n.Info.Port))+" with hashed addresses:"+n.Info.hashedAddress.String(), 1)
	n.NodeMessageQueueOut <- *welmsg
	for {
		n.currentMsg = <-n.UserMessageQueueIn
		n.handleMsg(&n.currentMsg)
		if len(n.IfStop) > 0 {
			break
		}
	}
	wg.Done()
}

func (n *RingNode) create() {
	n.nodeFingerTable.table[0] = n.Info
	n.nodeFingerTable.predecessor = n.Info
}

func (n *RingNode) quit() {}

func (n *RingNode) join() {}

//func (n *RingNode) closestPrecedingNode(hashedAddress big.Int) *NodeInfo {}

func (n *RingNode) Run(wg *sync.WaitGroup) {
	var wgi sync.WaitGroup
	wgi.Add(1)
	go n.ProcessUserCommand(&wgi)
	wgi.Wait()
	wg.Done()
}
