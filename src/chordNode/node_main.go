// node
package chordNode

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"net"
	"strconv"
	"strings"
	"sync"
)

const (
	EXIT_TIP              string = "You can press ANY KEY to exit."
	SERVER_TIME_OUT       int64  = 6e8
	MAX_QUEUE_LEN         int32  = 1024
	HASHED_ADDRESS_LENGTH int32  = 160
	FIX_FINGER_INTERVAL   int32  = 1000
	STABILIZE_INTERVAL    int32  = 1000
	CHECKPRE_INTERVAL     int32  = 1000
	MAX_SUCCESSORLIST_LEN int32  = 5
	STOP                  uint8  = 0
)

type ctrlMessage struct {
	name []string
	arg  int32 //This arg is useless
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

func NewCtrlMsgFromStringSplited(_n string, _arg int32) *ctrlMessage {
	tmsg := new(ctrlMessage)
	tmsg.arg = _arg
	tmsg.name = strings.Fields(_n)
	return tmsg
}

type RingNode struct {
	Info                NodeInfo
	InRing              bool
	currentMsg          ctrlMessage
	data                map[string]string
	UserMessageQueueIn  chan ctrlMessage
	NodeMessageQueueOut chan ctrlMessage
	IfStop              chan uint8
	nodeFingerTable     *fingerTable
	nodeSuccessorList   *successorList
	rpcModule           *rpcServer
	dataLocker          *sync.Mutex
}

func (n *RingNode) PrintNodeInfo() {
	n.Info.Print()
	//TODO PRINT FINGER LIST
}

func (n *RingNode) DumpData() {
	if len(n.data) == 0 {
		fmt.Println("No Data")
	} else {
		for k, v := range n.data {
			fmt.Println(string(k) + " " + string(v))
		}
	}
}

func Between(a *big.Int, i *big.Int, b *big.Int, inclusive bool) bool {
	if b.Cmp(a) > 0 {
		return (a.Cmp(i) < 0 && i.Cmp(b) < 0) || (inclusive && i.Cmp(b) == 0)
	} else {
		return a.Cmp(i) < 0 || i.Cmp(b) < 0 || (inclusive && i.Cmp(b) == 0)
	}
}

func hashAddress(ip string, port int32) big.Int {
	toHash := ip + strconv.Itoa(int(port))
	return hashString(toHash)
}

func hashAddressFromNodeInfo(nif *NodeInfo) big.Int {
	ip := nif.IpAddress
	port := nif.Port
	return hashAddress(ip, port)
}

func hashString(s string) big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(s))
	tmp := new(big.Int).SetBytes(hasher.Sum(nil))
	hashModAddress := *new(big.Int).Mod(tmp, new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(HASHED_ADDRESS_LENGTH)), nil))
	return hashModAddress
}

func NewNode(port int32) *RingNode {
	var ret = new(RingNode)
	ret.Info = *NewNodeInfo(ret.getIp(), port)
	ret.data = make(map[string]string)
	ret.InRing = false
	ret.UserMessageQueueIn = make(chan ctrlMessage, MAX_QUEUE_LEN)
	ret.NodeMessageQueueOut = make(chan ctrlMessage, MAX_QUEUE_LEN)
	ret.nodeFingerTable = NewFingerTable()
	tmpHash := hashAddressFromNodeInfo(&ret.Info)
	for pos, _ := range ret.nodeFingerTable.table {
		ret.nodeFingerTable.table[pos].HashedStartAddress = *AddPowerOfTwo(&tmpHash, pos)
	}
	ret.IfStop = make(chan uint8, 1)
	ret.nodeSuccessorList = newSuccessorList()
	ret.rpcModule = newRpcServer(ret)
	ret.dataLocker = new(sync.Mutex)
	ret.rpcModule.server.RegisterName("RingRPC", ret.rpcModule.service)
	return ret
}

func (n *RingNode) SendMessageIn(cmd string) {
	n.UserMessageQueueIn <- *NewCtrlMsgFromStringSplited(cmd, 0)
}

func (n *RingNode) SendMessageOut(info string) {
	n.NodeMessageQueueOut <- *NewCtrlMsgFromString(info, 0)
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
	n.SendMessageOut("The message " + msg.name[0] + " is handled")
	switch msg.name[0] {
	case "quit":
		n.Quit()
		for len(n.NodeMessageQueueOut) > 0 {
		}
		defer func() {
			n.IfStop <- STOP
			PrintLog("[STOP INFO]Node Stop")
		}()
		break
	case "create":
		n.Create()
		n.SendMessageOut("Create ring and set table[0] and pre to " + n.Info.IpAddress + ":" + strconv.Itoa(int(n.Info.Port)))
		break
	case "join":
		n.Join(msg.name[1])
		break
	case "dump":
		n.nodeFingerTable.DumpFingerTable()
		break
	case "ping":
		ret := n.rpcModule.ping(",fuck", msg.name[1])
		if ret != "" {
			n.SendMessageOut(ret)
		}
		break
	case "dumpsucc":
		n.nodeSuccessorList.DumpSuccessorList()
		break
	case "nf":
		n.Info.Print()
		break
	case "get":
		res, ok := n.rpcModule.get(msg.name[1])
		if ok {
			fmt.Println(res)
		}
		break
	case "put":
		n.rpcModule.put(msg.name[1], msg.name[2])
		break
	default:
	}
	//TODO
}

func (n *RingNode) ProcessUserCommand(wg *sync.WaitGroup) {
	defer wg.Done()
	nfh := hashAddressFromNodeInfo(&n.Info)
	var ok bool = true
	//welmsg := NewCtrlMsgFromString("The node start on ip "+n.Info.IpAddress+":"+strconv.Itoa(int(n.Info.Port))+" with hashed addresses:"+nfh.String(), 1)
	n.SendMessageOut("The node start on ip " + n.Info.IpAddress + ":" + strconv.Itoa(int(n.Info.Port)) + " with hashed addresses:" + nfh.String())
	for len(n.IfStop) == 0 {
		n.currentMsg, ok = <-n.UserMessageQueueIn
		if !ok {
			break
		}
		n.handleMsg(&n.currentMsg)
	}
	fmt.Println("PUC QUIT")
}

func (n *RingNode) Create() {
	n.nodeFingerTable.predecessor.IpAddress = ""
	n.nodeFingerTable.predecessor.Port = -1
	n.nodeFingerTable.table[0].remoteNode = n.Info
	n.nodeSuccessorList.list[0] = n.Info
	n.InRing = true
}

func (n *RingNode) Quit() {
	n.InRing = false
}

func (n *RingNode) Join(addrWithPort string) {
	n.nodeFingerTable.predecessor.IpAddress = ""
	n.nodeFingerTable.predecessor.Port = -1
	if n.rpcModule.join(addrWithPort) {
		n.InRing = true
	}
}

func (n *RingNode) Run(wg *sync.WaitGroup) {
	defer func() {
		n.rpcModule.listener.Close()
		close(n.NodeMessageQueueOut)
		wg.Done()
	}()
	var wgi sync.WaitGroup
	n.rpcModule.startListen()
	go n.rpcModule.accept()
	wgi.Add(1)
	go n.ProcessUserCommand(&wgi)
	wgi.Add(1)
	go n.rpcModule.stabilize(&wgi)
	wgi.Add(1)
	go n.rpcModule.fixFinger(&wgi)
	wgi.Add(1)
	go n.rpcModule.checkPredecessor(&wgi)
	wgi.Wait()
	close(n.UserMessageQueueIn)
}
