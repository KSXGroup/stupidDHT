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
	SERVER_TIME_OUT       int64  = 5e8
	MAX_QUEUE_LEN         int32  = 1024
	HASHED_ADDRESS_LENGTH int32  = 160
	FIX_FINGER_INTERVAL   int32  = 50
	STABILIZE_INTERVAL    int32  = 50
	CHECKPRE_INTERVAL     int32  = 50
	MAX_SUCCESSORLIST_LEN int32  = 5
	STOP                  uint8  = 0
)

type CtrlMessage struct {
	name []string
	arg  int32 //This arg is useless
}

func NewCtrlMsg(_n []string, _arg int32) *CtrlMessage {
	tmsg := new(CtrlMessage)
	tmsg.arg = _arg
	tmsg.name = _n
	return tmsg
}

func NewCtrlMsgFromString(_n string, _arg int32) *CtrlMessage {
	tmsg := new(CtrlMessage)
	tmsg.arg = _arg
	tmsg.name = make([]string, 1)
	tmsg.name[0] = _n
	return tmsg
}

func NewCtrlMsgFromStringSplited(_n string, _arg int32) *CtrlMessage {
	tmsg := new(CtrlMessage)
	tmsg.arg = _arg
	tmsg.name = strings.Fields(_n)
	return tmsg
}

func (msg *CtrlMessage) GetName() string {
	return msg.name[0]
}

type RingNode struct {
	Info                NodeInfo
	InRing              bool
	currentMsg          CtrlMessage
	data                map[string]string
	UserMessageQueueIn  chan CtrlMessage
	NodeMessageQueueOut chan CtrlMessage
	IfStop              chan uint8
	nodeFingerTable     *fingerTable
	nodeSuccessorList   *successorList
	rpcModule           *rpcServer
	dataLocker          *sync.Mutex
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
	ret.UserMessageQueueIn = make(chan CtrlMessage, MAX_QUEUE_LEN)
	ret.NodeMessageQueueOut = make(chan CtrlMessage, MAX_QUEUE_LEN)
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

func (n *RingNode) PrintNodeInfo() {
	n.Info.Print()
}

func (n *RingNode) GetDataSize() int {
	return len(n.data)
}

func (n *RingNode) getDataForPre(pre *NodeInfo, dta *map[string]string) {
	var hs, hpre, hself big.Int
	hpre = hashAddressFromNodeInfo(pre)
	hself = hashAddressFromNodeInfo(&n.Info)
	for k, v := range n.data {
		hs = hashString(k)
		if !Between(&hpre, &hs, &hself, true) {
			(*dta)[k] = v
			delete(n.data, k)
		}
	}
	return
}

func (n *RingNode) SendMessageIn(cmd string) {
	n.UserMessageQueueIn <- *NewCtrlMsgFromStringSplited(cmd, 0)
}

func (n *RingNode) SendMessageOut(info string) {
	//n.NodeMessageQueueOut <- *NewCtrlMsgFromString(info, 0)
}

func (n *RingNode) DumpData() {
	var pos int = 0
	if len(n.data) == 0 {
		fmt.Println("[No Data]")
		return
	}
	for k, v := range n.data {
		fmt.Printf("DATA#%d %s %s\n", pos, k, v)
		pos += 1
	}
}

func (n *RingNode) Remove(k string) bool {
	if !n.InRing || len(n.IfStop) > 0 {
		fmt.Print("Remove from node not in ring")
		return false
	}
	return n.rpcModule.remove(k)
}

func (n *RingNode) Put(k string, v string) bool {
	if !n.InRing || len(n.IfStop) > 0 {
		fmt.Print("Put from node not in ring")
		return false
	}
	return n.rpcModule.put(k, v)
}

func (n *RingNode) Get(k string) (string, bool) {
	if !n.InRing || len(n.IfStop) > 0 {
		fmt.Print("Get from node not in ring")
		return "", false
	}
	return n.rpcModule.get(k)
}

func (n *RingNode) AppendToData(k string, vap string) int {
	if !n.InRing || len(n.IfStop) > 0 {
		return 0
	}
	return n.rpcModule.appendToData(k, vap)
}

func (n *RingNode) RemoveFromData(k string, v string) int {
	if !n.InRing || len(n.IfStop) > 0 {
		return 0
	}
	return n.rpcModule.removeFromData(k, v)
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

func (n *RingNode) handleMsg(msg *CtrlMessage) {
	n.SendMessageOut("The message " + msg.name[0] + " is handled")
	switch msg.name[0] {
	case "quit":
		n.Quit()
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
	case "dumpdata":
		n.DumpData()
		break
	case "nf":
		n.Info.Print()
		break
	case "get":
		res, ok := n.Get(msg.name[1])
		if ok {
			fmt.Println(res)
		}
		break
	case "put":
		n.Put(msg.name[1], msg.name[2])
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
	//fmt.Println("PUC QUIT")
}

func (n *RingNode) Create() {
	n.nodeFingerTable.predecessor.IpAddress = ""
	n.nodeFingerTable.predecessor.Port = -1
	n.nodeFingerTable.table[0].remoteNode = n.Info
	n.nodeSuccessorList.list[0] = n.Info
	n.InRing = true
}

func (n *RingNode) Quit() {
	if !n.InRing {
		return
	}
	n.rpcModule.quit()
	n.InRing = false
	for len(n.NodeMessageQueueOut) > 0 {
	}
	n.IfStop <- STOP
	//PrintLog("[STOP INFO]Node Stop")
}

func (n *RingNode) Join(addrWithPort string) bool {
	n.nodeFingerTable.predecessor.IpAddress = ""
	n.nodeFingerTable.predecessor.Port = -1
	if n.rpcModule.join(addrWithPort) {
		n.InRing = true
		return true
	} else {
		return false
	}
}

func (n *RingNode) Run(wg *sync.WaitGroup) {
	defer func() {
		n.rpcModule.listener.Close()
		close(n.NodeMessageQueueOut)
		close(n.UserMessageQueueIn)
		wg.Done()
	}()
	var wgi sync.WaitGroup
	n.rpcModule.startListen()
	go n.rpcModule.accept()
	/*wgi.Add(1)
	go n.ProcessUserCommand(&wgi)*/
	wgi.Add(1)
	go n.rpcModule.stabilize(&wgi)
	wgi.Add(1)
	go n.rpcModule.fixFinger(&wgi)
	wgi.Add(1)
	go n.rpcModule.checkPredecessor(&wgi)
	wgi.Wait()
}
