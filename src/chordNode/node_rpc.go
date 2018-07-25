package chordNode

import (
	"errors"
	"math/big"
	"net"
	"strconv"
)

type HashedValue struct {
	v big.Int
}

//THIS IS FOR TEST ONLY
type Greet struct {
	name string
}

type rpcHandler struct {
	node     *RingNode
	tcpAddr  *net.TCPAddr
	listener *net.TCPListener
}

func newRpcHandler(n *RingNode) *rpcHandler {
	ret := new(rpcHandler)
	ret.node = n
	return ret
}

func (h *rpcHandler) startListen() {
	addr, err := net.ResolveTCPAddr("tcp", h.node.Info.IpAddress+":"+strconv.Itoa(int(h.node.Info.Port)))
	if err != nil {
		h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Fail to listen, node will stop."+EXIT_TIP, 0)
		h.node.IfStop <- STOP
		return
	}
	h.tcpAddr = addr
	h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("[NETWORK INFO]Start to listen on TCP address"+h.tcpAddr.String(), 0)
}

func (h *rpcHandler) stopListen() {}

func (h *rpcHandler) FindSuccessor(p HashedValue, ret *HashedValue) (err error) {
	z := big.Int{}
	if p.v.String() == "" {
		err = errors.New("INVALID ADDRESS")
		return
	}
	//TODO ADDRESS FIND IN FINGER TABLE
	ret.v = *z.Add(&p.v, big.NewInt(1))
	return
}

func (h *rpcHandler) Ping(p Greet, ret *Greet) (err error) {
	if p.name == "" {
		err = errors.New("INVALID PING")
		return
	}
	ret.name = "Hello" + p.name
	return
}

func (n *RingNode) notify(target *NodeInfo) {}
func (n *RingNode) ping(target *NodeInfo)   {}
func (n *RingNode) stablize()               {}
