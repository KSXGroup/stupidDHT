package chordNode

import (
	"errors"
	"math/big"
	"net"
	"net/rpc"
	"strconv"
)

type HashedValue struct {
	v big.Int
}

//THIS IS FOR TEST ONLY
type Greet struct {
	name string
}

type rpcServer struct {
	node     *RingNode
	tcpAddr  *net.TCPAddr
	listener *net.TCPListener
	server   *rpc.Server
	service  *RpcServiceModule
	//client part below
	conn   *net.Conn
	client *rpc.Client
}

type RpcServiceModule struct{}

func newRpcServer(n *RingNode) *rpcServer {
	ret := new(rpcServer)
	ret.service = new(RpcServiceModule)
	ret.server = rpc.NewServer()
	ret.node = n
	return ret
}

func (h *rpcServer) startListen() {
	addr, err := net.ResolveTCPAddr("tcp", h.node.Info.IpAddress+":"+strconv.Itoa(int(h.node.Info.Port)))
	if err != nil {
		PrintLog("[Error]" + err.Error())
		h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Fail to resolve address, node will stop."+EXIT_TIP, 0)
		h.node.IfStop <- STOP
		return
	}
	h.tcpAddr = addr
	lis, err := net.ListenTCP("tcp", h.tcpAddr)
	if err != nil {
		PrintLog("[Error]" + err.Error())
		h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Fail to listen, node will stop."+EXIT_TIP, 0)
		h.node.IfStop <- STOP
		return
	}
	h.listener = lis
	h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Start to listen on TCP address "+h.tcpAddr.String(), 0)
}

func (h *rpcServer) stopListen() {
	h.listener.Close()
	h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Stop to listen on TCP address "+h.tcpAddr.String(), 0)
}

func (h *rpcServer) ping(g string, addr string) string {
	var relpy, arg Greet
	arg.name = g
	tconn, err := net.Dial("tcp", addr)
	if err != nil {
		h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Dial fail when PING:"+err.Error()+" Please enter correct address.", 0)
		return ""
	}
	h.conn = &tconn
	h.client = rpc.NewClient(*h.conn)
	err = h.client.Call("RingRPC.Ping", g, &relpy)
	if err != nil {
		h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("PING Fail:"+err.Error(), 0)
		return ""
	} else {
		h.client.Close()
		(*h.conn).Close()
		return relpy.name
	}
}

func (h *RpcServiceModule) FindSuccessor(p HashedValue, ret *HashedValue) (err error) {
	z := big.Int{}
	if p.v.String() == "" {
		err = errors.New("INVALID ADDRESS")
		return
	}
	//TODO ADDRESS FIND IN FINGER TABLE
	ret.v = *z.Add(&p.v, big.NewInt(1))
	return
}

func (h *RpcServiceModule) Ping(p Greet, ret *Greet) (err error) {
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
