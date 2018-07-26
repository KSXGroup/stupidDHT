package chordNode

import (
	"errors"
	"math/big"
	"net"
	"net/rpc"
	"strconv"
	"time"
)

type HashedValue struct {
	V big.Int
}

//THIS IS FOR TEST ONLY
type Greet struct {
	Name string
}

type KeyType struct {
	K string
}

type ValueType struct {
	V string
}

type rpcServer struct {
	node     *RingNode
	server   *rpc.Server
	listener *net.TCPListener
	service  *RpcServiceModule
	timeout  time.Duration
	//client part below
	client *rpc.Client
}

type RpcServiceModule struct {
	node *RingNode
}

func newRpcServer(n *RingNode) *rpcServer {
	ret := new(rpcServer)
	ret.service = new(RpcServiceModule)
	ret.server = rpc.NewServer()
	ret.node = n
	ret.service.node = n
	ret.timeout = time.Duration(SERVER_TIME_OUT)
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
	lis, err := net.ListenTCP("tcp", addr)
	h.listener = lis
	if err != nil {
		PrintLog("[Error]" + err.Error())
		h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Fail to listen, node will stop."+EXIT_TIP, 0)
		h.node.IfStop <- STOP
		return
	}
	h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Start to listen on TCP address "+addr.String(), 0)
}

func (h *rpcServer) accept() {
	for len(h.node.IfStop) == 0 {
		iconn, ierr := h.listener.Accept()
		if ierr != nil {
			PrintLog("[NETWORK ERROR]" + ierr.Error())
		} else {
			go h.server.ServeConn(iconn)
		}
	}
}

func (h *rpcServer) ping(g string, addr string) string {
	var relpy, arg Greet
	arg.Name = g
	tconn, err := net.DialTimeout("tcp", addr, h.timeout)
	if err != nil {
		h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Dial fail when PING: "+err.Error(), 0)
		return ""
	}
	h.client = rpc.NewClient(tconn)
	err = h.client.Call("RingRPC.Ping", arg, &relpy)
	if err != nil {
		h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Call Fail:"+err.Error(), 0)
		return ""
	} else {
		h.client.Close()
		return relpy.Name
	}
}

func (h *rpcServer) put(k KeyType, v ValueType) {
}

func (h *rpcServer) join(addrWithPort) {}

func (h *RpcServiceModule) FindSuccessor(p HashedValue, ret *HashedValue) (err error) {
	z := big.Int{}
	if p.V.String() == "" {
		err = errors.New("INVALID ADDRESS")
		return
	}
	//TODO ADDRESS FIND IN FINGER TABLE
	ret.V = *z.Add(&p.V, big.NewInt(1))
	return
}

//func (h *RpcServiceModule) ClosestPrecedingNode() (err error) {}

func (h *RpcServiceModule) Ping(p Greet, ret *Greet) (err error) {
	if p.Name == "" {
		err = errors.New("INVALID PING")
		return
	}
	PrintLog("New Ping Received")
	ret.Name = "Hello" + p.Name
	return
}

func (n *RpcServiceModule) notify(target *NodeInfo) {}
func (n *RpcServiceModule) ping(target *NodeInfo)   {}
func (n *RpcServiceModule) stablize()               {}
