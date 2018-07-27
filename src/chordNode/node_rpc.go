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

type rpcServer struct {
	node     *RingNode
	server   *rpc.Server
	listener *net.TCPListener
	service  *RpcServiceModule
	timeout  time.Duration
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

func (h *rpcServer) rpcDial(addr string) *net.Conn {
	tconn, err := net.DialTimeout("tcp", addr, h.timeout)
	if err != nil {
		h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Dial fail "+addr+" "+err.Error(), 0)
		return nil
	} else {
		return &tconn
	}
}

func (h *rpcServer) ping(g string, addr string) string {
	var relpy, arg Greet
	arg.Name = g
	tconn := *h.rpcDial(addr)
	if tconn == nil {
		return ""
	}
	cl := rpc.NewClient(tconn)
	err := cl.Call("RingRPC.Ping", arg, &relpy)
	if err != nil {
		h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Call Fail:"+err.Error(), 0)
		return ""
	} else {
		cl.Close()
		return relpy.Name
	}
}

func (h *rpcServer) put(k KeyType, v ValueType) {
}

func (h *rpcServer) join(addrWithPort string) {}

func (h *RpcServiceModule) FindSuccessor(p HashedValue, ret *NodeInfo) (err error) {
	if p.V.String() == "" {
		err = errors.New("INVALID ADDRESS")
		return
	}
	n := &h.node.Info.HashedAddress
	successor := &h.node.nodeFingerTable.table[0].HashedAddress
	if p.V.Cmp(n) == 1 && p.V.Cmp(successor) == -1 {
		ret = &h.node.nodeFingerTable.table[0]
		return
	} else {
		var cpn NodeInfo
		h.ClosestPrecedingNode(p, &cpn)
		tconn := *h.node.rpcModule.rpcDial(cpn.IpAddress + ":" + strconv.Itoa(int(cpn.Port)))
		if tconn == nil {
			err = errors.New("Network error")
			return nil
		}
		cl := rpc.NewClient(tconn)
		rerr := cl.Call("RingRPC.FindSuccessor", p, &cpn)
		if err != nil {
			err = rerr
			return nil
		} else {
			ret = &cpn
			return nil
		}
	}
}

func (h *RpcServiceModule) ClosestPrecedingNode(p HashedValue, ret *NodeInfo) (err error) {
	return
}

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
func (n *RpcServiceModule) stablize()               {}
