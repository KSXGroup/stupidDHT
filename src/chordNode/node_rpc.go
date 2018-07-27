package chordNode

import (
	"errors"
	"math/big"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

type HashedValue struct {
	V    big.Int
	From NodeInfo
}

//THIS IS FOR TEST ONLY
type Greet struct {
	Name string
	From NodeInfo
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

func (h *rpcServer) rpcDialWithNodeInfo(n *NodeInfo) *net.Conn {
	addr := n.IpAddress + ":" + strconv.Itoa(int(n.Port))
	tconn := h.rpcDial(addr)
	return tconn
}

func (h *rpcServer) ping(g string, addr string) string {
	var relpy, arg Greet
	arg.Name = g
	arg.From = h.node.Info
	tconn := *h.rpcDial(addr)
	if tconn == nil {
		return ""
	}
	cl := rpc.NewClient(tconn)
	err := cl.Call("RingRPC.Ping", arg, &relpy)
	if err != nil {
		cl.Close()
		h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Call Fail:"+err.Error(), 0)
		return ""
	} else {
		cl.Close()
		return (relpy.From.IpAddress + ":" + strconv.Itoa(int(relpy.From.Port)) + ":" + relpy.Name)
	}
}

func (h *rpcServer) put(k KeyType, v ValueType) {
	//TODO
}

func (h *rpcServer) join(addrWithPort string) {
	tconn := h.rpcDial(addrWithPort)
	if tconn == nil {
		PrintLog("Dial fail when join: " + addrWithPort)
		return
	} else {
		var arg HashedValue
		var ret NodeInfo
		arg.V = h.node.Info.HashedAddress
		arg.From = h.node.Info
		cl := rpc.NewClient(*tconn)
		rerr := cl.Call("RingRPC.FindSuccessor", arg, &ret)
		if rerr != nil {
			cl.Close()
			h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Call remote FindSuccessor fail:"+rerr.Error(), 0)
			return
		} else {
			cl.Close()
			h.node.nodeFingerTable.table[0].remoteNode = ret
			h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Update successor: "+ret.IpAddress+strconv.Itoa(int(ret.Port)), 0)
			return
		}
	}
}

//i use finger[0] as successor, but actually this may cause problem
//stablize go wrong!!!!
func (h *rpcServer) stablize(wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()
	target := h.node.nodeFingerTable.table[0].remoteNode
	tconn := *h.rpcDialWithNodeInfo(&target)
	if tconn == nil {
		PrintLog("Dial fail when stablize")
		return
	} else {
		var arg HashedValue
		var reply NodeInfo
		arg.From = h.node.Info
		arg.V = h.node.Info.HashedAddress
		cl := rpc.NewClient(tconn)
		rerr := cl.Call("RingRPC.GetPredecessor", arg, &reply)
		if rerr != nil {
			cl.Close()
			h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Call remote GetPredecssor fail: "+target.IpAddress+strconv.Itoa(int(ret.Port)), 0)
			return
		} else {
			if Between(&target.HashedAddress, &h.node.nodeFingerTable.table[0].remoteNode.HashedAddress, &reply.HashedAddress) {
				h.node.nodeFingerTable.table[0].remoteNode = reply
				h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Update successor: "+ret.IpAddress+strconv.Itoa(int(ret.Port)), 0)
			}
			rerr = cl.Call("RingRPC.Notify", arg, nil)
			if rerr != nil {
				h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Update successor: "+ret.IpAddress+strconv.Itoa(int(ret.Port)), 0)
			}
		}
	}
}

func (h *RpcServiceModule) GetPredecessor(p HashedValue, ret *NodeInfo) (err error) {
	if p.From.IpAddress == "" {
		err = errors.New("Why you give me a FUCKING EMPTY ADDRESS? Auth Fail!")
		return nil
	} else {
		ret = &h.node.Info
		return nil
	}
}

func (h *RpcServiceModule) FindSuccessor(p HashedValue, ret *NodeInfo) (err error) {
	if p.V.String() == "" {
		err = errors.New("INVALID ADDRESS")
		return
	}
	n := &h.node.Info.HashedAddress
	successor := &h.node.nodeFingerTable.table[0].remoteNode.HashedAddress
	if Between(n, successor, &p.V) || successor.Cmp(&p.V) == 0 {
		ret = &h.node.nodeFingerTable.table[0].remoteNode
		return
	} else {
		cpn := h.node.closetPrecedingNode(p)
		tconn := *h.node.rpcModule.rpcDial(cpn.IpAddress + ":" + strconv.Itoa(int(cpn.Port)))
		if tconn == nil {
			err = errors.New("Network error")
			return nil
		}
		cl := rpc.NewClient(tconn)
		rerr := cl.Call("RingRPC.FindSuccessor", p, cpn)
		if err != nil {
			err = rerr
			return nil
		} else {
			ret = cpn
			return nil
		}
	}
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

func (n *RpcServiceModule) Notify(target *NodeInfo) {}
