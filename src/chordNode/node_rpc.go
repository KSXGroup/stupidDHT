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
	node       *RingNode
	server     *rpc.Server
	listener   *net.TCPListener
	service    *RpcServiceModule
	timeout    time.Duration
	currentFix int
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
	ret.currentFix = 0
	return ret
}

func (h *rpcServer) startListen() {
	addr, err := net.ResolveTCPAddr("tcp", h.node.Info.GetAddrWithPort())
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
	err := cl.Call("RingRPC.Ping", &arg, &relpy)
	if err != nil {
		cl.Close()
		h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Call Fail:"+err.Error(), 0)
		return ""
	} else {
		cl.Close()
		return relpy.From.GetAddrWithPort() + ":" + relpy.Name
	}
}

func (h *rpcServer) put(k KeyType, v ValueType) bool {
	//TODO
	return true
}

func (h *rpcServer) find(k KeyType) *ValueType {
	//TODO
	return nil
}

func (h *rpcServer) doCheckPredecessor() {
	if h.ping("a", h.node.nodeFingerTable.predecessor.GetAddrWithPort()) == "" {
		h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Predecessor fail, set to null", 0)
		h.node.nodeFingerTable.predecessor.Reset()
	}
}

func (h *rpcServer) doFixFinger() {
	h.findSuccessor(&h.node.nodeFingerTable.table[h.currentFix].HashedStartAddress)
	if int32(h.currentFix+1) >= HASHED_ADDRESS_LENGTH {
		h.currentFix = 1
	} else {
		h.currentFix += 1
		ret := h.findSuccessor(&h.node.nodeFingerTable.table[h.currentFix].HashedStartAddress)
		if ret == nil {
			h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Fix finger"+strconv.Itoa(h.currentFix)+"fail", 0)
		}
	}
}

func (h *rpcServer) findSuccessor(v *big.Int) *NodeValue {
	ret := new(NodeValue)
	if Between(&h.node.Info.HashedAddress, v, &h.node.nodeSuccessorList.list[0].HashedAddress, true) {
		ret.V = h.node.nodeSuccessorList.list[0]
		ret.Status = true
		return ret
	} else {
		var tmp HashedValue
		var tconn *net.Conn
		var cl *rpc.Client
		tmp.V = *v
		cpn := h.node.closestPrecedingNode(tmp)
		reply := new(NodeValue)
		reply.V = *cpn
		for {
			tconn = h.rpcDialWithNodeInfo(&reply.V)
			if tconn == nil {
				PrintLog("Dial fail when findSucc:" + reply.V.GetAddrWithPort())
				return nil
			} else {
				cl = rpc.NewClient(*tconn)
				tmp.From = h.node.Info
				tmp.V = cpn.HashedAddress
				rerr := cl.Call("RingRPC.FindSuccessor", &tmp, &reply)
				if rerr != nil {
					h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Call remote FindSuccessor fail:"+rerr.Error(), 0)
					cl.Close()
					return nil
				} else {
					if reply.Status == true {
						return reply
					}
				}
			}
			cl.Close()
		}
	}
}

func (h *rpcServer) join(addrWithPort string) bool {
	tconn := h.rpcDial(addrWithPort)
	if tconn == nil {
		PrintLog("Dial fail when join: " + addrWithPort)
		return false
	} else {
		var arg HashedValue
		var ret NodeValue
		arg.V = h.node.Info.HashedAddress
		arg.From = h.node.Info
		cl := rpc.NewClient(*tconn)
		rerr := cl.Call("RingRPC.FindSuccessor", &arg, &ret)
		if rerr != nil {
			cl.Close()
			h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Call remote FindSuccessor fail:"+rerr.Error(), 0)
			return false
		} else {
			cl.Close()
			h.node.nodeSuccessorList.list[0] = ret.V
			h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Update successor[0]: "+ret.V.GetAddrWithPort(), 0)
			return true
		}
	}
}

func (h *rpcServer) stablize(wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()
	target := h.node.nodeSuccessorList.list[0]
	tconn := *h.rpcDialWithNodeInfo(&target)
	if tconn == nil {
		PrintLog("Dial fail when stablize")
		return
	} else {
		var arg HashedValue
		var reply NodeInfo
		var arg1 NodeValue
		var reply1 Greet
		arg.From = h.node.Info
		arg.V = h.node.Info.HashedAddress
		cl := rpc.NewClient(tconn)
		rerr := cl.Call("RingRPC.GetPredecessor", &arg, &reply)
		if rerr != nil {
			cl.Close()
			h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Call remote GetPredecssor fail: "+target.GetAddrWithPort(), 0)
			return
		} else if reply.IpAddress == "" {
			cl.Close()
			return
		} else {
			if Between(&h.node.Info.HashedAddress, &reply.HashedAddress, &h.node.nodeSuccessorList.list[0].HashedAddress, false) {
				h.node.nodeSuccessorList.list[0] = reply
				h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Update successor: "+reply.GetAddrWithPort(), 0)
			}
			arg1.From = h.node.Info
			arg1.V = h.node.Info
			rerr = cl.Call("RingRPC.Notify", arg1, &reply1)
			if rerr != nil {
				cl.Close()
				h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Notify Call fail: "+h.node.nodeSuccessorList.list[0].GetAddrWithPort()+":"+rerr.Error(), 0)
			} else {
				cl.Close()
				h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Notify Success to: "+h.node.nodeSuccessorList.list[0].GetAddrWithPort(), 0)
			}
		}
	}
}

func (h *RpcServiceModule) GetPredecessor(p HashedValue, ret *NodeInfo) (err error) {
	if p.From.IpAddress == "" {
		err = errors.New("Why you give me a FUCKING EMPTY ADDRESS? Auth Fail!")
		return nil
	} else {
		ret = &h.node.nodeFingerTable.predecessor
		return nil
	}
}

func (h *RpcServiceModule) FindSuccessor(p HashedValue, ret *NodeValue) (err error) {
	if p.V.String() == "" {
		err = errors.New("INVALID ADDRESS")
		return
	}
	n := &h.node.Info.HashedAddress
	successor := &h.node.nodeSuccessorList.list[0].HashedAddress
	if Between(n, &p.V, successor, true) {
		ret.V = h.node.nodeSuccessorList.list[0]
		ret.From = h.node.Info
		ret.Status = true
		return
	} else {
		cpn := h.node.closestPrecedingNode(p)
		ret.V = *cpn
		ret.From = h.node.Info
		ret.Status = false
		return
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

func (h *RpcServiceModule) Notify(arg NodeValue, reply *Greet) (err error) {
	if arg.V.IpAddress == "" {
		err = errors.New("Why you give me a FUCKING EMPTY ADDRESS?")
		return nil
	} else {
		if h.node.nodeFingerTable.predecessor.IpAddress == "" || Between(&h.node.nodeFingerTable.predecessor.HashedAddress, &arg.From.HashedAddress, &h.node.Info.HashedAddress, false) {
			h.node.nodeFingerTable.predecessor = arg.V
		}
		reply.Name = "Success"
		reply.From = h.node.Info
		return nil
	}
}
