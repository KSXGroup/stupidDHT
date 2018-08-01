package chordNode

import (
	"errors"
	"fmt"
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

type SuccListInfo struct {
	SuccList [MAX_SUCCESSORLIST_LEN]NodeInfo
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
	tconn := h.rpcDial(addr)
	if tconn == nil {
		return ""
	}
	cl := rpc.NewClient(*tconn)
	err := cl.Call("RingRPC.Ping", &arg, &relpy)
	if err != nil {
		cl.Close()
		if err != nil {
			PrintLog("close error:" + err.Error())
		}
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

func (h *rpcServer) checkPredecessor() {
	var cnt int = 0
	for len(h.node.IfStop) == 0 {
		time.Sleep(time.Millisecond * 1)
		cnt++
		if cnt == int(CHECKPRE_INTERVAL) {
			h.doCheckPredecessor()
			cnt = 0
		}
	}
}

func (h *rpcServer) doCheckPredecessor() {
	if len(h.node.IfStop) > 0 || h.node.InRing == false {
		return
	}
	PrintLog("check pre")
	if h.ping("a", h.node.nodeFingerTable.predecessor.GetAddrWithPort()) == "" {
		h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Predecessor fail, set to null", 0)
		h.node.nodeFingerTable.predecessor.Reset()
	}
}

func (h *rpcServer) fixFinger(wg *sync.WaitGroup) {
	var cnt int32 = 0
	for len(h.node.IfStop) == 0 {
		time.Sleep(time.Millisecond * 1)
		cnt += 1
		if cnt == FIX_FINGER_INTERVAL {
			h.doFixFinger()
			cnt = 0
		}
	}
	fmt.Println("FIXFINGER QUIT")
	wg.Done()
}

func (h *rpcServer) doFixFinger() {
	if len(h.node.IfStop) > 0 || !h.node.InRing {
		//h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("node stop or not in ring,fixfinger stop", 0)
		return
	}
	//h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Start fix finger "+strconv.Itoa(h.currentFix), 0)
	var ret NodeValue
	var hv HashedValue
	hv.From = h.node.Info
	hv.V = h.node.nodeFingerTable.table[h.currentFix].HashedStartAddress
	err := h.service.FindSuccessorInit(hv, &ret)
	if err != nil {
		h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Find succ fail when fix finger"+err.Error(), 0)
		return
	} else {
		h.node.nodeFingerTable.table[h.currentFix].remoteNode = ret.V
		//h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("update finger "+strconv.Itoa(h.currentFix)+"to"+ret.V.GetAddrWithPort(), 0)
	}
	if int32(h.currentFix+1) == HASHED_ADDRESS_LENGTH {
		h.currentFix = 0
	} else {
		h.currentFix += 1
	}
	return
}

func (h *rpcServer) join(addrWithPort string) bool {
	tconn := h.rpcDial(addrWithPort)
	if tconn == nil {
		PrintLog("Dial fail when join: " + addrWithPort)
		return false
	} else {
		var arg HashedValue
		var ret NodeValue
		arg.V = hashAddressFromNodeInfo(&h.node.Info)
		arg.From = h.node.Info
		cl := rpc.NewClient(*tconn)
		rerr := cl.Call("RingRPC.FindSuccessorInit", &arg, &ret)
		if rerr != nil {
			cl.Close()
			h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Call remote FindSuccessorInit fail:"+rerr.Error(), 0)
			return false
		} else {
			cl.Close()
			h.node.nodeSuccessorList.list[0] = ret.V
			h.node.nodeFingerTable.table[0].remoteNode = ret.V
			h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Update successor[0]: "+ret.V.GetAddrWithPort(), 0)
			return true
		}
	}
}

func (h *rpcServer) stabilize(wg *sync.WaitGroup) {
	var cnt int32 = 0
	for len(h.node.IfStop) == 0 {
		time.Sleep(time.Millisecond * 1)
		cnt += 1
		if cnt == STABILIZE_INTERVAL {
			h.doStabilize()
			cnt = 0
		}
	}
	fmt.Println("STAB QUIT")
	wg.Done()
}

func (h *rpcServer) doStabilize() {
	//h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Start stab", 0)
	if len(h.node.IfStop) > 0 || !h.node.InRing {
		//h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Node stop or not in ring, stab exit", 0)
		return
	}
	h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Start stabilize", 0)
	var arg HashedValue
	var reply NodeInfo
	var arg1 NodeValue
	var reply1 Greet
	var argSucc Greet
	var replySucc SuccListInfo
	var target *NodeInfo
	var tconn *net.Conn = nil
	var ifChanged bool = false
	//h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Do start stab", 0)
	for {
		target = &h.node.nodeSuccessorList.list[0]
		tconn = h.rpcDialWithNodeInfo(target)
		if tconn != nil {
			break
		} else if h.node.nodeSuccessorList.list[0].IpAddress == "" {
			//TODO There is few possibility, let it stop temperoraily
			PrintLog("All Succ Fail, node stop")
			h.node.IfStop <- STOP
			return
		} else {
			PrintLog("Remove first entry")
			h.node.nodeSuccessorList.DumpSuccessorList()
			for i := 0; i < int(MAX_SUCCESSORLIST_LEN-1); i += 1 {
				h.node.nodeSuccessorList.list[i] = h.node.nodeSuccessorList.list[i+1]
			}
			h.node.nodeSuccessorList.list[int(MAX_SUCCESSORLIST_LEN-1)].Reset()
			h.node.nodeSuccessorList.DumpSuccessorList()
		}
	}
	arg.From = h.node.Info
	arg.V = hashAddressFromNodeInfo(&h.node.Info)
	cl := rpc.NewClient(*tconn)
	rerr := cl.Call("RingRPC.GetPredecessor", &arg, &reply)
	if rerr != nil {
		cl.Close()
		h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Call remote GetPredecssor fail: "+target.GetAddrWithPort(), 0)
		return
	} else {
		n := hashAddressFromNodeInfo(&h.node.Info)
		x := hashAddressFromNodeInfo(&reply)
		successor := hashAddressFromNodeInfo(&h.node.nodeSuccessorList.list[0])
		if reply.IpAddress != "" && Between(&n, &x, &successor, false) {
			ifChanged = true
			h.node.nodeSuccessorList.list[0] = reply
			h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Update successor: "+reply.GetAddrWithPort(), 0)
		}
		if h.node.nodeSuccessorList.list[0].IpAddress != "" {
			if ifChanged {
				cl.Close()
				tconn = h.node.rpcModule.rpcDialWithNodeInfo(&h.node.nodeSuccessorList.list[0])
				if tconn == nil {
					PrintLog("Dial fail when stab when copy successor's succ list")
					return
				}
				cl = rpc.NewClient(*tconn)
			}
			argSucc.From = h.node.Info
			serr := cl.Call("RingRPC.GetSuccessorList", &argSucc, &replySucc)
			if serr != nil {
				h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Copy successor list fail"+serr.Error()+" "+h.node.nodeSuccessorList.list[0].GetAddrWithPort(), 0)
			} else {
				for i := 1; i < int(MAX_SUCCESSORLIST_LEN); i += 1 {
					h.node.nodeSuccessorList.list[i] = replySucc.SuccList[i-1]
				}
				h.node.nodeSuccessorList.DumpSuccessorList()
				h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Copy successor list success"+" "+h.node.nodeSuccessorList.list[0].GetAddrWithPort(), 0)
			}
		}
		arg1.From = h.node.Info
		arg1.V = h.node.Info
		//h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Call notify", 0)
		rerr = cl.Call("RingRPC.Notify", &arg1, &reply1)
		cl.Close()
		if rerr != nil {
			h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Notify Call fail: "+h.node.nodeSuccessorList.list[0].GetAddrWithPort()+":"+rerr.Error(), 0)
		} else {
			//h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Notify Success to: "+h.node.nodeSuccessorList.list[ptr].GetAddrWithPort(), 0)
		}
	}
}

func (h *RpcServiceModule) GetPredecessor(p HashedValue, ret *NodeInfo) (err error) {
	if p.From.IpAddress == "" {
		err = errors.New("Why you give me a FUCKING EMPTY ADDRESS? Auth Fail!")
		return err
	} else {
		ret.IpAddress = h.node.nodeFingerTable.predecessor.IpAddress
		ret.Port = h.node.nodeFingerTable.predecessor.Port
		return nil
	}
}

func (h *RpcServiceModule) FindSuccessor(p HashedValue, ret *NodeValue) (err error) {
	if p.V.String() == "" {
		err = errors.New("INVALID ADDRESS")
		return
	}
	PrintLog("New FindSucc request from" + p.From.GetAddrWithPort())
	n := hashAddressFromNodeInfo(&h.node.Info)
	successor := hashAddressFromNodeInfo(&h.node.nodeSuccessorList.list[0])
	if Between(&n, &p.V, &successor, true) {
		ret.V = h.node.nodeSuccessorList.list[0]
		ret.From = h.node.Info
		ret.Status = true
		return
	} else {
		ret.V = h.node.closestPrecedingNode(p)
		ret.From = h.node.Info
		ret.Status = false
		return
	}
}

func (h *RpcServiceModule) FindSuccessorInit(p HashedValue, ret *NodeValue) (err error) {
	if h.node.InRing == false {
		err = errors.New("Not in ring")
		return
	}
	tp := p
	n := hashAddressFromNodeInfo(&h.node.Info)
	successor := hashAddressFromNodeInfo(&h.node.nodeSuccessorList.list[0])
	if Between(&n, &tp.V, &successor, true) {
		ret.V = h.node.nodeSuccessorList.list[0]
		ret.Status = true
		ret.From = h.node.Info
		return
	} else {
		var tconn *net.Conn
		var cl *rpc.Client
		reply := new(NodeValue)
		reply.V = h.node.closestPrecedingNode(tp)
		reply.From = h.node.Info
		for {
			tconn = h.node.rpcModule.rpcDialWithNodeInfo(&reply.V)
			if tconn == nil {
				err = errors.New("Dial fail:" + reply.V.GetAddrWithPort())
				return err
			} else {
				cl = rpc.NewClient(*tconn)
				/*if h.node.rpcModule.currentFix == 158 {
					reply.V.Print()
					reply.From.Print()
					h.node.nodeSuccessorList.list[ptr].Print()
					h.node.nodeFingerTable.DumpFingerTable()
				}*/
				rerr := cl.Call("RingRPC.FindSuccessor", &tp, reply)
				/*if h.node.rpcModule.currentFix == 158 {
					reply.V.Print()
					reply.From.Print()
					h.node.nodeSuccessorList.list[ptr].Print()
					h.node.nodeFingerTable.DumpFingerTable()
				}*/
				if rerr != nil {
					err = errors.New("Call remote FindSuccessor fail:" + rerr.Error())
					cl.Close()
					return rerr
				} else {
					ret.From = reply.From
					ret.Status = reply.Status
					ret.V = reply.V
					if ret.Status == true {
						cl.Close()
						return nil
					}
				}
			}
			cl.Close()
		}
	}
}

func (h *RpcServiceModule) GetSuccessorList(p Greet, ret *SuccListInfo) (err error) {
	if p.From.IpAddress == "" {
		err = errors.New("Why you give me a FUCKING EMPTY ADDRESS?")
		return err
	}
	for i := 0; i < int(MAX_SUCCESSORLIST_LEN); i += 1 {
		ret.SuccList[i] = h.node.nodeSuccessorList.list[i]
	}
	return nil
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
		return err
	} else {
		//PrintLog("ReceiveNotify")
		pre := hashAddressFromNodeInfo(&h.node.nodeFingerTable.predecessor)
		myargv := hashAddressFromNodeInfo(&arg.V)
		self := hashAddressFromNodeInfo(&h.node.Info)
		if h.node.nodeFingerTable.predecessor.IpAddress == "" || Between(&pre, &myargv, &self, false) {
			h.node.nodeFingerTable.predecessor = arg.V
			//h.node.NodeMessageQueueOut <- *NewCtrlMsgFromString("Receive Notify, update pre to:"+arg.V.GetAddrWithPort(), 0)
		}
		reply.Name = "Success"
		reply.From = h.node.Info
		return nil
	}
}
