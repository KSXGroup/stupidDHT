package chordNode

import (
	"errors"
	//"fmt"
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

type NodeData struct {
	Key   string
	Value string
	From  NodeInfo
}

type NodeDataSet struct {
	DataSet map[string]string
	From    NodeInfo
}

type SuccListInfo struct {
	SuccList [MAX_SUCCESSORLIST_LEN]NodeInfo
}

type NodeQuitData struct {
	DataSet map[string]string
	From    NodeInfo
	Pre     NodeInfo
}

type rpcServer struct {
	node                  *RingNode
	server                *rpc.Server
	listener              *net.TCPListener
	service               *RpcServiceModule
	successorListLocker   *sync.Mutex
	predecessorLocker     *sync.Mutex
	fingerTableLockerList [HASHED_ADDRESS_LENGTH]*sync.Mutex
	timeout               time.Duration
	currentFix            int
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
	ret.successorListLocker = new(sync.Mutex)
	ret.predecessorLocker = new(sync.Mutex)
	ret.timeout = time.Duration(SERVER_TIME_OUT)
	ret.currentFix = 0
	for i := 0; i < int(HASHED_ADDRESS_LENGTH); i += 1 {
		ret.fingerTableLockerList[i] = new(sync.Mutex)
	}
	return ret
}

func (h *rpcServer) startListen() {
	addr, err := net.ResolveTCPAddr("tcp", h.node.Info.GetAddrWithPort())
	if err != nil {
		h.node.SendMessageOut("[Error]" + err.Error())
		h.node.SendMessageOut("Fail to resolve address, node will stop." + EXIT_TIP)
		h.node.IfStop <- STOP
		return
	}
	lis, err := net.ListenTCP("tcp", addr)
	h.listener = lis
	if err != nil {
		h.node.SendMessageOut("[Error]" + err.Error())
		h.node.SendMessageOut("Fail to listen, node will stop." + EXIT_TIP)
		h.node.IfStop <- STOP
		return
	}
	h.node.SendMessageOut("Start to listen on TCP address " + addr.String())
}

func (h *rpcServer) accept() {
	for len(h.node.IfStop) == 0 {
		iconn, ierr := h.listener.Accept()
		if ierr != nil {
			h.node.SendMessageOut("[NETWORK ERROR]" + ierr.Error())
			return
		} else {
			go h.server.ServeConn(iconn)
		}
	}
}

func (h *rpcServer) closestPrecedingNodeInFingerTable(v HashedValue) NodeInfo {
	for pos := int((HASHED_ADDRESS_LENGTH - 1)); pos >= 0; pos -= 1 {
		self := hashAddressFromNodeInfo(&h.node.Info)
		rmtnd := hashAddressFromNodeInfo(&h.node.nodeFingerTable.table[pos].remoteNode)
		if (h.node.nodeFingerTable.table[pos].remoteNode.IpAddress != "") && Between(&self, &rmtnd, &v.V, false) {
			if h.pingWithNodeInfo("a", &h.node.nodeFingerTable.table[pos].remoteNode) != "" {
				return h.node.nodeFingerTable.table[pos].remoteNode
			}
		}
	}
	return h.node.Info
}

func (h *rpcServer) closestPrecedingNodeInSuccessorList(v HashedValue) NodeInfo {
	var stpos int = -1
	for i := 0; i < int(MAX_SUCCESSORLIST_LEN); i += 1 {
		if h.node.nodeSuccessorList.list[i] == h.node.Info {
			stpos = i
			break
		}
	}
	if stpos == -1 {
		stpos = int(MAX_SUCCESSORLIST_LEN) - 1
	}
	for pos := stpos - 1; pos >= 0; pos -= 1 {
		self := hashAddressFromNodeInfo(&h.node.Info)
		rmtnd := hashAddressFromNodeInfo(&h.node.nodeSuccessorList.list[pos])
		if (h.node.nodeSuccessorList.list[pos].IpAddress != "") && Between(&self, &rmtnd, &v.V, false) {
			if h.pingWithNodeInfo("a", &h.node.nodeSuccessorList.list[pos]) != "" {
				return h.node.nodeSuccessorList.list[pos]
			}
		}
	}
	return h.node.Info
}

func (h *rpcServer) closestPrecedingNode(v HashedValue) NodeInfo {
	var closestNodeInSuccList, closestNodeInFingerTable NodeInfo
	closestNodeInFingerTable = h.closestPrecedingNodeInFingerTable(v)
	closestNodeInSuccList = h.closestPrecedingNodeInSuccessorList(v)
	fhash := hashAddressFromNodeInfo(&closestNodeInFingerTable)
	shash := hashAddressFromNodeInfo(&closestNodeInSuccList)
	nhash := hashAddressFromNodeInfo(&h.node.Info)
	if Between(&nhash, &fhash, &shash, true) {
		return closestNodeInSuccList
	} else {
		return closestNodeInFingerTable
	}
}

func (h *rpcServer) rpcDial(addr string) *net.Conn {
	tconn, err := net.DialTimeout("tcp", addr, h.timeout)
	if err != nil {
		h.node.SendMessageOut("Dial fail " + addr + " " + err.Error())
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
			h.node.SendMessageOut("close error:" + err.Error())
		}
		h.node.SendMessageOut("Call Fail:" + err.Error())
		return ""
	} else {
		cl.Close()
		return relpy.From.GetAddrWithPort() + ":" + relpy.Name
	}
}

func (h *rpcServer) pingWithNodeInfo(g string, nif *NodeInfo) string {
	return h.ping(g, nif.IpAddress+":"+strconv.Itoa(int(nif.Port)))
}

func (h *rpcServer) put(k string, v string) bool {
	var arg HashedValue
	var arg1 NodeData
	var ret NodeValue
	var ret1 Greet
	var cl *rpc.Client
	hashedKey := hashString(k)
	arg.From = h.node.Info
	arg.V = hashedKey
	ferr := h.service.FindSuccessorInit(arg, &ret)
	if ferr != nil {
		h.node.SendMessageOut("Put fail when find succ")
		return false
	}
	tconn := h.rpcDialWithNodeInfo(&ret.V)
	if tconn == nil {
		h.node.SendMessageOut("Dial fail when put")
		return false
	}
	arg1.From = h.node.Info
	arg1.Key = k
	arg1.Value = v
	cl = rpc.NewClient(*tconn)
	err := cl.Call("RingRPC.Put", &arg1, &ret1)
	cl.Close()
	if err != nil {
		h.node.SendMessageOut("Put error: " + err.Error())
		return false
	} else {
		h.node.SendMessageOut("Put Success")
		return true
	}

}

func (h *rpcServer) get(k string) (string, bool) {
	var arg HashedValue
	var arg1 NodeData
	var ret NodeValue
	var ret1 NodeData
	var cl *rpc.Client
	hashedKey := hashString(k)
	arg.From = h.node.Info
	arg.V = hashedKey
	ferr := h.service.FindSuccessorInit(arg, &ret)
	if ferr != nil {
		h.node.SendMessageOut("Get fail when find succ: " + ferr.Error())
		return "", false
	}
	arg1.From = h.node.Info
	arg1.Key = k
	tconn := h.rpcDialWithNodeInfo(&ret.V)
	if tconn == nil {
		h.node.SendMessageOut("Get fail when dial succ")
		return "", false
	}
	cl = rpc.NewClient(*tconn)
	err := cl.Call("RingRPC.Get", &arg1, &ret1)
	cl.Close()
	if err != nil {
		h.node.SendMessageOut("Get fail when Call" + err.Error())
		return "", false
	} else {
		h.node.SendMessageOut("Get success")
		return ret1.Value, true
	}
}

func (h *rpcServer) remove(k string) bool {
	var arg HashedValue
	var arg1 NodeData
	var ret NodeValue
	var ret1 Greet
	var cl *rpc.Client
	arg.V = hashString(k)
	arg.From = h.node.Info
	ferr := h.service.FindSuccessorInit(arg, &ret)
	if ferr != nil {
		h.node.SendMessageOut("Remove fail when find succ: " + ferr.Error())
		return false
	}
	arg1.From = h.node.Info
	arg1.Key = k
	tconn := h.rpcDialWithNodeInfo(&ret.V)
	if tconn == nil {
		h.node.SendMessageOut("Remove fail when dial succ")
		return false
	}
	cl = rpc.NewClient(*tconn)
	err := cl.Call("RingRPC.Remove", &arg1, &ret1)
	cl.Close()
	if err != nil {
		h.node.SendMessageOut("Remove fail when call " + err.Error())
		return false
	} else {
		if ret1.Name == "Not Found" {
			h.node.SendMessageOut("Key not found")
			return false
		}
		h.node.SendMessageOut("Remove Success")
		return true
	}
}

func (h *rpcServer) quit() {
	var ret Greet
	var arg1 NodeValue
	var arg2 NodeQuitData
	var tconn *net.Conn
	arg1.From = h.node.Info
	arg2.From = h.node.Info
	arg1.V = h.node.nodeSuccessorList.list[int(MAX_SUCCESSORLIST_LEN)-1]
	if !(h.node.nodeFingerTable.predecessor.IpAddress == "") && !h.node.nodeFingerTable.predecessor.Equal(&h.node.Info) {
		tconn = h.rpcDialWithNodeInfo(&h.node.nodeFingerTable.predecessor)
		if tconn == nil {
			h.node.SendMessageOut("Dail pre fail when quit")
		} else {
			cl := rpc.NewClient(*tconn)
			preErr := cl.Call("RingRPC.NotifyLeaveAsPre", &arg1, &ret)
			if preErr != nil || ret.Name != "Success" || ret.From.IpAddress == "" || ret.From.Port == 0 {
				h.node.SendMessageOut("Notify pre fail when quit " + preErr.Error())
			}
			cl.Close()
		}
	}
	if len(h.node.data) > 0 && !h.node.Info.Equal(&h.node.nodeSuccessorList.list[0]) {
		tconn = h.rpcDialWithNodeInfo(&h.node.nodeSuccessorList.list[0])
		if tconn == nil {
			h.node.SendMessageOut("Dail succ fail when quit")
		} else {
			arg2.DataSet = make(map[string]string)
			for k, v := range h.node.data {
				arg2.DataSet[k] = v
			}
			cl := rpc.NewClient(*tconn)
			preErr := cl.Call("RingRPC.NotifyLeaveAsSucc", &arg2, &ret)
			if preErr != nil || ret.Name != "Success" || ret.From.IpAddress == "" || ret.From.Port == 0 {
				h.node.SendMessageOut("Notify succ fail when quit " + preErr.Error())
			}
			cl.Close()
		}
	}
}

func (h *rpcServer) checkPredecessor(wg *sync.WaitGroup) {
	defer wg.Done()
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
	if len(h.node.IfStop) > 0 || h.node.InRing == false || h.node.nodeFingerTable.predecessor.IpAddress == "" {
		return
	}
	//PrintLog("check pre")
	if h.ping("a", h.node.nodeFingerTable.predecessor.GetAddrWithPort()) == "" {
		h.node.SendMessageOut("Predecessor fail, set to null")
		h.node.rpcModule.predecessorLocker.Lock()
		h.node.nodeFingerTable.predecessor.Reset()
		h.node.rpcModule.predecessorLocker.Unlock()
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
	//fmt.Println("FIXFINGER QUIT")
	wg.Done()
}

func (h *rpcServer) doFixFinger() {
	if len(h.node.IfStop) > 0 || !h.node.InRing {
		//h.node.SendMessageOut("node stop or not in ring,fixfinger stop", 0)
		return
	}
	//h.node.SendM("Start fix finger "+strconv.Itoa(h.currentFix), 0)
	var ret NodeValue
	var hv HashedValue
	hv.From = h.node.Info
	hv.V = h.node.nodeFingerTable.table[h.currentFix].HashedStartAddress
	err := h.service.FindSuccessorInit(hv, &ret)
	if err != nil {
		h.node.SendMessageOut("Find succ fail when fix finger" + err.Error())
		return
	} else {
		h.node.rpcModule.fingerTableLockerList[h.currentFix].Lock()
		h.node.nodeFingerTable.table[h.currentFix].remoteNode = ret.V
		h.node.rpcModule.fingerTableLockerList[h.currentFix].Unlock()
		//h.node.SendMessageOut("update finger "+strconv.Itoa(h.currentFix)+"to"+ret.V.GetAddrWithPort(), 0)
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
		h.node.SendMessageOut("Dial fail when join: " + addrWithPort)
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
			h.node.SendMessageOut("Call remote FindSuccessorInit fail:" + rerr.Error())
			return false
		} else {
			cl.Close()
			h.node.nodeSuccessorList.list[0] = ret.V
			h.node.nodeFingerTable.table[0].remoteNode = ret.V
			h.node.SendMessageOut("Update successor[0]: " + ret.V.GetAddrWithPort())
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
	//fmt.Println("STAB QUIT")
	wg.Done()
}

func (h *rpcServer) doStabilize() {
	//h.node.SendMessageOut("Start stab", 0)
	if len(h.node.IfStop) > 0 || !h.node.InRing {
		//h.node.SendMessageOut("Node stop or not in ring, stab exit", 0)
		return
	}
	//h.node.SendMessageOut("Start stabilize")
	var arg HashedValue
	var reply NodeInfo
	var arg1 NodeValue
	var reply1 Greet
	var argSucc Greet
	var replySucc SuccListInfo
	var target *NodeInfo
	var tconn *net.Conn = nil
	var ifChanged bool = false
	//h.node.SendMessageOut("Do start stab", 0)
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
			//PrintLog("Remove first entry")
			//	h.node.nodeSuccessorList.DumpSuccessorList()
			for i := 0; i < int(MAX_SUCCESSORLIST_LEN-1); i += 1 {
				h.node.nodeSuccessorList.list[i] = h.node.nodeSuccessorList.list[i+1]
			}
			h.node.nodeSuccessorList.list[int(MAX_SUCCESSORLIST_LEN-1)].Reset()
			//	h.node.nodeSuccessorList.DumpSuccessorList()
		}
	}
	arg.From = h.node.Info
	arg.V = hashAddressFromNodeInfo(&h.node.Info)
	cl := rpc.NewClient(*tconn)
	rerr := cl.Call("RingRPC.GetPredecessor", &arg, &reply)
	if rerr != nil {
		cl.Close()
		h.node.SendMessageOut("Call remote GetPredecssor fail: " + target.GetAddrWithPort())
		return
	} else {
		n := hashAddressFromNodeInfo(&h.node.Info)
		x := hashAddressFromNodeInfo(&reply)
		successor := hashAddressFromNodeInfo(&h.node.nodeSuccessorList.list[0])
		if reply.IpAddress != "" && Between(&n, &x, &successor, false) {
			ifChanged = true
			h.node.rpcModule.successorListLocker.Lock()
			h.node.nodeSuccessorList.list[0] = reply
			h.node.rpcModule.successorListLocker.Unlock()
			h.node.SendMessageOut("Update successor: " + reply.GetAddrWithPort())
		}
		if h.node.nodeSuccessorList.list[0].IpAddress != "" {
			if ifChanged {
				cl.Close()
				tconn = h.node.rpcModule.rpcDialWithNodeInfo(&h.node.nodeSuccessorList.list[0])
				if tconn == nil {
					h.node.SendMessageOut("Dial fail when stab when copy successor's succ list")
					return
				}
				cl = rpc.NewClient(*tconn)
			}
			argSucc.From = h.node.Info
			serr := cl.Call("RingRPC.GetSuccessorList", &argSucc, &replySucc)
			if serr != nil {
				h.node.SendMessageOut("Copy successor list fail" + serr.Error() + " " + h.node.nodeSuccessorList.list[0].GetAddrWithPort())
			} else {
				h.node.rpcModule.successorListLocker.Lock()
				for i := 1; i < int(MAX_SUCCESSORLIST_LEN); i += 1 {
					h.node.nodeSuccessorList.list[i] = replySucc.SuccList[i-1]
				}
				h.node.rpcModule.successorListLocker.Unlock()
				//h.node.nodeSuccessorList.DumpSuccessorList()
				//h.node.SendMessageOut("Copy successor list success" + " " + h.node.nodeSuccessorList.list[0].GetAddrWithPort())
			}
		}
		arg1.From = h.node.Info
		arg1.V = h.node.Info
		//h.node.SendMessageOut("Call notify", 0)
		rerr = cl.Call("RingRPC.Notify", &arg1, &reply1)
		cl.Close()
		if rerr != nil {
			h.node.SendMessageOut("Notify Call fail: " + h.node.nodeSuccessorList.list[0].GetAddrWithPort() + ":" + rerr.Error())
		} else {
			//h.node.SendMessageOut("Notify Success to: "+h.node.nodeSuccessorList.list[ptr].GetAddrWithPort(), 0)
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
	//PrintLog("New FindSucc request from" + p.From.GetAddrWithPort())
	n := hashAddressFromNodeInfo(&h.node.Info)
	successor := hashAddressFromNodeInfo(&h.node.nodeSuccessorList.list[0])
	if Between(&n, &p.V, &successor, true) {
		ret.V = h.node.nodeSuccessorList.list[0]
		ret.From = h.node.Info
		ret.Status = true
		return
	} else {
		ret.V = h.node.rpcModule.closestPrecedingNode(p)
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
		reply.V = h.node.rpcModule.closestPrecedingNode(tp)
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
	//PrintLog("New Ping Received")
	ret.Name = "Hello" + p.Name
	return
}

func (h *RpcServiceModule) Notify(arg NodeValue, reply *Greet) (err error) {
	var argData NodeDataSet
	var retData Greet
	if arg.V.IpAddress == "" {
		err = errors.New("Why you give me a FUCKING EMPTY ADDRESS?")
		return err
	} else {
		//PrintLog("ReceiveNotify")
		pre := hashAddressFromNodeInfo(&h.node.nodeFingerTable.predecessor)
		myargv := hashAddressFromNodeInfo(&arg.V)
		self := hashAddressFromNodeInfo(&h.node.Info)
		if h.node.nodeFingerTable.predecessor.IpAddress == "" || Between(&pre, &myargv, &self, false) {
			argData.DataSet = make(map[string]string)
			h.node.rpcModule.predecessorLocker.Lock()
			h.node.nodeFingerTable.predecessor = arg.V
			h.node.SendMessageOut("Receive Notify, update pre to:" + arg.V.GetAddrWithPort())
			// Transfer some data to pre
			if !(arg.V.IpAddress == h.node.Info.IpAddress && arg.V.Port == h.node.Info.Port) {
				tconn := h.node.rpcModule.rpcDialWithNodeInfo(&arg.V)
				if tconn == nil {
					h.node.SendMessageOut("Fail to dial when try to transfer data to pre, set to nil")
					h.node.nodeFingerTable.predecessor.Reset()
					h.node.rpcModule.predecessorLocker.Unlock()
					return nil
				} else {
					cl := rpc.NewClient(*tconn)
					h.node.getDataForPre(&arg.V, &argData.DataSet)
					argData.From = h.node.Info
					err := cl.Call("RingRPC.PutMany", &argData, &retData)
					cl.Close()
					if err != nil || retData.Name != "Success" {
						h.node.SendMessageOut("Transfer Keys to pre error:" + err.Error())
					}
				}
			}
			h.node.rpcModule.predecessorLocker.Unlock()
		}
		reply.Name = "Success"
		reply.From = h.node.Info
		return nil
	}
}

func (h *RpcServiceModule) Remove(arg NodeData, ret *Greet) (err error) {
	ret.From = h.node.Info
	if arg.From.IpAddress == "" || arg.From.Port == 0 {
		err := errors.New("Who you are! Remove fail")
		ret.Name = ""
		return err
	} else {
		h.node.dataLocker.Lock()
		_, ok := h.node.data[arg.Key]
		if !ok {
			ret.Name = "Not Found"
		} else {
			delete(h.node.data, arg.Key)
			ret.Name = "Success"
		}
		h.node.dataLocker.Unlock()
		return nil
	}
}

func (h *RpcServiceModule) Put(arg NodeData, s *Greet) (err error) {
	s.From = h.node.Info
	if arg.From.IpAddress == "" || arg.From.Port == 0 {
		err = errors.New("Who you are! Put fail")
		s.Name = ""
		return err
	} else {
		h.node.dataLocker.Lock()
		h.node.data[arg.Key] = arg.Value
		h.node.dataLocker.Unlock()
		s.Name = "Success"
		return nil
	}
}

func (h *RpcServiceModule) Get(arg NodeData, ret *NodeData) (err error) {
	if arg.From.IpAddress == "" || arg.From.Port == 0 {
		err = errors.New("Who you are! Get fail")
		return err
	} else {
		//Not lock and have a try
		ret.From = h.node.Info
		ret.Key = arg.Key
		s, ok := h.node.data[arg.Key]
		if !ok {
			ret.Value = "Not Found"
			return nil
		} else {
			ret.Value = s
			return nil
		}
	}
}

func (h *RpcServiceModule) PutMany(arg NodeDataSet, ret *Greet) (err error) {
	ret.From = h.node.Info
	if arg.From.IpAddress == "" || arg.From.Port == 0 {
		err = errors.New("Who you are! PutMany fail")
		return err
	}
	h.node.dataLocker.Lock()
	for k, v := range arg.DataSet {
		h.node.data[k] = v
	}
	h.node.dataLocker.Unlock()
	ret.Name = "Success"
	return nil
}

func (h *RpcServiceModule) GetAll(arg Greet, ret *NodeDataSet) (err error) {
	if arg.From.IpAddress == "" || arg.From.Port == 0 {
		err = errors.New("Who you are! GetAll fail")
		return err
	}
	ret.From = h.node.Info
	ret.DataSet = make(map[string]string)
	for k, v := range h.node.data {
		ret.DataSet[k] = v
	}
	return nil
}

func (h *RpcServiceModule) NotifyLeaveAsPre(arg NodeValue, ret *Greet) (err error) {
	//TODO lock SuccessorList lock fingerTable
	ret.From = h.node.Info
	if arg.V.IpAddress == "" {
		err = errors.New("Why you give me a FUCKING EMPTY ADDRESS?")
		ret.Name = ""
		return err
	} else {
		h.node.rpcModule.successorListLocker.Lock()
		h.node.rpcModule.fingerTableLockerList[0].Lock()
		var stpos int = -1
		for i := 0; i < int(MAX_SUCCESSORLIST_LEN)-1; i += 1 {
			if h.node.nodeSuccessorList.list[i].Equal(&arg.From) {
				stpos = i
				break
			}
		}
		if stpos != -1 {
			for i := stpos; i < int(MAX_SUCCESSORLIST_LEN)-1; i += 1 {
				h.node.nodeSuccessorList.list[i] = h.node.nodeSuccessorList.list[i+1]
			}
			h.node.nodeSuccessorList.list[int(MAX_SUCCESSORLIST_LEN)-1] = arg.V // last element of next
			h.node.nodeFingerTable.table[0].remoteNode = h.node.nodeSuccessorList.list[0]
		}
		h.node.rpcModule.successorListLocker.Unlock()
		h.node.rpcModule.fingerTableLockerList[0].Unlock()
		ret.Name = "Success"
		return nil
	}
}

func (h *RpcServiceModule) NotifyLeaveAsSucc(arg NodeQuitData, ret *Greet) (err error) {
	//TODO lock pre ,getdata & update pre
	if arg.From.IpAddress == "" || arg.From.Port == 0 {
		err = errors.New("Who you are! NotifyLeaveAsSucc fail")
		return err
	}
	h.node.rpcModule.predecessorLocker.Lock()
	h.node.nodeFingerTable.predecessor = arg.Pre
	h.node.rpcModule.predecessorLocker.Unlock()
	if len(arg.DataSet) > 0 {
		h.node.dataLocker.Lock()
		for k, v := range arg.DataSet {
			h.node.data[k] = v
		}
		h.node.dataLocker.Unlock()
	}
	ret.From = h.node.Info
	ret.Name = "Success"
	return nil
}
