package chordNode

import (
	"fmt"
	"math/big"
	"strconv"
)

type NodeInfo struct {
	IpAddress string
	Port      int32
}

type tableInfo struct {
	remoteNode         NodeInfo
	HashedStartAddress big.Int
}

type fingerTable struct {
	table       []tableInfo
	predecessor NodeInfo
}

type successorList struct {
	list []NodeInfo
}

type NodeValue struct {
	V      NodeInfo
	From   NodeInfo
	Status bool
}

func newNodeValue(_v *NodeInfo, _f *NodeInfo, _s bool) *NodeValue {
	ret := new(NodeValue)
	ret.V = *_v
	ret.From = *_f
	ret.Status = _s
	return ret
}

func newSuccessorList() *successorList {
	ret := new(successorList)
	ret.list = make([]NodeInfo, HASHED_ADDRESS_LENGTH)
	return ret
}

func NewFingerTable() *fingerTable {
	ret := new(fingerTable)
	ret.table = make([]tableInfo, HASHED_ADDRESS_LENGTH)
	return ret
}

func NewNodeInfo(ip string, port int32) *NodeInfo {
	ret := new(NodeInfo)
	ret.IpAddress = ip
	ret.Port = port
	return ret
}

func AddPowerOfTwo(toAdd *big.Int, power int) *big.Int {
	bpower := big.NewInt(int64(power))
	two := big.NewInt(2)
	return new(big.Int).Mod(new(big.Int).Add(toAdd, new(big.Int).Exp(two, bpower, nil)), new(big.Int).Exp(two, big.NewInt(160), nil))
}

func (nif *NodeInfo) Print() {
	if nif.IpAddress != "" {
		fmt.Println(nif.IpAddress + ":" + strconv.Itoa(int(nif.Port)))
	} else {
		fmt.Println("[None]")
	}
}

func (nif *NodeInfo) Equal(o *NodeInfo) bool {
	if nif.Port == o.Port && nif.IpAddress == o.IpAddress {
		return true
	} else {
		return false
	}
}

func (nif *NodeInfo) GetAddrWithPort() string {
	return nif.IpAddress + ":" + strconv.Itoa(int(nif.Port))
}

func (nif *NodeInfo) Reset() {
	nif.IpAddress = ""
	nif.Port = 0
}

func (tif *tableInfo) Print() {
	fmt.Print("Start hashed address is:" + tif.HashedStartAddress.String() + " ,and the closest start from this address info is below\n")
	tif.remoteNode.Print()
}

func (slst *successorList) DumpSuccessorList() {
	for i := 0; i < int(MAX_SUCCESSORLIST_LEN); i += 1 {
		if slst.list[i].IpAddress == "" {
			break
		}
		fmt.Printf("#%d ", i)
		slst.list[i].Print()
	}
}

func (f *fingerTable) DumpFingerTable() {
	fmt.Print("Predecessor:")
	f.predecessor.Print()
	if f.table[0].remoteNode.IpAddress == "" {
		fmt.Println("There is nothing in finger table")
	} else {
		for i := 0; i < int(HASHED_ADDRESS_LENGTH); i += 1 {
			if f.table[i].remoteNode.IpAddress == "" {
				break
			} else {
				fmt.Printf("#%d ", i)
				f.table[i].Print()
			}
		}
	}
}

func (s *successorList) pushFront(item *NodeInfo) {
	for i := 0; i < int(MAX_SUCCESSORLIST_LEN-1); i += 1 {
		s.list[i+1] = s.list[i]
	}
	s.list[0] = *item
}
