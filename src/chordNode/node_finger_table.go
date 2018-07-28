package chordNode

import (
	"fmt"
	"math/big"
	"strconv"
)

type NodeInfo struct {
	IpAddress     string
	Port          int32
	HashedAddress big.Int
}

type tableInfo struct {
	remoteNode         NodeInfo
	HashedStartAddress big.Int
}

type fingerTable struct {
	table       [HASHED_ADDRESS_LENGTH]tableInfo
	predecessor NodeInfo
}

type successorList struct {
	list   [HASHED_ADDRESS_LENGTH]NodeInfo
	length uint8
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
	ret.length = 0
	return ret
}

func NewFingerTable() *fingerTable {
	ret := new(fingerTable)
	return ret
}

func NewNodeInfo(ip string, port int32) *NodeInfo {
	ret := new(NodeInfo)
	ret.IpAddress = ip
	ret.Port = port
	ret.HashedAddress = hashAddress(ip, port)
	return ret
}

func AddPowerOfTwo(toAdd *big.Int, power int) *big.Int {
	bpower := big.NewInt(int64(power))
	two := big.NewInt(2)
	return new(big.Int).Mod(new(big.Int).Add(toAdd, new(big.Int).Exp(two, bpower, nil)), new(big.Int).Exp(two, big.NewInt(160), nil))
}

func (nif *NodeInfo) Print() {
	if nif.IpAddress != "" {
		fmt.Println(nif.IpAddress + ":" + strconv.Itoa(int(nif.Port)) + " " + nif.HashedAddress.String())
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
	nif.HashedAddress.SetInt64(0)
}

func (tif *tableInfo) Print() {
	fmt.Print("Start hashed address is:" + tif.HashedStartAddress.String() + " ,and the closest start from this address info is below\n")
	tif.remoteNode.Print()
}

func (f *fingerTable) DumpFingerTable() {
	fmt.Print("Predecessor:")
	f.predecessor.Print()
	if f.table[0].remoteNode.IpAddress == "" {
		fmt.Println("There is nothing in finger table")
	} else {
		pos := 0
		for {
			if f.table[pos].remoteNode.IpAddress == "" {
				break
			} else {
				f.table[pos].Print()
				pos += 1
			}
		}
	}
}
