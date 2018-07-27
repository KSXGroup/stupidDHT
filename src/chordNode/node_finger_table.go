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

type fingerTable struct {
	table       [HASHED_ADDRESS_LENGTH]NodeInfo
	predecessor NodeInfo
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

func (nif *NodeInfo) Print() {
	if nif.IpAddress != "" {
		fmt.Println(nif.IpAddress + ":" + strconv.Itoa(int(nif.Port)) + " " + nif.HashedAddress.String())
	} else {
		fmt.Println("[None]")
	}
}

func (f *fingerTable) DumpFingerTable() {
	fmt.Print("Predecessor:")
	f.predecessor.Print()
	if f.table[0].IpAddress == "" {
		fmt.Println("There is nothing in finger table")
	} else {
		pos := 0
		for {
			if f.table[pos].IpAddress == "" {
				break
			} else {
				f.table[pos].Print()
				pos += 1
			}
		}
	}
}
