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
