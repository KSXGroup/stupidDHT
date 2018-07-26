package chordNode

import (
	"math/big"
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
