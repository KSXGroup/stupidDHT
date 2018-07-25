package chordNode

import (
	"math/big"
)

type nodeInfo struct {
	ipAddress     string
	port          int32
	hashedAddress big.Int
}

type fingerTable struct {
	table       [HASHED_ADDRESS_LENGTH]nodeInfo
	predecessor nodeInfo
}

func NewFingerTable() *fingerTable {
	ret := new(fingerTable)
	return ret
}

func NewNodeInfo(ip string, port int32) *nodeInfo {
	ret := new(nodeInfo)
	ret.ipAddress = ip
	ret.port = port
	ret.hashedAddress = hashAddress(ip, port)
	return ret
}
