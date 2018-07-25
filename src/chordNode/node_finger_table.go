package chordNode

type nodeInfo struct {
	ipAddress string
	port      int
}

type fingerTable struct {
	table       [HASHED_ADDRESS_LENGTH]nodeInfo
	predecessor nodeInfo
}

func NewFingerTable() *fingerTable {
	ret := new(fingerTable)
	return ret
}
