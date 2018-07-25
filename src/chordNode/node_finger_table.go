package chordNode

type nodeInfo struct {
	ipAddress string
	port      int
}

type fingerTable struct {
	table       []nodeInfo
	predecessor nodeInfo
}

func NewFingerTable() *fingerTable {
	table = make(nodeInfo, HASHED_ADDRESS_LENGTH)
}
