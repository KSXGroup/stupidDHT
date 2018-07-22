// node
package chord

import (
	"crypto/sha256"
	"fmt"
	//"math/big"
	"net"
)

type ringNode struct {
	ipAddress     string
	port          int32
	hashedAddress [256]byte
}

func (n *ringNode) PrintName() {
	fmt.Println(n.ipAddress)
}

func (n *ringNode) Hash(ip string, port int32) {}

func NewNode(port int32) *ringNode {
	var ret = new(ringNode)
	ret.getIp()
	ret.port = port
	ret.hashedAddress = ret.Hash(ret.ipAddress, ret.port)
	return ret
}

func (n *ringNode) getIp() {
	addrList, err := net.InterfaceAddrs()
	if err != nil {

	}
}
