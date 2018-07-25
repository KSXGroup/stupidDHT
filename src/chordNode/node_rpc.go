package chordNode

import (
	"errors"
	"net/rpc"
)

type rpcMessage struct {
	Name string
}

func (n *RingNode) rpcHello(req rpcMessage, resp *rpcMessage) (err error) {
	if req.Name == "" {
		err = errors.New("YOU SEND NOTHING")
		return
	}
	resp.Name = "FUCK, " + resp.Name
	return
}
func (n *RingNode) notify(target *NodeInfo) {}
func (n *RingNode) ping(target *NodeInfo)   {}
func (n *RingNode) stablize()               {}
func (n *RingNode) findSuccessor()          {}
