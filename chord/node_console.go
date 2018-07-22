package chord

type NodeConsole struct {
	helpInfo string
	node     *ringNode
}

func NewNodeConsole(port int32) *NodeConsole {
	cmd := new(NodeConsole)
	cmd.node := NewNode(port)
	cmd.helpInfo := "No help info! You are on your own"
}

func main() {
	cmd := NewNodeConsole(114514)
}
