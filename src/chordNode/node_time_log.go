package chordNode

import (
	"fmt"
	"time"
)

func (n *NodeConsole) PrintLog(log string) {
	fmt.Print("[" + time.Unix(time.Now().Unix(), 0).Format("20060102150405") + "]" + log + "\n")
}
