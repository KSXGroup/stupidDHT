package chordNode

import (
	"fmt"
	"time"
)

func PrintLog(log string) {
	s := "[" + time.Unix(time.Now().Unix(), 0).Format("2006/01/02 15:04:05") + "]" + log + "\n"
	fmt.Print(s)
}
