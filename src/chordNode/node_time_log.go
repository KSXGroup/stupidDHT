package chordNode

import (
	"fmt"
	"time"
)

func PrintLog(log string) {
	s := "[" + time.Unix(time.Now().Unix(), 0).Format("20060102150405") + "]" + log + "\n"
	fmt.Print(s)
}
