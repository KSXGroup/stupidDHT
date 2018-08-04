package main

import (
	"chordNode"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	MAX_NODE   int   = 200
	MAX_DATA   int64 = int64(1e3 + 500)
	START_PORT int   = 1111
)

var nodeGroup [MAX_NODE]*chordNode.RingNode
var keyArray [MAX_DATA]string
var keyPos int = 0
var wg *sync.WaitGroup
var msg chordNode.CtrlMessage
var Stop bool = false
var localIp string
var datalocal map[string]string
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var totfailcnt int = 0

func getIp() string {
	var ipAddress string
	addrList, err := net.InterfaceAddrs()
	if err != nil {
		panic("Fail to get IP address")
	}
	for _, a := range addrList {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ipAddress = ipnet.IP.String()
			}
		}
	}
	return ipAddress
}

func readMsg() {
	var ok bool
	for !Stop {
		time.Sleep(time.Millisecond * 10)
		for i := 0; i < MAX_NODE; i += 1 {
			for len(nodeGroup[i].NodeMessageQueueOut) > 0 {
				msg, ok = <-nodeGroup[i].NodeMessageQueueOut
				if !ok {
					break
				}
				fmt.Printf("[NODE#%d] %s\n", i, msg.GetName())
			}
		}
	}
}

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func main() {
	localIp = getIp()
	datalocal = make(map[string]string)
	wg = new(sync.WaitGroup)
	var joinpos int = 1
	var leavepos int = 0
	var failcnt int = 0
	var cnt int = 0
	for i := 0; i < int(MAX_NODE); i += 1 {
		nodeGroup[i] = chordNode.NewNode(int32(1111 + i))
		wg.Add(1)
		go nodeGroup[i].Run(wg)
	}
	go readMsg()
	time.Sleep(time.Millisecond * 200)
	nodeGroup[0].Create()
	for i := 1; i <= 5; i += 1 {
		fmt.Printf("Round#%d\n", i)
		fmt.Println("Start to join")
		for j := 1; j <= 15; j += 1 {
			nodeGroup[joinpos].Join(localIp + ":" + strconv.Itoa(1111+leavepos))
			time.Sleep(time.Millisecond * 1000)
			joinpos += 1
		}
		fmt.Println("Wait for 30 seconds")
		time.Sleep(time.Second * 30)
		fmt.Println("Start to put")
		for j := 1; j <= 300; j += 1 {
			k := randString(50)
			v := randString(50)
			datalocal[k] = v
			nodeGroup[rand.Intn(int(joinpos-leavepos))+leavepos].Put(k, v)
			fmt.Printf("Put %s\n", k)
			//time.Sleep(10 * time.Millisecond)
		}
		fmt.Println("Start to get")
		failcnt = 0
		for tk, tv := range datalocal {
			res, ok := nodeGroup[rand.Intn(int(joinpos-leavepos))+leavepos].Get(tk)
			if !ok {
				fmt.Println("Find fail")
				failcnt += 1
			} else {
				if res != tv {
					fmt.Println("Not found")
					failcnt += 1
				}
			}
			cnt += 1
			if cnt == 300 {
				break
			}
			//time.Sleep(time.Millisecond * 10)
		}
		fmt.Printf("Fail count: %d / 300\n", failcnt)
		totfailcnt += failcnt
		cnt = 0
		fmt.Println("Start to remove")
		for rk, _ := range datalocal {
			delete(datalocal, rk)
			ok1 := nodeGroup[rand.Intn(int(joinpos-leavepos))+leavepos].Remove(rk)
			cnt += 1
			if !ok1 {
				fmt.Println("Remove fail")
			}
			if cnt == 150 {
				break
			}
			//time.Sleep(time.Millisecond * 5)
		}
		fmt.Println("Start to quit")
		for j := 1; j <= 5; j += 1 {
			nodeGroup[leavepos].Quit()
			leavepos += 1
			time.Sleep(time.Millisecond * 1000)
		}
		fmt.Println("Wait for 30 seconds")
		time.Sleep(time.Second * 30)
		fmt.Printf("Start to put\n")
		for j := 1; j <= 300; j += 1 {
			k := randString(50)
			v := randString(50)
			datalocal[k] = v
			nodeGroup[rand.Intn(int(joinpos-leavepos))+leavepos].Put(k, v)
			fmt.Printf("Put %s\n", k)
			//time.Sleep(10 * time.Millisecond)
		}
		fmt.Println("Start to get")
		failcnt = 0
		for tk, tv := range datalocal {
			res, ok := nodeGroup[rand.Intn(int(joinpos-leavepos))+leavepos].Get(tk)
			if !ok {
				fmt.Println("Find fail")
				failcnt += 1
			} else {
				if res != tv {
					fmt.Println("Not found")
					failcnt += 1
				}
			}
			cnt += 1
			if cnt == 300 {
				break
			}
			//time.Sleep(time.Millisecond * 10)
		}
		fmt.Printf("Fail count: %d / 300\n", failcnt)
		totfailcnt += failcnt
		cnt = 0
		fmt.Println("Start to remove")
		for rk, _ := range datalocal {
			delete(datalocal, rk)
			ok1 := nodeGroup[rand.Intn(int(joinpos-leavepos))+leavepos].Remove(rk)
			cnt += 1
			if !ok1 {
				fmt.Println("Remove fail")
			}
			if cnt == 150 {
				break
			}
			//time.Sleep(time.Millisecond * 10)
		}
	}
	fmt.Printf("Total fail count: %d", totfailcnt)
}
