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
	MAX_DATA   int64 = int64(1e4)
	START_PORT int   = 1111
)

var nodeGroup [MAX_NODE]*chordNode.RingNode
var wg sync.WaitGroup
var msg chordNode.CtrlMessage
var Stop bool = false
var localIp string
var datalocal map[string]string
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

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

func testWhenStab() {
	datalocal = make(map[string]string)
	for i := 0; i < MAX_NODE; i += 1 {
		nodeGroup[i] = chordNode.NewNode(int32(START_PORT + i))
	}
	localIp = getIp()
	wg.Add(1)
	go nodeGroup[0].Run(&wg)
	nodeGroup[0].Create()
	for i := 1; i < MAX_NODE; i += 1 {
		wg.Add(1)
		go nodeGroup[i].Run(&wg)
		time.Sleep(time.Millisecond * 20)
		nodeGroup[i].Join(localIp + ":" + strconv.Itoa(rand.Intn(i)+1111))
	}
	go readMsg()
	time.Sleep(30 * time.Second)
	for i := int64(0); i < MAX_DATA; i += 1 {
		k := randString(50)
		v := randString(50)
		datalocal[k] = v
		nodeGroup[rand.Intn(MAX_NODE)].Put(k, v)
		time.Sleep(time.Millisecond * 10)
	}
	for k, v := range datalocal {
		ret, ok := nodeGroup[rand.Intn(MAX_NODE)].Get(k)
		if !ok {
			fmt.Println("Test Fail")
		} else {
			if ret != v {
				fmt.Println("Test Fail")
			} else {
				fmt.Println("Test Success")
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
	for i := 0; i < MAX_NODE; i += 1 {
		nodeGroup[i].Quit()
		time.Sleep(time.Second)
	}
	//Stop = true
	wg.Wait()
}

func testForceQuit() {
	datalocal = make(map[string]string)
	for i := 0; i < MAX_NODE; i += 1 {
		nodeGroup[i] = chordNode.NewNode(int32(START_PORT + i))
	}
	localIp = getIp()
	wg.Add(1)
	go nodeGroup[0].Run(&wg)
	nodeGroup[0].Create()
	for i := 1; i < MAX_NODE; i += 1 {
		wg.Add(1)
		go nodeGroup[i].Run(&wg)
		time.Sleep(time.Millisecond * 20)
		nodeGroup[i].Join(localIp + ":" + strconv.Itoa(rand.Intn(i)+1111))
	}
	go readMsg()
	for i := 0; i < MAX_NODE; i += 1 {
		nodeGroup[i].IfStop <- chordNode.STOP
		time.Sleep(time.Millisecond * 500)
	}
	//Stop = true
	wg.Wait()
}

func testWhileJoin() {
	datalocal = make(map[string]string)
	for i := 0; i < MAX_NODE; i += 1 {
		nodeGroup[i] = chordNode.NewNode(int32(START_PORT + i))
	}
	localIp = getIp()
	wg.Add(1)
	go readMsg()
	go nodeGroup[0].Run(&wg)
	nodeGroup[0].Create()
	for i := 1; i < MAX_NODE; i += 1 {
		wg.Add(1)
		go nodeGroup[i].Run(&wg)
		time.Sleep(time.Millisecond * 100)
		nodeGroup[i].Join(localIp + ":" + strconv.Itoa(rand.Intn(i)+1111))
		for j := 1; j <= 100; j += 1 {
			k := randString(50)
			v := randString(50)
			datalocal[k] = v
			nodeGroup[rand.Intn(i)].Put(k, v)
			time.Sleep(time.Millisecond * 10)
		}
	}
	time.Sleep(time.Millisecond * 1000)
	for k, v := range datalocal {
		ret, ok := nodeGroup[rand.Intn(MAX_NODE)].Get(k)
		if !ok {
			fmt.Println("Test Fail")
		} else {
			if ret != v {
				fmt.Println("Test Fail")
			} else {
				fmt.Println("Test Success")
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
	for i := 0; i < MAX_NODE; i += 1 {
		nodeGroup[i].Quit()
		time.Sleep(time.Millisecond * 500)
	}
	//Stop = true
	wg.Wait()
}

func main() {
	//testWhenStab()
	//testForceQuit()
	testWhileJoin()
}
