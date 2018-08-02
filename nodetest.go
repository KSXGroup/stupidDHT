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
	MAX_NODE   int   = 50
	MAX_DATA   int64 = int64(1e3)
	START_PORT int   = 1111
)

var nodeGroup [MAX_NODE]*chordNode.RingNode
var keyArray [MAX_DATA]string
var keyPos int = 0
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

func testWhenStabAndQuit() {
	fmt.Println("Start test when stable")
	datalocal = make(map[string]string)
	var failcnt int = 0
	for i := 0; i < MAX_NODE; i += 1 {
		nodeGroup[i] = chordNode.NewNode(int32(START_PORT + i))
	}
	localIp = getIp()
	wg.Add(1)
	go nodeGroup[0].Run(&wg)
	nodeGroup[0].Create()
	fmt.Println("Join node together")
	for i := 1; i < MAX_NODE; i += 1 {
		wg.Add(1)
		go nodeGroup[i].Run(&wg)
		time.Sleep(time.Millisecond * 50)
		nodeGroup[i].Join(localIp + ":" + strconv.Itoa(rand.Intn(i)+1111))
	}
	fmt.Println("Finished")
	go readMsg()
	fmt.Println("Wait for 30 seconds to stabilize and fix")
	time.Sleep(30 * time.Second)
	fmt.Println("Finished")
	fmt.Println("Put data")
	for i := int64(0); i < MAX_DATA; i += 1 {
		k := randString(50)
		v := randString(50)
		keyArray[i] = k
		datalocal[k] = v
		nodeGroup[rand.Intn(MAX_NODE)].Put(k, v)
		time.Sleep(time.Millisecond * 10)
	}
	fmt.Println("Finished")
	fmt.Println("Get data")
	for k, v := range datalocal {
		ret, ok := nodeGroup[rand.Intn(MAX_NODE)].Get(k)
		if !ok {
			fmt.Println("Test Fail")
		} else {
			if ret != v {
				failcnt += 1
				fmt.Println("Test Fail")
			} else {
				//fmt.Println("Test Success")
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
	fmt.Printf("Test finished, fail rate: %d / %d\n", failcnt, len(datalocal))
	failcnt = 0
	fmt.Println("Node start to quit, start to test when quit")
	for i := 0; i < MAX_NODE; i += 1 {
		for j := 1; j <= 10; j += 1 {
			rk := keyArray[rand.Intn(int(MAX_DATA))]
			ret, ok := nodeGroup[i].Get(rk)
			if !ok {
				fmt.Println("Test Fail")
				failcnt += 1
			} else {
				if ret != datalocal[rk] {
					failcnt += 1
					fmt.Println("Test Fail")
				} else {
					//fmt.Println("Test Success")
				}
			}
			time.Sleep(time.Millisecond * 10)
		}
		nodeGroup[i].Quit()
		time.Sleep(time.Millisecond * 200)
	}
	fmt.Printf("Test finished, fail rate: %d / %d\n", failcnt, MAX_NODE*10)
	//Stop = true
	wg.Wait()
	fmt.Println("Finished")
}

/*func testForceQuit() {
	fmt.Println("Start Test Force Quit")
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
		time.Sleep(time.Millisecond * 400)
	}
	//Stop = true
	wg.Wait()
	fmt.Println("Exit Success")
}*/

/*func testload() {
	fmt.Println("Start Test Load Distribution")
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
		time.Sleep(time.Millisecond * 100)
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
	for i := 0; i < MAX_NODE; i += 1 {
		fmt.Printf("NODE#%d:%d\n", i, nodeGroup[i].GetDataSize())
	}
	for i := 0; i < MAX_NODE; i += 1 {
		nodeGroup[i].Quit()
		time.Sleep(time.Millisecond * 400)
	}
	wg.Wait()
	fmt.Println("Test Finished\n")
}*/

func testWhileJoin() {
	fmt.Println("Test when network is not stable")
	var failcnt int = 0
	datalocal = make(map[string]string)
	for i := 0; i < MAX_NODE; i += 1 {
		nodeGroup[i] = chordNode.NewNode(int32(START_PORT + i))
	}
	localIp = getIp()
	wg.Add(1)
	go readMsg()
	fmt.Println("Join node per 600ms, and put some data when join, put data per 10ms")
	go nodeGroup[0].Run(&wg)
	nodeGroup[0].Create()
	for i := 1; i < MAX_NODE; i += 1 {
		wg.Add(1)
		go nodeGroup[i].Run(&wg)
		time.Sleep(100 * time.Millisecond)
		nodeGroup[i].Join(localIp + ":" + strconv.Itoa(rand.Intn(i)+1111))
		for j := 1; j <= 10; j += 1 {
			k := randString(50)
			v := randString(50)
			datalocal[k] = v
			nodeGroup[rand.Intn(i)].Put(k, v)
			time.Sleep(time.Millisecond * 50)
		}
	}
	fmt.Println("Finished")
	fmt.Println("Wait for 1 second")
	time.Sleep(1000 * time.Millisecond)
	fmt.Println("Start get per 10ms Immediately")
	for k, v := range datalocal {
		ret, ok := nodeGroup[rand.Intn(MAX_NODE)].Get(k)
		if !ok {
			failcnt += 1
			fmt.Println("Test Fail when call get")
		} else {
			if ret != v {
				failcnt += 1
				fmt.Println("Test Fail")
			} else {
				//fmt.Println("Test Success")
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
	fmt.Printf("Test finished, fail rate: %d / %d\n", failcnt, len(datalocal))
	fmt.Println("Dump the load of nodes")
	for i := 0; i < MAX_NODE; i += 1 {
		fmt.Printf("NODE#%d:%d\n", i, nodeGroup[i].GetDataSize())
	}
	fmt.Println("Finished")
	fmt.Println("Node start to quit")
	for i := 0; i < MAX_NODE; i += 1 {
		nodeGroup[i].Quit()
		time.Sleep(time.Millisecond * 400)
	}
	//Stop = true
	wg.Wait()
	fmt.Println("Finished")
}

/*func testRandom() {
	fmt.Println("Test with random operation")
	var failcnt int = 0
	var exit [MAX_NODE]int
	var trKeyArray [MAX_NODE]string
	datalocal = make(map[string]string)
	for i := 0; i < MAX_NODE; i += 1 {
		nodeGroup[i] = chordNode.NewNode(int32(START_PORT + i))
	}
	localIp = getIp()
	wg.Add(1)
	go readMsg()
	fmt.Println("Join node per 500ms")
	go nodeGroup[0].Run(&wg)
	time.Sleep(time.Millisecond * 50)
	nodeGroup[0].Create()
	for i := 1; i < MAX_NODE; i += 1 {
		wg.Add(1)
		go nodeGroup[i].Run(&wg)
		time.Sleep(time.Millisecond * 50)
		nodeGroup[i].Join(localIp + ":" + strconv.Itoa(rand.Intn(i)+1111))
	}
	fmt.Println("Wait for 30 seconds")
	fmt.Println("Put 5e2 data randomly per 10ms")
	for i := 0; i <= 500; i += 1 {
		k := randString(50)
		v := randString(50)
		trKeyArray[i] = k
		datalocal[k] = v
		nodeGroup[rand.Intn(MAX_NODE)].Put(k, v)
	}
	pos := 500
	fmt.Println("Do something randomly every 100ms")
	for i := 1; i <= 5000; i += 1 {
		time.Sleep(time.Millisecond * 100)
		id := 0
		if i%500 == 0 {
			for {
				id = rand.Intn(MAX_NODE)
				if exit[id] == 0 {
					exit[id] = 1
					break
				}
			}
			nodeGroup[i].Quit()
		}
		for {
			id = rand.Intn(MAX_NODE)
			if exit[id] == 0 {
				break
			}
		}
		if len(datalocal) == MAX_DATA {
			for {
				tp := rand.Intn
				tp
			}
			k := trKeyArray[rand.Intn]
			ret, ok := nodeGroup[id].Get(k)
			if !ok {
				failcnt += 1
				fmt.Println("Test Fail when call get")
			} else {
				if ret != datalocal[k] {
					failcnt += 1
					fmt.Println("Test Fail")
				} else {
					//fmt.Println("Test Success")
				}
			}
		} else {
			op := rand.Intn(3)
			switch op {
			case 0:

			}
		}
	}
}*/

func testPutAndDelete() {
	fmt.Println("Start test put and delete")
	datalocal = make(map[string]string)
	var failcnt int = 0
	for i := 0; i < MAX_NODE; i += 1 {
		nodeGroup[i] = chordNode.NewNode(int32(START_PORT + i))
	}
	localIp = getIp()
	wg.Add(1)
	go nodeGroup[0].Run(&wg)
	nodeGroup[0].Create()
	fmt.Println("Join node together")
	for i := 1; i < MAX_NODE; i += 1 {
		wg.Add(1)
		go nodeGroup[i].Run(&wg)
		time.Sleep(time.Millisecond * 50)
		nodeGroup[i].Join(localIp + ":" + strconv.Itoa(rand.Intn(i)+1111))
	}
	fmt.Println("Finished")
	go readMsg()
	fmt.Println("Wait for 30 seconds to stabilize and fix")
	time.Sleep(30 * time.Second)
	fmt.Println("Finished")
	fmt.Println("Put data")
	for i := int64(0); i < MAX_DATA; i += 1 {
		k := randString(50)
		v := randString(50)
		keyArray[i] = k
		datalocal[k] = v
		nodeGroup[rand.Intn(MAX_NODE)].Put(k, v)
		time.Sleep(time.Millisecond * 10)
	}
	fmt.Println("Finished")
	fmt.Println("Get data")
	for k, v := range datalocal {
		ret, ok := nodeGroup[rand.Intn(MAX_NODE)].Get(k)
		if !ok {
			fmt.Println("Test Fail")
		} else {
			if ret != v {
				failcnt += 1
				fmt.Println("Test Fail")
			} else {
				//fmt.Println("Test Success")
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
	fmt.Printf("Test finished, fail rate: %d / %d\n", failcnt, len(datalocal))
	failcnt = 0
	fmt.Println("Remove all data randomly")
	for k, _ := range datalocal {
		nodeGroup[rand.Intn(MAX_NODE)].Remove(k)
		time.Sleep(time.Millisecond * 50)
	}
	fmt.Println("Finish")
	fmt.Println("Find them again")
	for k, _ := range datalocal {
		s, ok := nodeGroup[rand.Intn(MAX_NODE)].Get(k)
		if !ok {
			//fmt.Println("Test Fail")
		} else {
			if s != "Not Found" {
				failcnt += 1
				fmt.Println("Still found key " + k)
			}
		}
		time.Sleep(time.Millisecond * 50)
	}
	fmt.Printf("Test finished, fail rate: %d / %d\n", failcnt, len(datalocal))
	fmt.Println("Node start to quit")
	for i := 0; i < MAX_NODE; i += 1 {
		nodeGroup[i].Quit()
		time.Sleep(time.Millisecond * 400)
	}
	//Stop = true
	wg.Wait()
	fmt.Println("Finished")
}

func testRobust() {
	fmt.Println("Start test Robust")
	datalocal = make(map[string]string)
	var failcnt int = 0
	for i := 0; i < MAX_NODE; i += 1 {
		nodeGroup[i] = chordNode.NewNode(int32(START_PORT + i))
	}
	localIp = getIp()
	wg.Add(1)
	go nodeGroup[0].Run(&wg)
	nodeGroup[0].Create()
	fmt.Println("Join node together")
	for i := 1; i < MAX_NODE; i += 1 {
		wg.Add(1)
		go nodeGroup[i].Run(&wg)
		time.Sleep(time.Millisecond * 50)
		nodeGroup[i].Join(localIp + ":" + strconv.Itoa(rand.Intn(i)+1111))
	}
	fmt.Println("Finished")
	go readMsg()
	fmt.Println("Wait for 30 seconds to stabilize and fix")
	time.Sleep(30 * time.Second)
	fmt.Println("Finished")
	fmt.Println("Force some node to quit")
	for i := 40; i < MAX_NODE; i += 1 {
		nodeGroup[i].IfStop <- chordNode.STOP
		time.Sleep(time.Millisecond * 200)
	}
	fmt.Println("Finish")
	fmt.Println("Put data")
	for i := int64(0); i < MAX_DATA; i += 1 {
		k := randString(50)
		v := randString(50)
		keyArray[i] = k
		datalocal[k] = v
		nodeGroup[rand.Intn(MAX_NODE-10)].Put(k, v)
		time.Sleep(time.Millisecond * 10)
	}
	fmt.Println("Finished")
	fmt.Println("Get data")
	for k, v := range datalocal {
		ret, ok := nodeGroup[rand.Intn(MAX_NODE-10)].Get(k)
		if !ok {
			fmt.Println("Test Fail")
		} else {
			if ret != v {
				failcnt += 1
				fmt.Println("Test Fail")
			} else {
				//fmt.Println("Test Success")
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
	fmt.Printf("Test finished, fail rate: %d / %d\n", failcnt, len(datalocal))
	fmt.Println("Node start to quit")
	for i := 0; i < MAX_NODE-10; i += 1 {
		nodeGroup[i].Quit()
		time.Sleep(time.Millisecond * 200)
	}
	//Stop = true
	wg.Wait()
	fmt.Println("Finished")
}

func main() {
	testPutAndDelete()
	fmt.Print("\n")
	testWhenStabAndQuit()
	fmt.Print("\n")
	testWhileJoin()
	fmt.Print("\n")
	testRobust()
}
