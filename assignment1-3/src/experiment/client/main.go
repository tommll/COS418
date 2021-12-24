package main

import (
	"fmt"
	"log"
	"net/rpc"
	"strconv"
	"time"
)

func main() {
	c, err := rpc.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	interval := time.Duration(10000) * time.Microsecond
	timeout := time.Duration(2000) * time.Microsecond
	ticker := time.NewTicker(time.Duration(interval))
	// f, err := os.Create("RRTdata.txt")

	for i := 0; ; i++ {
		<-ticker.C

		fmt.Println("Send HB at", time.Now().UnixMicro())
		start := time.Now().UnixMicro()
		rcvCh := sendHeartBeat(c, "Server.ReceiveHeartBeat", strconv.Itoa(i))

		// f.Write([]byte(strconv.Itoa(RRT) + "\n"))

		select {
		case <-time.After(timeout):
			end := time.Now().UnixMicro()
			RRT := int(end - start)
			fmt.Println("TIME OUT after", RRT)

		case <-rcvCh:
			end := time.Now().UnixMicro()
			RRT := int(end - start)
			fmt.Println("Heartbeat received")
			fmt.Println("RRT:", RRT)

		}

	}
}

func sendHeartBeat(c *rpc.Client, method, request string) <-chan string {
	ch := make(chan string)

	go func() {
		s := ""
		err := c.Call(method, request, &s)
		if err != nil {
			log.Fatal(err)
		} else {
			ch <- s
		}
	}()

	return ch
}
