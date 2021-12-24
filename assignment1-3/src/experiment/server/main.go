package main

import (
	"log"
	"net"
	"net/rpc"
	"strconv"
	"time"
)

type Server struct {
	Addr string
}

func (s *Server) ReceiveHeartBeat(sequence string, response *string) error {
	t := time.Now()
	*response = sequence + " heart beat received at second " + strconv.Itoa(t.Second())
	return nil
}

func (s *Server) Hello(request string, response *string) error {
	*response = "hello" + request
	return nil
}

func main() {
	rpc.RegisterName("Server", new(Server))
	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}

	for {
		// accept một connection đến
		conn, err := l.Accept()
		// in ra lỗi nếu có
		if err != nil {
			log.Fatal("Accept error:", err)
		}

		// phục vụ client trên một goroutine khác
		// để giải phóng main thread tiếp tục vòng lặp
		go rpc.ServeConn(conn)
	}
}
