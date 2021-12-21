package main

import (
	"fmt"
	"time"
)

var ElecTimeOut = 1 * time.Second

const HBReq = "HBRequest"
const HBRep = "HBResponse"
const VReq = "VoteRequest"
const VRep = "VoteResponse"

type Req struct {
	Type string
	Id   int
	Term int
}

type Rep struct {
	Type   string
	Id     int
	Term   int
	Result string
}

type Server struct {
	id            int
	term          int
	voteFor       int
	inChans       []chan Req
	outChans      []chan Rep
	resetElecChan chan struct{}
}

func NewServer(id, term, n int) Server {
	return Server{id, term, -1, make([]chan Req, n), make([]chan Rep, n), make(chan struct{})}
}

func (s *Server) StartElecTimer() {
	select {
	case <-s.resetElecChan: // received RPC calls
		s.StartElecTimer() // reset election timer

	case <-time.After(ElecTimeOut):
		s.StartElection()
	}
}

func (s *Server) Listen(nodeId int) {
	for x := range s.inChans[nodeId] {
		switch x.Type {
		case HBReq:
			s.resetElecChan <- struct{}{}
			s.outChans[nodeId] <- Rep{HBRep, s.id, s.term, "NA"}
		case VReq:
			s.resetElecChan <- struct{}{}
			if x.Term > s.term {
				s.term = x.Term
				s.voteFor = s.id
				s.outChans[nodeId] <- Rep{VRep, s.id, s.term, "Granted"}
			}

		}

	}
}

func (s *Server) StartElection() {
	fmt.Printf("%d start election\n", s.id)
}

func SendAndRcvHB(s1, s2 *Server, interval int) {
	id1 := s1.id
	id2 := s2.id

	rcvChan := s1.outChans[id2]
	sendChan := s2.inChans[id1]

	ticker := time.NewTicker(time.Duration(interval * int(time.Second)))

	for {
		<-ticker.C
		fmt.Printf("%d send HB to %d\n", id1, id2)
		sendChan <- Req{Type: HBReq, Id: id1, Term: s1.term}
		<-rcvChan
		fmt.Printf("%d received HB from %d\n", id1, id2)
	}
}

func main() {
	s0 := NewServer(0, 0, 2)
	s1 := NewServer(1, 0, 2)

	s0.StartElecTimer()
	s1.StartElecTimer()
	go s0.Listen(1)
	go s1.Listen(0)

	SendAndRcvHB(&s0, &s1, 1)
}
