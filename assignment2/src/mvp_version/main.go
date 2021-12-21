package main

import (
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"
)

var marker = "Marker"
var snapShotChan = make(chan SnapShot)
var C_LWg = sync.WaitGroup{}
var debug = true

type Server struct {
	sync.RWMutex
	idx            int
	events         []string // protected by mutex
	seenMarker     bool     // protected by mutex
	markerReceived int      // protected by mutex
	outChs         []chan string
	inChs          []chan string
}

type SnapShot struct {
	serverIdx int
	events    []string
	time      int
}

func MakeChans(servers []*Server) {
	n := len(servers)
	for i1 := 1; i1 < n; i1++ {
		for i2 := i1 + 1; i2 < n; i2++ {
			s1 := servers[i1]
			s2 := servers[i2]
			ch12 := make(chan string, 1)
			ch21 := make(chan string, 1)
			s1.outChs[s2.idx] = ch12
			s1.inChs[s2.idx] = ch21
			s2.outChs[s1.idx] = ch21
			s2.inChs[s1.idx] = ch12
		}
	}

}

func (s *Server) Send(destIdx int, message string) error {
	if s.outChs[destIdx] == nil || destIdx >= len(s.outChs) {
		return errors.New("No channel to" + strconv.Itoa(destIdx))
	}

	s.outChs[destIdx] <- message
	if debug {
		fmt.Printf("Server %d sent %s on %v at %d\n", s.idx, message, s.outChs[destIdx], int(time.Now().UnixNano()))
	}
	return nil
}

func (s *Server) Listen(srcIdx int) error {
	if debug {
		fmt.Printf("Server %d listening to %d at %d\n", s.idx, srcIdx, int(time.Now().UnixNano()))
	}

loop:
	for message := range s.inChs[srcIdx] {
		if debug {
			fmt.Printf("Server %d received %s on %v at %d\n", s.idx, message, s.inChs[srcIdx], int(time.Now().UnixNano()))
		}

		if message == marker {
			s.Lock()
			{
				if s.seenMarker == false {
					s.seenMarker = true
					snapShotChan <- SnapShot{s.idx, s.events, int(time.Now().UnixMilli())}
					s.BroadCast()
				}

				// seen marker before
				s.markerReceived++
				if s.markerReceived == len(s.inChs)-2 { // finish C_L algorithm
					if debug {
						fmt.Printf("Server %d finished C_L algorithm at %d\n", s.idx, int(time.Now().UnixNano()))
					}
					C_LWg.Done()
				}
			}
			s.Unlock()

			break loop

		} else {
			s.Lock()
			s.events = append(s.events, message)
			s.Unlock()
		}
	}

	if debug {
		fmt.Printf("Server %d stop listening from server %d at %d\n", s.idx, srcIdx, int(time.Now().UnixNano()))
	}
	return nil
}

func (s *Server) BroadCast() {
	if debug {
		fmt.Printf("Server %d broadcasting at %d\n", s.idx, int(time.Now().UnixNano()))
	}

	for i := 1; i < len(s.outChs); i++ {
		if i != s.idx && s.outChs[i] != nil {
			s.outChs[i] <- marker
			if debug {
				fmt.Printf("Server %d sent %s on %v at %d\n", s.idx, marker, s.outChs[i], int(time.Now().UnixNano()))
			}
		}
	}
}

func (s *Server) InitListeningChans() {
	for i := 1; i < len(s.inChs); i++ {
		go s.Listen(i)
	}

}

func NewServer(idx, numChan int) Server {
	return Server{
		sync.RWMutex{},
		idx,
		make([]string, 0),
		false,
		0,
		make([]chan string, numChan+1),
		make([]chan string, numChan+1)}
}

func RecordSnaps() {
	for snap := range snapShotChan {
		if debug {
			fmt.Printf("Received %v from server %d at %d\n", snap.events, snap.serverIdx, snap.time)
		}
	}
}

func main() {
	runtime.GOMAXPROCS(2)
	n := 2
	C_LWg.Add(n)

	go RecordSnaps()

	servers := make([]*Server, 0)
	dumpS := NewServer(0, n)
	servers = append(servers, &dumpS) // 0-index is empty struct

	for i := 1; i <= n; i++ {
		s := NewServer(i, n)
		servers = append(servers, &s)
	}

	MakeChans(servers)
	s1 := servers[1]
	s2 := servers[2]
	//s3 := servers[3]

	s1.events = append(s1.events, "event X")
	time.AfterFunc(1*time.Second, func() {
		s1.Lock()
		s1.events = append(s1.events, "event Y")
		s1.Unlock()
	})

	s2.events = append(s2.events, "event W")
	time.AfterFunc(5*time.Millisecond, func() {
		s2.Lock()
		s2.events = append(s2.events, "event Q")
		s2.Unlock()
	})

	s1.InitListeningChans()
	s2.InitListeningChans()
	//s3.InitListeningChans()

	// record s1 states
	s1.Lock()
	snapShotChan <- SnapShot{s1.idx, s1.events, int(time.Now().UnixMilli())}
	s1.Unlock()

	s1.seenMarker = true
	s1.BroadCast()
	C_LWg.Wait()
}
