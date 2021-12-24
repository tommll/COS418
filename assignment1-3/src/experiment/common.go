package common

import (
	"time"
)

type HeartBeatMsg struct {
	SeqNum int
}

type AckMsg struct {
	Time time.Time
}

func A() {
	s := server.main.Server{}
}
