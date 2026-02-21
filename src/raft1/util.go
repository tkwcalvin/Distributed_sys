package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func getNextElectionDeadline() time.Time {
	ms := 150 + (rand.Int63() % 150)
	return time.Now().Add(time.Duration(ms) * time.Millisecond)
}
