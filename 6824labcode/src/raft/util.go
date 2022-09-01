package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type EntryQueue struct {
	queue []Entry
}

func (eq *EntryQueue) Push(e Entry) {
	eq.queue = append(eq.queue, e)
}

func (eq *EntryQueue) Pop() *Entry {
	if len(eq.queue) > 0 {
		e := eq.queue[0]
		eq.queue = eq.queue[1:]
		return &e
	}
	return nil
}
