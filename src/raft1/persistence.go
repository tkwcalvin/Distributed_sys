package raft

import (
	"bytes"
	"log"

	"6.5840/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot []byte) {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	if snapshot != nil {
		rf.persister.Save(raftstate, snapshot)
	} else {
		rf.persister.Save(raftstate, nil)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var persistedLog []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&persistedLog) != nil {
		// failed to decode persisted state
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = persistedLog
	rf.lastLogIndex = persistedLog[len(persistedLog)-1].Index
	log.Printf("DEBUG [readPersist] server %d restored term=%d votedFor=%d logLen=%d", rf.me, currentTerm, votedFor, rf.lastLogIndex+1)

}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}
