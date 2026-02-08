package raft

import (
	"sync"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// LogEntry represents a single entry in the log
type LogEntry struct {
	Command interface{} // Command for state machine
	Term    int         // Term when entry was received by leader
}

// Server state constants
const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

// Election timeout
const (
	HeartbeatTimeout = 250 * time.Millisecond
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers (updated on stable storage before responding to RPCs)
	currentTerm int        // Latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // CandidateId that received vote in current term (or -1 if none)
	log         []LogEntry // Log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers
	commitIndex int // Index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // Index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	applyCh     chan raftapi.ApplyMsg

	// Volatile state on leaders (reinitialized after election)
	nextIndex  []int // For each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // For each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// Additional state for tracking server role
	state int // Current state: Follower, Candidate, or Leader

	// Last time receive the heartbeat from leader
	lastHeartbeatTime time.Time
}

// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0, Command: nil}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	rf.lastHeartbeatTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
