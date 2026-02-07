package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"log"
	//	"6.5840/labgob"
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
	ElectionTimeout = 500 * time.Millisecond
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

	// Volatile state on leaders (reinitialized after election)
	nextIndex  []int // For each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // For each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// Additional state for tracking server role
	state int // Current state: Follower, Candidate, or Leader

	// Last time receive the heartbeat from leader
	lastHeartbeatTime time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // Candidate's term
	CandidateId  int // Candidate requesting vote
	LastLogIndex int // Index of candidate's last log entry (§5.4)
	LastLogTerm  int // Term of candidate's last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // CurrentTerm, for candidate to update itself
	VoteGranted bool // True means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if rf.killed() == false {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		if args.Term < rf.currentTerm {
			log.Printf("[Term %d] server %d does not vote for %d", rf.currentTerm, rf.me, args.CandidateId)
			return
		}
		if args.Term > rf.currentTerm {
			rf.becomeFollower(args.Term)
			reply.Term = args.Term
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			log.Printf("[Term %d] server %d votes for %d", rf.currentTerm, rf.me, args.CandidateId)
			return
		}
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			rf.lastHeartbeatTime = time.Now()
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			log.Printf("[Term %d] server %d votes for %d", rf.currentTerm, rf.me, args.CandidateId)
			return
		}

	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderId     int        // So follower can redirect clients
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // Leader's commitIndex
}

// AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	Term    int  // CurrentTerm, for leader to update itself
	Success bool // True if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	if rf.killed() == false {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		reply.Term = rf.currentTerm
		reply.Success = false
		if args.Entries == nil {
			if args.Term < rf.currentTerm {
				// Outdated term, reply false and keep the current term
				log.Printf("[Term %d] server %d receives heartbeat from %d, reply: %v", rf.currentTerm, rf.me, args.LeaderId, reply)
				return
			}
			if args.Term > rf.currentTerm {
				// See updateTerm, become Follower for the new term
				rf.becomeFollower(args.Term)
				reply.Success = true
				reply.Term = rf.currentTerm
				return
			}
			// Heartbeat, reply true and keep the current term
			reply.Success = true
			rf.lastHeartbeatTime = time.Now()

			log.Printf("[Term %d] server %d receives heartbeat from %d, reply: %v", rf.currentTerm, rf.me, args.LeaderId, reply)
			return
		}
	}
}

// example code to send a AppendEntries RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// State machine helpers for server role transitions.
// These helpers assume rf.mu is already held by the caller.

// becomeFollower transitions this server to follower state with the given term.
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.lastHeartbeatTime = time.Now()
	log.Printf("[Term %d] server %d becomes Follower", rf.currentTerm, rf.me)
}

// becomeCandidate transitions this server to candidate state and starts a new term.
func (rf *Raft) becomeCandidate() {

	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeatTime = time.Now()
	log.Printf("[Term %d] server %d becomes Candidate", rf.currentTerm, rf.me)

}

// becomeLeader transitions this server to leader state and initializes leader volatile state.
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	next := len(rf.log)
	for i := range rf.peers {
		rf.nextIndex[i] = next
		rf.matchIndex[i] = 0
	}
	log.Printf("[Term %d] server %d becomes Leader", rf.currentTerm, rf.me)
}

// StateSnapshot contains the state snapshot needed for RPC operations
type StateSnapshot struct {
	CurrentTerm  int
	Me           int
	CommitIndex  int
	LastLogIndex int
	LastLogTerm  int
	PrevLogTerm  int
}

// getStateSnapshot creates a snapshot of the current state.
// Caller must hold rf.mu lock before calling this function.
func (rf *Raft) getStateSnapshot() StateSnapshot {
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	prevLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
		prevLogTerm = lastLogTerm
	}

	return StateSnapshot{
		CurrentTerm:  rf.currentTerm,
		Me:           rf.me,
		CommitIndex:  rf.commitIndex,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		PrevLogTerm:  prevLogTerm,
	}
}

// sendHeartbeats is called when this server is leader; it sends
// periodic AppendEntries (heartbeats) to all other peers.
func (rf *Raft) sendHeartbeats(snapshot StateSnapshot) {

	peerCount := len(rf.peers) - 1 // Exclude self
	replyCh := make(chan *AppendEntriesReply, peerCount)
	stopCh := make(chan struct{}) // Channel to signal stop processing replies
	var once sync.Once

	// Start goroutines to send RPCs to all peers asynchronously
	for peer := range rf.peers {
		if peer == snapshot.Me {
			continue
		}

		go func(server int) {
			// Check if we should stop before sending
			select {
			case <-stopCh:
				return
			default:
			}

			// For heartbeat, use last log index as prevLogIndex
			args := &AppendEntriesArgs{
				Term:         snapshot.CurrentTerm,
				LeaderId:     snapshot.Me,
				PrevLogIndex: snapshot.LastLogIndex,
				PrevLogTerm:  snapshot.PrevLogTerm,
				LeaderCommit: snapshot.CommitIndex,
			}

			reply := &AppendEntriesReply{}
			log.Printf("[Term %d] leader %d sends AppendEntries to %d", snapshot.CurrentTerm, snapshot.Me, server)
			ok := rf.sendAppendEntries(server, args, reply)

			// Check if we should stop processing before sending reply
			select {
			case <-stopCh:
				// Stop channel closed, don't send reply
				return
			default:
				if ok {
					replyCh <- reply
				} else {
					replyCh <- nil
				}
			}
		}(peer)
	}

	// Process replies asynchronously
	go func() {
		for {
			select {
			case reply := <-replyCh:
				if reply == nil {
					continue
				}

				// If reply has higher term, stop processing and convert to follower
				if reply.Term > snapshot.CurrentTerm {
					// Close stop channel to prevent other goroutines from sending more replies
					once.Do(func() {
						close(stopCh)
					})

					// Acquire lock and convert to follower
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term)
					}
					rf.mu.Unlock()

					// Drain remaining replies and exit
					for {
						select {
						case <-replyCh:
							// Drain
						default:
							return
						}
					}
				}

			case <-stopCh:
				// Stop processing, exit
				return
			}
		}
	}()
}

// startElection is called by a follower/candidate when the election
// timeout elapses and it should try to become leader.
func (rf *Raft) startElection(snapshot StateSnapshot) {
	peerCount := len(rf.peers) - 1 // Exclude self
	majority := (len(rf.peers))/2 + 1
	voteCh := make(chan *RequestVoteReply, peerCount)
	stopCh := make(chan struct{}) // Channel to signal stop processing replies
	var once sync.Once
	votesReceived := 1 // Count self vote

	// Start goroutines to send RequestVote to all peers asynchronously
	for peer := range rf.peers {
		if peer == snapshot.Me {
			continue
		}

		go func(server int) {
			// Check if we should stop before sending
			select {
			case <-stopCh:
				return
			default:
			}

			args := &RequestVoteArgs{
				Term:         snapshot.CurrentTerm,
				CandidateId:  snapshot.Me,
				LastLogIndex: snapshot.LastLogIndex,
				LastLogTerm:  snapshot.LastLogTerm,
			}
			reply := &RequestVoteReply{}

			ok := rf.sendRequestVote(server, args, reply)

			// Check if we should stop processing before sending reply
			select {
			case <-stopCh:
				// Stop channel closed, don't send reply
				return
			default:
				if ok {
					log.Printf("[Term %d] server %d sent RequestVote to %d, get reply: %v", snapshot.CurrentTerm, snapshot.Me, server, reply)
					voteCh <- reply
				} else {
					log.Printf("[Term %d] server %d sent RequestVote to %d, failed", snapshot.CurrentTerm, snapshot.Me, server)
					voteCh <- nil
				}
			}
		}(peer)
	}

	// Process replies asynchronously with countdown timer
	electionTimeout := 500 * time.Millisecond
	timer := time.NewTimer(electionTimeout)
	defer timer.Stop()

	go func() {
		for {
			select {
			case reply := <-voteCh:
				if reply == nil {
					continue
				}

				// If reply has higher term, stop processing and convert to follower
				if reply.Term > snapshot.CurrentTerm {
					// Close stop channel to prevent other goroutines from sending more replies
					once.Do(func() {
						close(stopCh)
						timer.Stop()
					})

					// Acquire lock and convert to follower
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term)
					}
					rf.mu.Unlock()

					// Drain remaining replies and exit
					for {
						select {
						case <-voteCh:
							// Drain
						default:
							return
						}
					}
				}

				// Only count votes for current term
				rf.mu.Lock()
				if reply.VoteGranted && rf.state == Candidate && rf.currentTerm == snapshot.CurrentTerm {
					votesReceived++
					if votesReceived >= majority {
						// Become leader and initialize leader state
						rf.becomeLeader()
						log.Printf("Candidate %d becomes leader", snapshot.Me)
						rf.mu.Unlock()

						// Close stop channel and stop timer
						once.Do(func() {
							close(stopCh)
							timer.Stop()
						})

						// Drain remaining replies and exit
						for {
							select {
							case <-voteCh:
								// Drain
							default:
								return
							}
						}
					}
				}
				rf.mu.Unlock()

			case <-timer.C:
				// Countdown expired: election failed, convert back to follower
				once.Do(func() {
					close(stopCh)
				})

				rf.mu.Lock()
				if rf.state == Candidate && rf.currentTerm == snapshot.CurrentTerm {
					// Check one more time if we have enough votes
					if votesReceived >= majority {
						rf.becomeLeader()
						log.Printf("Candidate %d becomes leader (after timeout)", snapshot.Me)
					} else {
						// Election failed, convert back to follower
						rf.state = Follower
						rf.lastHeartbeatTime = time.Now()
						log.Printf("Candidate %d does not become leader (timeout), reverting to follower", snapshot.Me)
					}
				}
				rf.mu.Unlock()

				// Drain remaining replies and exit
				for {
					select {
					case <-voteCh:
						// Drain
					default:
						return
					}
				}

			case <-stopCh:
				// Stop processing, exit
				return
			}
		}
	}()
}

// ticker runs in a loop and either sends heartbeats (if leader)
// or checks for election timeouts (if not leader).
func (rf *Raft) ticker() {
	for rf.killed() == false {

		rf.mu.Lock()
		state := rf.state
		lastHeartbeat := rf.lastHeartbeatTime

		if state == Leader {
			// Create snapshot while holding lock
			snapshot := rf.getStateSnapshot()
			rf.mu.Unlock()

			rf.sendHeartbeats(snapshot) // sendHeartbeats uses snapshot, doesn't need lock
			// Heartbeat interval (e.g., 100ms)
			time.Sleep(100 * time.Millisecond)
		} else {
			// Follower/Candidate checks for election timeout
			if time.Now().After(lastHeartbeat.Add(ElectionTimeout)) {
				rf.becomeCandidate()
				rf.mu.Unlock()
				// Get fresh snapshot after becoming candidate (term has changed)
				snapshot := rf.getStateSnapshot()
				rf.mu.Unlock()

				rf.startElection(snapshot) // startElection uses snapshot, doesn't need lock
			} else {
				rf.mu.Unlock()
			}

			// pause for a random amount of time between 50 and 350
			// milliseconds.
			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
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
	rf.log = []LogEntry{}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.lastHeartbeatTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
