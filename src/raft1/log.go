package raft

import (
	"log"
	"sort"
	"sync"
	"time"

	"6.5840/raftapi"
)

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

func (rf *Raft) replicateLog() {
	once := sync.Once{}
	stopCh := make(chan struct{})
	for !rf.killed() {
		select {
		case <-stopCh:
			return
		default:
		}

		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go rf.NotifyLogReplication(server, &once, stopCh)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) NotifyLogReplication(server int, once *sync.Once, stopCh chan struct{}) {

	rf.mu.Lock()
	if rf.state != Leader {
		once.Do(func() {
			close(stopCh)
		})
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	nextIndex := rf.nextIndex[server]
	newNextIndex := len(rf.log)
	entries := rf.log[nextIndex:]
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: nextIndex - 1,
		PrevLogTerm:  rf.log[nextIndex-1].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	// 发送AppendEntries

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	//log.Printf("[leader sendAppendEntries] leader %d term %d <- server %d: ok=%v", rf.me, term, server, ok)
	if !ok {
		return
	}

	if reply.Term > term {
		log.Printf("DEBUG [leader stepdown] leader %d term %d <- server %d reply.term=%d", rf.me, term, server, reply.Term)
		once.Do(func() {
			close(stopCh)
			rf.mu.Lock()
			if rf.state == Leader && rf.currentTerm == term {
				rf.becomeFollower(reply.Term)
			}
			rf.mu.Unlock()
		})
		return
	}

	if reply.Success {
		// 第一点：用发送时的 nextIndex + 本次发送的条目数更新，避免 leader 在 RPC 期间 append 导致漏复制
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term {
			once.Do(func() {
				close(stopCh)
			})
			rf.mu.Unlock()
			return
		}
		rf.nextIndex[server] = newNextIndex
		rf.matchIndex[server] = newNextIndex - 1
		rf.mu.Unlock()

		return
	}

	// if the follower does not contain the entry matching prevLogIndex and prevLogTerm
	// decrement the nextIndex and try again (but never below 1)

	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != term {
		once.Do(func() {
			close(stopCh)
		})
		rf.mu.Unlock()
		return
	}
	if reply.XTerm == 0 {
		rf.nextIndex[server] = reply.XLen
		rf.mu.Unlock()
		return
	}
	lastXTermIndex := -1
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == reply.XTerm {
			lastXTermIndex = i
			break
		}
	}
	// leader has the conflicting term
	if lastXTermIndex >= 0 {
		rf.nextIndex[server] = lastXTermIndex + 1
	} else {
		// leader does not has the conflicting term
		rf.nextIndex[server] = reply.XIndex
	}
	nextCopy := make([]int, len(rf.nextIndex))
	copy(nextCopy, rf.nextIndex)
	commitIdx := rf.commitIndex
	rf.mu.Unlock()
	log.Printf("DEBUG [nextIdx] leader %d term %d after conflict <- server %d (XTerm=%d XIdx=%d): nextIdx=%v commitIdx=%d",
		rf.me, term, server, reply.XTerm, reply.XIndex, nextCopy, commitIdx)

}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.state == Leader {
			majority := (len(rf.peers))/2 + 1
			matchCopy := make([]int, len(rf.matchIndex))
			copy(matchCopy, rf.matchIndex)
			// 打印match index
			log.Printf("DEBUG [applier] leader %d term %d matchIndex=%v", rf.me, rf.currentTerm, matchCopy)
			sort.Ints(matchCopy)
			n := matchCopy[len(matchCopy)-majority]
			// 规则1：仅当前任期日志可直接提交

			// 打印log任期和当前任期
			log.Printf("DEBUG [applier] leader %d term %d log[%d].Term=%d currentTerm=%d", rf.me, rf.currentTerm, n, rf.log[n].Term, rf.currentTerm)
			if rf.log[n].Term == rf.currentTerm {
				rf.commitIndex = n
			}

		}
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			entry := rf.log[rf.lastApplied]
			idx := rf.lastApplied
			rf.mu.Unlock()
			// 不要在持锁时向 applyCh 发送，否则 channel 满会阻塞并导致死锁（Start/ticker 等拿不到锁）
			rf.applyCh <- raftapi.ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: idx}
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
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

	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with that term (if any)
	XLen   int // log length
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// Outdated term, reply false and keep the current term
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm || (rf.state == Candidate && args.LeaderId != rf.me) {
		// See updateTerm, become Follower for the new term or
		// there is a new leader send append entries
		rf.becomeFollower(args.Term)
	}

	// return false if prevLogIndex is out of range or
	// prevLogTerm is not the same as the term of the log entry at prevLogIndex

	if !rf.checkIfUpdatedLog(args, reply) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// append new entries to the log
	logInsertIndex := args.PrevLogIndex + 1
	newEntriesIndex := 0

	for {
		if logInsertIndex > len(rf.log)-1 || newEntriesIndex > len(args.Entries)-1 {
			break
		}
		if rf.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
			break
		}
		logInsertIndex++
		newEntriesIndex++
	}

	if newEntriesIndex < len(args.Entries) {
		rf.log = append(rf.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
		log.Printf("DEBUG [AE accept] follower %d <- leader %d term %d: appended %d entries, logLen=%d",
			rf.me, args.LeaderId, args.Term, len(args.Entries)-newEntriesIndex, len(rf.log))
		rf.persist()
	}

	// commit the log if the leader commit index is greater than the follower commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		log.Printf("DEBUG [commitIdx] follower %d <- leader %d: commitIdx -> %d (logLen=%d)",
			rf.me, args.LeaderId, rf.commitIndex, len(rf.log))
	}

	rf.nextElectionTimeout = getNextElectionDeadline()
	//log.Printf("DEBUG [AE handler] follower %d <- leader %d term %d: reset timeout",
	//	rf.me, args.LeaderId, args.Term)
	reply.Term = rf.currentTerm
	reply.Success = true

}

func (rf *Raft) checkIfUpdatedLog(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if args.PrevLogIndex > len(rf.log)-1 {
		reply.XTerm = 0
		reply.XIndex = 0
		reply.XLen = len(rf.log)
		log.Printf("DEBUG [AE reject] follower %d <- leader %d: prevIdx=%d > logLen-1=%d (log too short)",
			rf.me, args.LeaderId, args.PrevLogIndex, len(rf.log)-1)
		return false
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		reply.XIndex = args.PrevLogIndex
		for reply.XIndex > 0 && rf.log[reply.XIndex-1].Term == reply.XTerm {
			reply.XIndex--
		}
		reply.XLen = len(rf.log)
		log.Printf("DEBUG [AE reject] follower %d <- leader %d: term mismatch at prevIdx=%d (have %d want %d), XTerm=%d XIdx=%d",
			rf.me, args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm, reply.XTerm, reply.XIndex)
		return false
	}
	return true

}
