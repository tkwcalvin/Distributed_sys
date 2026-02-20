package raft

import (
	"log"
	"sort"
	"sync"
	"time"

	"6.5840/raftapi"
)

func (rf *Raft) replicateLog() {
	once := sync.Once{}
	stopCh := make(chan struct{})
	for {
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

		time.Sleep(100 * time.Millisecond)
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
	log.Printf("[leader sendAppendEntries] leader %d term %d <- server %d: ok=%v", rf.me, term, server, ok)
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
		rf.matchIndex[server] = max(newNextIndex-1, rf.matchIndex[server])
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
	// defer log.Printf("[Term %d] server %d receives entries from %d, reply: %v", rf.currentTerm, rf.me, args.LeaderId, reply)
	// defer log.Printf("[Term %d] server %d receives entries from %d, args: %v", rf.currentTerm, rf.me, args.LeaderId, args)

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

	for i, entry := range args.Entries {
		// if the log is out of range or the term is not the same, append the new entries
		newIndex := args.PrevLogIndex + i + 1
		if newIndex > len(rf.log)-1 || rf.log[newIndex].Term != entry.Term {
			rf.log = append(rf.log[:newIndex], args.Entries[i:]...)
			log.Printf("DEBUG [AE accept] follower %d <- leader %d term %d: appended %d entries, logLen=%d",
				rf.me, args.LeaderId, args.Term, len(args.Entries)-i, len(rf.log))
			rf.persist()
			break
		}

	}

	// commit the log if the leader commit index is greater than the follower commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		log.Printf("DEBUG [commitIdx] follower %d <- leader %d: commitIdx -> %d (logLen=%d)",
			rf.me, args.LeaderId, rf.commitIndex, len(rf.log))
	}

	rf.nextElectionTimeout = getNextElectionDeadline()
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
