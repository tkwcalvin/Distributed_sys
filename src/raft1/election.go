package raft

import (
	"log"
	"sync"
	"time"
)

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
	if rf.killed() == true {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		log.Printf("[Term %d] server %d does not vote for %d, term %d < currentTerm %d", rf.currentTerm, rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
			rf.votedFor = args.CandidateId
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			log.Printf("[Term %d] server %d votes for %d", rf.currentTerm, rf.me, args.CandidateId)
			return
		}
	}

	log.Printf("[Term %d] server %d does not vote for %d, votedFor %d, server's lastLogTerm %d, lastLogIndex %d, candidate's lastLogTerm %d, lastLogIndex %d", rf.currentTerm, rf.me, args.CandidateId, rf.votedFor, rf.log[len(rf.log)-1].Term, len(rf.log)-1, args.LastLogTerm, args.LastLogIndex)

}

// startElection is called by a follower/candidate when the election
// timeout elapses and it should try to become leader.
func (rf *Raft) startElection(term int) {

	rf.mu.Lock()
	if rf.state != Follower || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	rf.becomeCandidate()
	electionDeadline := rf.nextElectionTimeout
	peerCount := len(rf.peers)
	majority := peerCount/2 + 1
	voteCh := make(chan *RequestVoteReply, peerCount)
	stopCh := make(chan struct{})
	Once := sync.Once{}
	votesReceived := 1 // Count self vote
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	// Start goroutines to send RequestVote to all peers asynchronously
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(server int) {

			reply := &RequestVoteReply{}

			ok := rf.sendRequestVote(server, args, reply)

			// Check if we should stop processing before sending reply
			select {
			case <-stopCh:

				return
			default:
			}

			if ok {
				log.Printf("DEBUG server %d send RequestVote to %d term %d", rf.me, server, args.Term)
				voteCh <- reply
			} else {
				log.Printf("DEBUG server %d send RequestVote to %d term %d failed", rf.me, server, args.Term)
				voteCh <- nil
			}

		}(peer)
	}

	// check if election timeout
	go func() {

		for {
			select {
			case <-stopCh:
				Once.Do(func() {
					close(voteCh)
				})
				return
			default:
			}
			if time.Now().After(electionDeadline) {
				// log.Printf("[Term %d] server %d election timeout", snapshot.CurrentTerm, rf.me)
				Once.Do(func() {
					close(stopCh)
				})
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Process replies
	for {
		select {
		case <-stopCh:
			// Timeout Stop election, exit
			log.Printf("DEBUG server %d election TIMEOUT term %d", rf.me, args.Term)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state == Candidate && rf.currentTerm == args.Term {
				rf.becomeFollower(args.Term)
			}
			return

		case reply := <-voteCh:
			if reply == nil {
				continue
			}

			// If reply has higher term, stop processing and convert to follower
			if reply.Term > args.Term {
				// Acquire lock and convert to follower
				rf.mu.Lock()
				if rf.state == Candidate && rf.currentTerm == args.Term {
					rf.becomeFollower(reply.Term)
				}
				rf.mu.Unlock()
				return
			}

			// Only count votes for current term
			if !reply.VoteGranted {
				continue
			}

			votesReceived++
			if votesReceived >= majority {
				// Close stop channel and stop timer
				rf.mu.Lock()

				if rf.state == Candidate && rf.currentTerm == args.Term {
					rf.becomeLeader()
					log.Printf("DEBUG server %d WON election term %d", rf.me, rf.currentTerm)
				}
				rf.mu.Unlock()

				Once.Do(func() {
					close(stopCh)
				})
				return
			}

		}
	}

}

// ticker runs in a loop and either sends heartbeats (if leader)
// or checks for election timeouts (if not leader).
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		term := rf.currentTerm
		// Follower/Candidate checks for election timeout
		if rf.state == Follower && time.Now().After(rf.nextElectionTimeout) {
			rf.mu.Unlock()
			rf.startElection(term)
			continue
		}

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)

	}

}
