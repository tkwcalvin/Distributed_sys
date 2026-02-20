package raft

import "log"

// becomeFollower transitions this server to follower state with the given term.
func (rf *Raft) becomeFollower(term int) {

	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.nextElectionTimeout = getNextElectionDeadline()
	log.Printf("[Term %d] server %d becomes Follower", rf.currentTerm, rf.me)
	rf.persist()
}

// becomeCandidate transitions this server to candidate state and starts a new term.
func (rf *Raft) becomeCandidate() {

	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.nextElectionTimeout = getNextElectionDeadline()
	log.Printf("[Term %d] server %d becomes Candidate", rf.currentTerm, rf.me)
	rf.persist()

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
	nextCopy := make([]int, len(rf.nextIndex))
	copy(nextCopy, rf.nextIndex)
	log.Printf("DEBUG [becomeLeader] server %d term %d: nextIdx=%v commitIdx=%d lastLogIdx=%d",
		rf.me, rf.currentTerm, nextCopy, rf.commitIndex, len(rf.log)-1)
	rf.persist()
	go rf.replicateLog()
}
