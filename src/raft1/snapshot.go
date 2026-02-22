package raft

import (
	"6.5840/raftapi"
)

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.lastLogIndex {
		return
	}
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[rf.getIndexAfterCompaction(index)].Term
	rf.log = rf.log[rf.getIndexAfterCompaction(index):]
	rf.persist(snapshot)
	// print the snapshot data length
	//log.Printf("DEBUG [Snapshot] server %d term %d snapshot data length %d", rf.me, rf.currentTerm, len(rf.persister.ReadSnapshot()))

}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Your code here (3D).
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	rf.persist(args.Data)
	if args.LastIncludedIndex <= rf.lastLogIndex &&
		rf.log[rf.getIndexAfterCompaction(args.LastIncludedIndex)].Term == args.LastIncludedTerm {
		rf.log = rf.log[rf.getIndexAfterCompaction(args.LastIncludedIndex):]
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.persist(args.Data)
		reply.Term = rf.currentTerm

	} else {
		rf.log = []LogEntry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm, Command: nil}}
		rf.lastLogIndex = args.LastIncludedIndex
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.persist(args.Data)
		reply.Term = rf.currentTerm

	}

	rf.lastApplied = args.LastIncludedIndex

	rf.mu.Unlock()
	rf.applyCh <- raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	// print the snapshot data length
	//log.Printf("DEBUG [InstallSnapshot] server %d term %d snapshot data length %d", rf.me, rf.currentTerm, len(args.Data))
}
