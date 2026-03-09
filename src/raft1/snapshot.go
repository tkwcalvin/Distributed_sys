package raft

import (
	"6.5840/raftapi"
)

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
		// follower has the log of the last included index and term
		// remain the following entries in the log
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

	if rf.killed() {
		rf.mu.Unlock()
		return
	}
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
