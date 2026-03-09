// Package rsm implements a replicated state machine on top of Raft. Callers use
// Submit(req) to run a command through Raft; when the log entry is committed and
// applied, the result of DoOp(req) is returned from Submit.
//
// Result delivery uses a pending map instead of a single shared result channel:
// each Submit registers a dedicated channel for its log index in pending[index];
// the reader, when it applies an entry at that index, looks up the waiter and
// sends the result only to that channel. This supports an unbounded number of
// concurrent Submits without any fixed buffer size.
package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

// pendingWaiter represents one Submit waiting for its log index to be applied.
// Each Submit gets its own ch; the reader sends the result to this ch only,
// so we don't need a shared channel with a fixed buffer.
// ch has buffer 1 so that if the Submit has already returned (e.g. timeout),
// the reader's send doesn't block.
type pendingWaiter struct {
	counter int       // op.Id for this request; disambiguates same index after restart
	ch      chan result
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// counter: unique id for each Op (op.Id), used to match apply results.
	// pending: log index -> waiter for that index. Reader looks up by index and
	//          sends only to that waiter's ch, so concurrency is unbounded.
	// doneCh: closed when reader exits (e.g. Raft Kill closed applyCh); all
	//         waiting Submits then return ErrWrongLeader.
	counter int
	pending map[int]*pendingWaiter
	doneCh  chan struct{}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.

type result struct {
	counter int
	index   int
	term    int
	opres   any
}

func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pending:      make(map[int]*pendingWaiter),
		doneCh:       make(chan struct{}),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit sends the command to Raft and blocks until that log entry is committed
// and applied, or this node is no longer leader / has been killed. Caller should
// retry on another server when ErrWrongLeader is returned.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// 1. Build Op with unique counter (op.Id), call Raft.Start.
	rsm.mu.Lock()
	counter := rsm.counter
	op := Op{Me: rsm.me, Id: rsm.counter, Req: req}
	rsm.counter++
	rsm.mu.Unlock()
	index, term, ok := rsm.rf.Start(op)
	if !ok {
		return rpc.ErrWrongLeader, nil
	}

	// 2. Create a channel for this request and register in pending[index].
	//    The reader will send the result only to this ch when it applies this index.
	//    Buffer 1: if we return early (timeout/doneCh), the reader's send won't block.
	ch := make(chan result, 1)
	rsm.mu.Lock()
	rsm.pending[index] = &pendingWaiter{counter: counter, ch: ch}
	rsm.mu.Unlock()

	// 3. On any return path, unregister so the reader won't send to a stale ch.
	defer func() {
		rsm.mu.Lock()
		delete(rsm.pending, index)
		rsm.mu.Unlock()
	}()

	// 4. Wait only on this request's ch; also watch doneCh and periodically
	//    check if we're still leader.
	for {
		select {
		case r := <-ch:
			if r.counter != counter {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, r.opres
		case <-rsm.doneCh:
			return rpc.ErrWrongLeader, nil
		case <-time.After(10 * time.Millisecond):
			currentTerm, isLeader := rsm.rf.GetState()
			if currentTerm > term || !isLeader {
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}

// reader consumes applyCh, runs DoOp for each committed entry, and delivers
// the result only to the Submit that is waiting for that index (via pending map).
func (rsm *RSM) reader() {
	defer close(rsm.doneCh)
	for msg := range rsm.applyCh {
		if !msg.CommandValid {
			continue
		}
		op := msg.Command.(Op)
		res := rsm.sm.DoOp(op.Req)
		r := result{counter: op.Id, index: msg.CommandIndex, opres: res}
		rsm.mu.Lock()
		pw := rsm.pending[msg.CommandIndex]
		if pw != nil && pw.counter == op.Id {
			delete(rsm.pending, msg.CommandIndex)
			ch := pw.ch
			rsm.mu.Unlock()
			ch <- r
		} else {
			rsm.mu.Unlock()
		}
	}
}
