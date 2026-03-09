package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu sync.Mutex
	db map[string]*Entry
}

type Entry struct {
	Value   string
	Version rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
// DoOp runs the requested op. We use value types so persisted log decodes to the same type.
func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case rpc.GetArgs:
		return kv.doGet(args)
	case rpc.PutArgs:
		return kv.doPut(args)
	default:
		log.Fatalf("DoOp should execute only put and get, and not %T", req)
		return nil
	}
}

func (kv *KVServer) doGet(args rpc.GetArgs) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &rpc.GetReply{}
	entry, ok := kv.db[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		reply.Version = 0
		return reply
	}
	reply.Value = entry.Value
	reply.Version = entry.Version
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) doPut(args rpc.PutArgs) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &rpc.PutReply{}
	entry, ok := kv.db[args.Key]
	if !ok {
		if args.Version != 0 {
			// KvModel only accepts OK, ErrVersion, ErrMaybe. Key absent => state version 0; request version != 0 => mismatch.
			reply.Err = rpc.ErrVersion
			return reply
		}
		reply.Err = rpc.OK
		kv.db[args.Key] = &Entry{Value: args.Value, Version: args.Version + 1}
		log.Printf("[KV apply] Put key=%s newKey ver=0 -> OK (newVer=1)", args.Key)
		return reply
	}
	if args.Version != entry.Version {
		reply.Err = rpc.ErrVersion
		log.Printf("[KV apply] Put key=%s reqVer=%d curVer=%d -> ErrVersion", args.Key, args.Version, entry.Version)
		return reply
	}
	reply.Err = rpc.OK
	kv.db[args.Key] = &Entry{Value: args.Value, Version: args.Version + 1}
	log.Printf("[KV apply] Put key=%s ver=%d -> OK (newVer=%d)", args.Key, args.Version, args.Version+1)
	return reply
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Do not hold kv.mu across Submit: when the entry is applied, the RSM reader
	// calls DoOp which needs kv.mu; holding it here would deadlock.
	log.Printf("[KV %d] Get key=%s calling Submit", kv.me, args.Key)
	err, res := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		log.Printf("[KV %d] Get key=%s -> ErrWrongLeader", kv.me, args.Key)
		return
	}
	*reply = *(res.(*rpc.GetReply))
	log.Printf("[KV %d] Get key=%s -> OK", kv.me, args.Key)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Do not hold kv.mu across Submit: when the entry is applied, the RSM reader
	// calls DoOp which needs kv.mu; holding it here would deadlock.
	log.Printf("[KV %d] Put key=%s calling Submit", kv.me, args.Key)
	err, res := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		log.Printf("[KV %d] Put key=%s -> ErrWrongLeader", kv.me, args.Key)
		return
	}
	*reply = *(res.(*rpc.PutReply))
	log.Printf("[KV %d] Put key=%s -> OK", kv.me, args.Key)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})
	labgob.Register(rpc.Err(""))

	kv := &KVServer{me: me, db: make(map[string]*Entry)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
