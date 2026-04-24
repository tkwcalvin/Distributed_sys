package shardgrp

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu           sync.Mutex
	db           map[string]*Entry
	num          shardcfg.Tnum
	ownedShards  map[shardcfg.Tshid]bool // shards this group owns; nil = not yet set (e.g. part A)
	frozenShards map[shardcfg.Tshid]bool // shards that have been frozen (reject Get/Put)
}

type Entry struct {
	Value   string
	Version rpc.Tversion
}

func truncValForLog(s string) string {
	const max = 60
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

func (kv *KVServer) doGet(args rpc.GetArgs) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &rpc.GetReply{}
	shard := shardcfg.Key2Shard(args.Key)
	// Don't serve shards we don't own (e.g. new group before InstallShard applied).
	// Returning ErrWrongGroup lets the client retry; ErrNoKey would violate linearizability.
	if !kv.ownedShards[shard] {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
	// If shard is frozen (we're migrating), don't read: snapshot was already sent;
	// reading here could return stale value to client after migration completes.
	if kv.frozenShards[shard] {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
	entry, ok := kv.db[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		reply.Version = 0
		log.Printf("[LIN] KV gid=%d me=%d doGet key=%s -> NoKey", kv.gid, kv.me, args.Key)
		return reply
	}
	reply.Value = entry.Value
	reply.Version = entry.Version
	reply.Err = rpc.OK
	log.Printf("[LIN] KV gid=%d me=%d doGet key=%s -> value=%q ver=%d", kv.gid, kv.me, args.Key, truncValForLog(entry.Value), entry.Version)
	return reply
}

func (kv *KVServer) doPut(args rpc.PutArgs) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &rpc.PutReply{}
	shard := shardcfg.Key2Shard(args.Key)
	// Don't serve shards we don't own (e.g. new group before InstallShard applied).
	if !kv.ownedShards[shard] {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
	// If shard is frozen, don't write: snapshot was already sent; applying this put
	// would make our db newer than what the new owner received (linearization violation).
	if kv.frozenShards[shard] {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
	entry, ok := kv.db[args.Key]
	if !ok {
		if args.Version != 0 {
			// KvModel only accepts OK, ErrVersion, ErrMaybe. Key absent => state version 0; request version != 0 => mismatch.
			reply.Err = rpc.ErrVersion
			return reply
		}
		reply.Err = rpc.OK
		kv.db[args.Key] = &Entry{Value: args.Value, Version: args.Version + 1}
		log.Printf("[LIN] KV gid=%d me=%d doPut key=%s newKey value=%q -> OK (newVer=1)", kv.gid, kv.me, args.Key, truncValForLog(args.Value))
		return reply
	}
	if args.Version != entry.Version {
		reply.Err = rpc.ErrVersion
		log.Printf("[LIN] KV gid=%d me=%d doPut key=%s reqVer=%d curVer=%d -> ErrVersion", kv.gid, kv.me, args.Key, args.Version, entry.Version)
		return reply
	}
	reply.Err = rpc.OK
	kv.db[args.Key] = &Entry{Value: args.Value, Version: args.Version + 1}
	log.Printf("[LIN] KV gid=%d me=%d doPut key=%s value=%q ver=%d -> OK (newVer=%d)", kv.gid, kv.me, args.Key, truncValForLog(args.Value), args.Version, args.Version+1)
	return reply
}

func (kv *KVServer) doFreezeShard(args *shardrpc.FreezeShardArgs) *shardrpc.FreezeShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply := &shardrpc.FreezeShardReply{}
	// Ignore stale freeze for an older config (e.g. duplicate or reordered RPC).
	// Accept when args.Num == kv.num: same config, we may be freezing another shard (first FreezeShard set kv.num = newNum).
	if args.Num < kv.num {
		reply.Err = rpc.ErrWrongGroup
		reply.Num = kv.num
		return reply
	}
	if !kv.ownedShards[args.Shard] {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	kv.frozenShards[args.Shard] = true
	shardDB := make(map[string]*Entry)
	for k, v := range kv.db {
		if shardcfg.Key2Shard(k) == args.Shard {
			shardDB[k] = v
		}
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(shardDB)
	reply.State = w.Bytes()

	kv.num = args.Num

	reply.Num = kv.num
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) doInstallShard(args *shardrpc.InstallShardArgs) *shardrpc.InstallShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &shardrpc.InstallShardReply{}
	// Reject stale install: don't overwrite state with an older migration (avoids linearization violation).
	// Accept when args.Num == kv.num: same config, we may be installing another shard (first InstallShard set kv.num = newNum).
	if args.Num < kv.num {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
	// We are the new owner for this shard (ShardCtrler only sends InstallShard to the new group).
	// Accept the install and record ownership.

	r := bytes.NewBuffer(args.State)
	d := labgob.NewDecoder(r)
	var shardDB map[string]*Entry
	if d.Decode(&shardDB) != nil {
		log.Fatalf("kv %d: couldn't decode snapshot db", kv.me)
	}
	for k, v := range shardDB {
		kv.db[k] = v
	}
	kv.ownedShards[args.Shard] = true
	kv.num = args.Num
	// When we receive a shard (e.g. after leave), we become the owner; clear frozen
	// in case this shard was previously sent away and DeleteShard hadn’t been applied yet.

	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) doDeleteShard(args *shardrpc.DeleteShardArgs) *shardrpc.DeleteShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &shardrpc.DeleteShardReply{}
	// Idempotent: if we no longer have this shard (already deleted, e.g. duplicate RPC in unreliable net), return OK.
	if !kv.ownedShards[args.Shard] {
		reply.Err = rpc.OK
		return reply
	}
	// Accept when shard is frozen: this delete is the one matching our FreezeShard (even if
	// kv.num advanced due to another shard's migration in unreliable network).
	// Reject only when shard is not frozen and args.Num < kv.num (stale duplicate delete).
	if !kv.frozenShards[args.Shard] && args.Num < kv.num {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	for k := range kv.db {
		if shardcfg.Key2Shard(k) == args.Shard {
			delete(kv.db, k)
		}
	}

	delete(kv.frozenShards, args.Shard)
	delete(kv.ownedShards, args.Shard)

	if args.Num > kv.num {
		kv.num = args.Num
	}

	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case rpc.GetArgs:
		return kv.doGet(args)
	case rpc.PutArgs:
		return kv.doPut(args)
	case shardrpc.FreezeShardArgs:
		return kv.doFreezeShard(&args)
	case shardrpc.InstallShardArgs:
		return kv.doInstallShard(&args)
	case shardrpc.DeleteShardArgs:
		return kv.doDeleteShard(&args)
	default:
		log.Fatalf("DoOp should execute only put, get, freeze, install, delete; got %T", req)
		return nil
	}
}

// Snapshot returns a copy of the DB; caller must hold the lock (Lock before, Unlock after).
func (kv *KVServer) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var db map[string]*Entry
	if d.Decode(&db) != nil {
		log.Fatalf("kv %d: couldn't decode snapshot db", kv.me)
	}
	if db == nil {
		db = make(map[string]*Entry)
	}
	kv.mu.Lock()
	kv.db = db
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	kv.mu.Lock()
	if kv.frozenShards[shardcfg.Key2Shard(args.Key)] {
		kv.mu.Unlock()
		reply.Err = rpc.ErrWrongGroup
		log.Printf("[KV %d] Get key=%s -> ErrWrongGroup (shard frozen)", kv.me, args.Key)
		return
	}
	kv.mu.Unlock()
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
	// Your code here
	kv.mu.Lock()
	if kv.frozenShards[shardcfg.Key2Shard(args.Key)] {
		kv.mu.Unlock()
		reply.Err = rpc.ErrWrongGroup
		log.Printf("[KV %d] Put key=%s -> ErrWrongGroup (shard frozen)", kv.me, args.Key)
		return
	}
	kv.mu.Unlock()
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

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
// Replicated via Raft so all replicas freeze and return consistent state.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, res := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		return
	}
	*reply = *(res.(*shardrpc.FreezeShardReply))
}

// Install the supplied state for the specified shard.
// Replicated via Raft so all replicas have the shard data.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, res := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		return
	}
	*reply = *(res.(*shardrpc.InstallShardReply))
}

// Delete the specified shard.
// Replicated via Raft so all replicas drop the shard data.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, res := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		return
	}
	*reply = *(res.(*shardrpc.DeleteShardReply))
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

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me, db: make(map[string]*Entry)}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here
	kv.frozenShards = make(map[shardcfg.Tshid]bool)
	kv.ownedShards = make(map[shardcfg.Tshid]bool)
	// Initial group (Gid1) owns all shards until config changes.
	if gid == shardcfg.Gid1 {
		for s := shardcfg.Tshid(0); s < shardcfg.NShards; s++ {
			kv.ownedShards[s] = true
		}
	}

	return []tester.IService{kv, kv.rsm.Raft()}
}
