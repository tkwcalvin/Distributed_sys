package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

type Entry struct {
	Value   string
	Version rpc.Tversion
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	db map[string]*Entry
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.db = make(map[string]*Entry)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, ok := kv.db[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		reply.Version = 0
		return
	}
	reply.Value = entry.Value
	reply.Version = entry.Version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, ok := kv.db[args.Key]
	if !ok {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		reply.Err = rpc.OK
		kv.db[args.Key] = &Entry{Value: args.Value, Version: args.Version + 1}
		return
	}
	if args.Version != entry.Version {
		reply.Err = rpc.ErrVersion
		return
	}
	reply.Err = rpc.OK
	kv.db[args.Key] = &Entry{Value: args.Value, Version: args.Version + 1}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
