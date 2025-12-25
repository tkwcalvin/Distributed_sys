package kvsrv

import (
	"log"
	"net"
	"net/http"
	"os"
	"sync"

	netrpc "net/rpc"

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
	// Your code here.
	// start a thread that listens for RPCs from worker.go
	netrpc.RegisterName("KVServer", kv)
	netrpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := rpc.SocketName()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		if err := http.Serve(l, nil); err != nil {
			log.Fatal("http serve error:", err)
		}
	}()

	kv.db = make(map[string]*Entry)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, ok := kv.db[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		reply.Version = 0
		return nil
	}
	reply.Value = entry.Value
	reply.Version = entry.Version
	reply.Err = rpc.OK
	return nil
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, ok := kv.db[args.Key]
	if !ok {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return nil
		}
		reply.Err = rpc.OK
		kv.db[args.Key] = &Entry{Value: args.Value, Version: args.Version + 1}
		return nil
	}
	if args.Version != entry.Version {
		reply.Err = rpc.ErrVersion
		return nil
	}
	reply.Err = rpc.OK
	kv.db[args.Key] = &Entry{Value: args.Value, Version: args.Version + 1}
	return nil
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
