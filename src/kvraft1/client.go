package kvraft

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	leader := 0
	lastStuckLog := time.Now()
	for {
		// find the leader server
		for {
			if time.Since(lastStuckLog) > 5*time.Second {
				log.Printf("[TEST] Clerk Get key=%s retrying >5s (no OK/ErrNoKey yet, leader=%d)", key, leader)
				lastStuckLog = time.Now()
			}
			ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply)
			if !ok {
				time.Sleep(10 * time.Millisecond)
				leader = (leader + 1) % len(ck.servers)
				continue
			}

			if reply.Err != rpc.ErrWrongLeader {
				break
			}
			leader = (leader + 1) % len(ck.servers)
		}

		if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
			break
		}
	}
	// Log what gets recorded in history (for linearizability debugging)
	log.Printf("[HISTORY] Get key=%s -> value=%q version=%d err=%s", key, reply.Value, reply.Version, reply.Err)
	return reply.Value, reply.Version, reply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}

	leader := 0
	// isResend = true if we might have already applied (reply lost or ErrWrongLeader after apply).
	// Then on ErrVersion we return ErrMaybe so the test counts this put in Nmaybe (server version <= Nok+Nmaybe).
	isResend := false
	lastStuckLog := time.Now()
	for {
		if time.Since(lastStuckLog) > 5*time.Second {
			log.Printf("[TEST] Clerk Put key=%s ver=%d retrying >5s (no OK/ErrVersion yet, leader=%d)", key, version, leader)
			lastStuckLog = time.Now()
		}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Put", &args, &reply)
		if !ok {
			isResend = true // reply lost; put might have been applied
			leader = (leader + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err != rpc.ErrWrongLeader {
			break
		}
		// Leader may have applied our put then stepped down before replying; treat as uncertain.
		isResend = true
		leader = (leader + 1) % len(ck.servers)
	}

	var ret rpc.Err
	if !isResend {
		ret = reply.Err
	} else if reply.Err == rpc.ErrVersion {
		ret = rpc.ErrMaybe
	} else {
		ret = reply.Err
	}
	log.Printf("[HISTORY] Put key=%s value=%q version=%d -> err=%s (isResend=%v replyErr=%s)", key, value, version, ret, isResend, reply.Err)
	return ret
}
