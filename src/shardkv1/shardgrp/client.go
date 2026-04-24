package shardgrp

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	leader := 0
	for {
		// find the leader server
		for {

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

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	var reply shardrpc.FreezeShardReply
	leader := 0
	for {
		reply = shardrpc.FreezeShardReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.FreezeShard", &args, &reply)
		if !ok {
			leader = (leader + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err != rpc.ErrWrongLeader {
			break
		}
		leader = (leader + 1) % len(ck.servers)
	}
	return reply.State, reply.Err
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	var reply shardrpc.InstallShardReply
	leader := 0
	for {
		reply = shardrpc.InstallShardReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.InstallShard", &args, &reply)
		if !ok {
			leader = (leader + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err != rpc.ErrWrongLeader {
			break
		}
		leader = (leader + 1) % len(ck.servers)
	}
	return reply.Err
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	var reply shardrpc.DeleteShardReply
	leader := 0
	for {
		reply = shardrpc.DeleteShardReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.DeleteShard", &args, &reply)
		if !ok {
			leader = (leader + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err != rpc.ErrWrongLeader {
			break
		}
		leader = (leader + 1) % len(ck.servers)
	}
	return reply.Err
}
