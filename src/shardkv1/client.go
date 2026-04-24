package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"log"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// cache clerks by gid; invalidated when config num changes
	mu     sync.Mutex
	cfgNum shardcfg.Tnum
	clerks map[tester.Tgid]*shardgrp.Clerk
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:   clnt,
		sck:    sck,
		clerks: make(map[tester.Tgid]*shardgrp.Clerk),
	}
	return ck
}

// getClerk returns a cached clerk for the given gid/servers, or creates and caches one.
// Cache is invalidated when config num changes.
func (ck *Clerk) getClerk(cfg *shardcfg.ShardConfig, gid tester.Tgid, servers []string) *shardgrp.Clerk {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if ck.cfgNum < cfg.Num {
		ck.clerks = make(map[tester.Tgid]*shardgrp.Clerk)
		ck.cfgNum = cfg.Num
	}
	if clerk, ok := ck.clerks[gid]; ok {
		return clerk
	}
	clerk := shardgrp.MakeClerk(ck.clnt, servers)
	ck.clerks[gid] = clerk
	return clerk
}

func truncVal(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		shard := shardcfg.Key2Shard(key)
		cfg := ck.sck.Query()
		gid, servers, ok := cfg.GidServers(shard)
		if !ok {
			return "", 0, rpc.ErrWrongGroup
		}
		clerk := ck.getClerk(cfg, gid, servers)
		log.Printf("[LIN] ShardKV.Get start key=%s shard=%d cfgNum=%d gid=%d", key, shard, cfg.Num, gid)
		val, ver, err := clerk.Get(key)
		if err != rpc.ErrWrongGroup {
			log.Printf("[LIN] ShardKV.Get done key=%s value=%q ver=%d err=%s gid=%d", key, truncVal(val, 60), ver, err, gid)
			return val, ver, err
		}
		// Shard moved or frozen (e.g. migration in progress). Back off so we don't spin
		// while ShardCtrler has not yet published the new config.
		log.Printf("[ShardKV clerk] Get key=%s ErrWrongGroup, retry with fresh config (current cfgNum=%d)", key, cfg.Num)
		time.Sleep(80 * time.Millisecond)
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	for {
		shard := shardcfg.Key2Shard(key)
		cfg := ck.sck.Query()
		gid, servers, ok := cfg.GidServers(shard)
		if !ok {
			return rpc.ErrWrongGroup
		}
		clerk := ck.getClerk(cfg, gid, servers)
		log.Printf("[LIN] ShardKV.Put start key=%s value=%q ver=%d cfgNum=%d gid=%d", key, truncVal(value, 60), version, cfg.Num, gid)
		err := clerk.Put(key, value, version)
		if err != rpc.ErrWrongGroup {
			log.Printf("[LIN] ShardKV.Put done key=%s ver=%d err=%s gid=%d", key, version, err, gid)
			return err
		}
		// Shard moved or frozen (e.g. migration in progress). Back off so we don't spin
		// while ShardCtrler has not yet published the new config.
		log.Printf("[ShardKV clerk] Put key=%s ErrWrongGroup, retry with fresh config (current cfgNum=%d)", key, cfg.Num)
		time.Sleep(80 * time.Millisecond)

	}
}
