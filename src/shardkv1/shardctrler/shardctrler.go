package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	num shardcfg.Tnum
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Store config under key "config"; kvsrv Put(key, value, version)
	sck.IKVClerk.Put("config", cfg.String(), 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	// Get current config and its stored version; kvsrv Put only succeeds when args.Version matches the key's current version.
	value, storedVersion, _ := sck.IKVClerk.Get("config")
	var oldCfg *shardcfg.ShardConfig

	oldCfg = shardcfg.FromString(value)

	for s := shardcfg.Tshid(0); s < shardcfg.NShards; s++ {
		oldGid := oldCfg.Shards[s]
		newGid := new.Shards[s]
		if oldGid == newGid {
			continue
		}
		oldServers, hasOld := oldCfg.Groups[oldGid]
		newServers, hasNew := new.Groups[newGid]
		if !hasOld || len(oldServers) == 0 || !hasNew || len(newServers) == 0 {
			continue
		}
		// Migrate shard s: freeze at old, install at new, delete at old.
		oldClerk := shardgrp.MakeClerk(sck.clnt, oldServers)
		newClerk := shardgrp.MakeClerk(sck.clnt, newServers)
		state, err := oldClerk.FreezeShard(s, new.Num)
		if err != rpc.OK {
			log.Fatalf("[ShardCtrler] FreezeShard shard=%d oldGid=%d err=%s", s, oldGid, err)
		}
		if err := newClerk.InstallShard(s, state, new.Num); err != rpc.OK {
			log.Fatalf("[ShardCtrler] InstallShard shard=%d newGid=%d err=%s", s, newGid, err)
		}
		if err := oldClerk.DeleteShard(s, new.Num); err != rpc.OK {
			log.Fatalf("[ShardCtrler] DeleteShard shard=%d oldGid=%d err=%s", s, oldGid, err)
		}
	}

	sck.IKVClerk.Put("config", new.String(), storedVersion)
	sck.num = new.Num
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	value, _, err := sck.IKVClerk.Get("config")
	if err == rpc.ErrNoKey {
		return nil
	}
	return shardcfg.FromString(value)
}
