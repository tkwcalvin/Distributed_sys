package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const (
	LOCKED   = "locked"
	UNLOCKED = "unlocked"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck        kvtest.IKVClerk
	client_id string
	lock_key  string
	lock_ver  rpc.Tversion
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, client_id: kvtest.RandValue(8), lock_key: l}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.lock_key)
		if err == rpc.ErrNoKey || (err == rpc.OK && val == UNLOCKED) {
			log.Println("Client ", lk.client_id, " get no key or unlocked, putting locked")
			err = lk.ck.Put(lk.lock_key, LOCKED, ver)
			if err == rpc.OK {
				lk.lock_ver = ver + 1
				log.Println("Client ", lk.client_id, " put locked, version ", lk.lock_ver)
				break
			}
		}
		log.Println("Client ", lk.client_id, " get error, sleeping")
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	lk.ck.Put(lk.lock_key, UNLOCKED, lk.lock_ver)
	log.Println("Client ", lk.client_id, " release lock, version ", lk.lock_ver)
}
