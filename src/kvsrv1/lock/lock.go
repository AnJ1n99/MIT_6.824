package lock

import (
	"fmt"
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	MutexName string
	id        string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:        ck,
		MutexName: l,
		id:        fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Int63()),
	}
	ck.Put(lk.MutexName, "", 0)
	return lk
}

func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.MutexName)
		if val == lk.id {
			return
		}
		if val == "" && err == rpc.OK {
			err := lk.ck.Put(lk.MutexName, lk.id, ver)
			switch err {
			case rpc.OK:
				return
			case rpc.ErrVersion:
				// 再看看情况
			default:
				val2, _, _ := lk.ck.Get(lk.MutexName)
				if val2 == lk.id {
					return
				}
			}
			time.Sleep(time.Duration(rand.Int63n(100)) * time.Millisecond)
		}
	}
}

func (lk *Lock) Release() {
	for {
		val, ver, _ := lk.ck.Get(lk.MutexName)
		if val != lk.id {
			return
		}
		err := lk.ck.Put(lk.MutexName, "", ver)
		switch err {
		case rpc.OK:
			return
		case rpc.ErrVersion:
			// 再看看情况
		default:
			val2, _, err := lk.ck.Get(lk.MutexName)
			if val2 == "" && err == rpc.OK {
				return
			}
		}
		time.Sleep(time.Duration(rand.Int63n(100)) * time.Millisecond)

	}
}
