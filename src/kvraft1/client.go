package kvraft

import (
	"math/rand"
	"sync"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	//  缓存上一次成功通信的服务器索引，减少错误重试次数
	cacheLeaderIdx int
	clientId       int64
	requestId      int64
	mu             sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:    clnt,
		servers: servers,

		cacheLeaderIdx: 0,
		clientId:       rand.Int63(),
		requestId:      0,
		mu:             sync.Mutex{},
	}
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
	i := ck.cacheLeaderIdx
	defer func() {
		ck.cacheLeaderIdx = i
	}()

	ck.mu.Lock()
	ck.requestId++
	reqId := ck.requestId
	ck.mu.Unlock()

	args := GetArgs{
		Key:      key,
		ReqId:    reqId,
		ClientId: ck.clientId,
	}

	reply := GetReply{}
	var ok bool
	// 收到非领导者或者错误时不断重复
	for reply.Err == rpc.ErrWrongLeader || !ok {
		ok = ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
		i = (i + 1) % len(ck.servers)
	}
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
	// You will have to modify this function.
	i := ck.cacheLeaderIdx
	defer func() {
		ck.cacheLeaderIdx = i
	}()

	ck.mu.Lock()
	ck.requestId++
	reqId := ck.requestId
	ck.mu.Unlock()

	args := PutArgs{
		Key:      key,
		Value:    value,
		ReqId:    reqId,
		ClientId: ck.clientId,
		Version:  version,
	}

	reply := PutReply{}
	var ok bool
	// 收到非领导者或者错误时不断重复
	for reply.Err == rpc.ErrWrongLeader || !ok {
		ok = ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
		i = (i + 1) % len(ck.servers)
	}
	return reply.Err
}
