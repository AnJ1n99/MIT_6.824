package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	kvs map[string]*KeyValue
}

type KeyValue struct {
	Key   string
	Value string
	Ver   rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		mu:  sync.Mutex{},
		kvs: make(map[string]*KeyValue),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	KV, ok := kv.kvs[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = KV.Value
	reply.Version = KV.Ver
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
	KV, ok := kv.kvs[args.Key]
	// 没有数据
	if !ok {
		// 版本号不对
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		// 版本号为 0，表示新增键值。
		KV = &KeyValue{args.Key, args.Value, 1}
		kv.kvs[args.Key] = KV
		reply.Err = rpc.OK
		return
	}
	// 有数据，判断版本号是否对得上
	if args.Version != KV.Ver {
		reply.Err = rpc.ErrVersion
		return
	}
	// 版本号对得上，更新数据
	KV.Value = args.Value
	KV.Ver += 1
	kv.kvs[args.Key] = KV
	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
