package kvraft

import (
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"

	"github.com/bytedance/sonic"
)

const (
	GetOperation = 1
	PutOperation = 2
)

type LastApplied struct {
	LastAppliedId int64
	Reply         Reply
}

type KeyValue struct {
	Key   string
	Value string
	Ver   rpc.Tversion
}

// DoOp 的参数和返回值
type Request struct {
	Type     int
	Key      string
	Value    string
	Version  rpc.Tversion
	ClientId int64
	ReqId    int64
}

type Reply struct {
	Key     string
	Value   string
	Version rpc.Tversion
	Err     rpc.Err
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu sync.Mutex
	// store
	kvs map[string]*KeyValue
	// clinetID --> lastApplied + lastResponse
	last map[int64]*LastApplied
}

// 有锁条件使用
func (kv *KVServer) IsDuplicate(clientId int64, reqId int64) (any, bool) {
	// 客户端对同一个 ClientId 的请求是顺序发送的：在收到某个ReqId 的回复前不会发下一个更大的 ReqId
	if last, ok := kv.last[clientId]; ok && last.LastAppliedId == reqId {
		return last.Reply, true
	}
	return nil, false
}

// 有锁条件使用
func (kv *KVServer) StoreLastApplied(clientId int64, reqId int64, reply Reply) {
	kv.last[clientId] = &LastApplied{
		reqId,
		reply,
	}
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
// 请求层的去重
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	Req := req.(Request)

	switch Req.Type {
	case GetOperation:
		kv.mu.Lock()
		defer kv.mu.Unlock()

		var reply Reply
		KV, ok := kv.kvs[Req.Key]
		if !ok {
			reply.Err = rpc.ErrNoKey
			return reply
		}
		reply.Value = KV.Value
		reply.Version = KV.Ver
		reply.Err = rpc.OK
		return reply
	case PutOperation:
		kv.mu.Lock()
		defer kv.mu.Unlock()

		if rep, ok := kv.IsDuplicate(Req.ClientId, Req.ReqId); ok {
			// 去重
			return rep
		}
		var reply Reply

		KV, ok := kv.kvs[Req.Key]
		if !ok {
			// 版本号错误
			if Req.Version != 0 {
				reply.Err = rpc.ErrNoKey
				kv.StoreLastApplied(Req.ClientId, Req.ReqId, reply)
				return reply
			}
			// 版本号为零
			// 新建值
			KV = &KeyValue{
				Req.Key,
				Req.Value,
				1,
			}
			kv.kvs[Req.Key] = KV
			reply.Err = rpc.OK
			kv.StoreLastApplied(Req.ClientId, Req.ReqId, reply)
			return reply
		}

		// 有数据，判断版本号
		if Req.Version != KV.Ver {
			reply.Err = rpc.ErrVersion
			kv.StoreLastApplied(Req.ClientId, Req.ReqId, reply)
			return reply
		}

		// 版本号正确，更新值
		KV.Value = Req.Value
		KV.Ver++
		kv.kvs[Req.Key] = KV
		reply.Err = rpc.OK
		kv.StoreLastApplied(Req.ClientId, Req.ReqId, reply)
		return reply
	default:
		return Reply{}
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	data, _ := sonic.Marshal(kv.kvs)
	return data
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.kvs = make(map[string]*KeyValue)
	sonic.Unmarshal(data, &kv.kvs)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	req := Request{
		Type: GetOperation,
		Key:  args.Key,
	}
	err, replyValue := kv.rsm.Submit(req)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	reply.Value = replyValue.(Reply).Value
	reply.Version = replyValue.(Reply).Version
	reply.Err = replyValue.(Reply).Err
}

func (kv *KVServer) Put(args *PutArgs, reply *PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	req := Request{
		Type:     PutOperation,
		Key:      args.Key,
		Value:    args.Value,
		Version:  args.Version,
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
	}
	err, replyValue := kv.rsm.Submit(req)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	reply.Err = replyValue.(Reply).Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(Request{})
	labgob.Register(Reply{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me:   me,
		kvs:  make(map[string]*KeyValue),
		mu:   sync.Mutex{},
		last: make(map[int64]*LastApplied),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
