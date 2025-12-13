package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"github.com/bwmarrin/snowflake"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

// 作为在 Raft 日志中复制状态变更操作的基本单元
// 以便 Go 的 RPC 系统能够正确序列化和反序列化
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int64
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.

	chdone chan struct{}
	// RPC Handler 是阻塞等待结果的，而 Raft 的提交是异步的。
	// 为了连接两者，我使用了一个 Map
	notifyChans map[int64]chan any // index -> 等待通道
	node        *snowflake.Node
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	sf, _ := snowflake.NewNode(int64(me))

	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		// RSM connect to raft module
		applyCh: make(chan raftapi.ApplyMsg),
		sm:      sm,

		chdone: make(chan struct{}),

		notifyChans: make(map[int64]chan any),
		node:        sf,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	data := persister.ReadSnapshot()
	if len(data) != 0 {
		rsm.sm.Restore(data)
	}

	// 启动 goroutine 处理 applyCh 中的消息
	go rsm.Reader()

	return rsm
}

// applier 处理从 applyCh 接收到的消息
func (rsm *RSM) Reader() {
	for msg := range rsm.applyCh {
		rsm.mu.Lock()

		if msg.CommandValid {
			op := msg.Command.(Op)
			res := rsm.sm.DoOp(op.Req)
			if rsm.shouldSnapshot() {
				rsm.Raft().Snapshot(msg.CommandIndex, rsm.sm.Snapshot())
			}
			// fmt.Println("RSM: got apply msg for op", op.Id, "res", res)
			if _, ok := rsm.notifyChans[op.Id]; !ok {
				rsm.mu.Unlock()
				continue
			}

			ch := rsm.notifyChans[op.Id]
			delete(rsm.notifyChans, op.Id)
			go func() {
				ch <- res
			}()
		} else if msg.SnapshotValid {
			// fmt.Println("backup snapshot")
			rsm.sm.Restore(msg.Snapshot)
		}
		rsm.mu.Unlock()
	}
	close(rsm.chdone)
}

func (rsm *RSM) shouldSnapshot() bool {
	if rsm.maxraftstate == -1 {
		return false
	}
	return rsm.rf.PersistBytes() > rsm.maxraftstate/10*9
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()
	me := rsm.me
	id := rsm.node.Generate().Int64()

	op := Op{
		Req: req,
		Me:  me,
		Id:  id,
	}
	// ch := make(chan any, 1) 创建一个缓冲区为 1 的 channel，
	// 用来在 Raft 日志提交后，把执行结果/确认信号异步送回发起方。
	// rsm.notifyChans[id] = ch 把这个通道按本次操作的唯一标识 id 存入映射，
	// 后续当 applyCh 里对应日志条目被提交并被状态机执行完时，
	// applier 会用同一个 id 取出 channel，向里面写入结果并唤醒等待的客户端 goroutine。
	// 这样 Submit/客户端可以阻塞等结果，又能按不同请求的 id 对应到各自的 channel，
	// 实现一对一通知。

	// NOTICE: ONLY Asynchronous
	ch := make(chan any, 1)
	rsm.notifyChans[id] = ch

	rsm.mu.Unlock()

	_, startTerm, isLeader := rsm.Raft().Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		// fmt.Printf("wait for %v", rsm.me)

		// Handler select 监听这个 channel
		select {
		case res := <-ch:
			return rpc.OK, res
		case <-rsm.chdone:
			return rpc.ErrWrongLeader, nil
		case <-ticker.C: // 每 50ms 检查一次当前节点是否仍是领导者，防止在领导权丢失后无限等待
			currentTerm, currentLeader := rsm.Raft().GetState()

			if currentTerm != startTerm || !currentLeader {
				rsm.mu.Lock()
				// 可能 op.Id 对应的通道已被其他协程删除或替换，如果 key 不在 map 里，ok 会是 false，
				// 避免对不存在的 key 做后续比较或删除。
				// 结合 cur == ch 的判断，只有当 map 里仍然保存的是本次请求的 channel 时才删除，
				// 防止同一个 op.Id 被新的请求复用时误删。这样可以避免竞争条件导致的新通道被误删。
				// 必须确保只删除本调用创建的 channel，防止多个协程间竞争导致误删
				if cur, ok := rsm.notifyChans[op.Id]; ok && cur == ch {
					delete(rsm.notifyChans, op.Id)
				}
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}
