package raft

import "6.5840/raftapi"

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	LastIncludedCmd   interface{} // 快照中最后一条日志的命令内容。在日志截断之后进行占位
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) RealLogIdx(vIdx int) int {
	return vIdx - rf.lastIncludedIndex
}

func (rf *Raft) VirtualLogIdx(realIdx int) int {
	return realIdx + rf.lastIncludedIndex
}

func (rf *Raft) readSnapshot(data []byte) {
	// 目前只在Make中调用, 因此不需要锁
	if len(data) == 0 {
		DPrintf("server %v 读取快照失败: 无快照\n", rf.me)
		return
	}
	rf.snapShot = data
	DPrintf("server %v 读取快照c成功\n", rf.me)
}

// Leader 发送快照
func (rf *Raft) handleInstallSnapshot(peer int) {
	reply := &InstallSnapshotReply{}
	rf.mu.Lock()
	// 只有领导者才能发送
	if rf.state != raftapi.Leader {
		rf.mu.Unlock()
		return
	}

	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapShot,
		LastIncludedCmd:   rf.log.Entries[0].Command,
	}

	// 发送 RPC 不应该持有锁
	rf.mu.Unlock()
	ok := rf.sendInstallSnapshot(peer, args, reply)
	if !ok {
		DPrintf("server %v 发送快照失败: peer %v", rf.me, peer)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}

	// 只有在当前任期的回复才更新nextIndex和matchIndex
	// 防止过期的RPC响应导致nextIndex错误回退
	if args.Term == rf.currentTerm {
		// 快照包含了到lastIncludedIndex的所有日志
		// follower现在拥有了这些日志，下一条要发送的是lastIncludedIndex+1
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
		rf.matchIndex[peer] = args.LastIncludedIndex
		DPrintf("leader %v 发送快照成功到 server %v, 更新 nextIndex[%v]=%v, matchIndex[%v]=%v",
			rf.me, peer, peer, rf.nextIndex[peer], peer, rf.matchIndex[peer])
	}
}

func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

/*
Usually the snapshot will contain new information not already in the recipient’s log.
In this case, the follower discards its entire log;
it is all superseded by the snapshot and may possibly have uncommitted entries that conflict
with the snapshot.
If instead the follower receives a snapshot that describes a prefix of its log (due to retransmis-
sion or by mistake), then log entries covered by the snapshot are deleted but entries following the snapshot are still
valid and must be retained
*/
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer func() {
		rf.resetElectionTimer()
		rf.mu.Unlock()
	}()

	// rules for all server 2
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = raftapi.Follower
		rf.voteFor = -1
	}

	hasEntry := false
	// 如果已有的日志条目与快照中最后包含的条目的索引和任期相同，则保留其后的日志条目并进行回复
	// 检查args.LastIncludedIndex是否在当前日志范围内
	if args.LastIncludedIndex >= rf.log.Index0 && args.LastIncludedIndex < rf.log.Len() && rf.log.At(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		hasEntry = true
	}

	msg := &raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	if hasEntry {
		idx := rf.RealLogIdx(args.LastIncludedIndex)
		rf.log.Entries = append([]raftapi.Entry{}, rf.log.Entries[idx:]...)
	} else {
		rf.log = raftapi.MakeEmptyLog()
		// 添加占位符条目，代表快照的最后一条日志
		// 必须设置Index，因为LastLog()会依赖它
		rf.log.Append(raftapi.Entry{
			Term:    args.LastIncludedTerm,
			Command: args.LastIncludedCmd,
			Index:   args.LastIncludedIndex,
		})
	}

	// 使用快照内容重置状态机（并加载快照中的集群配置信息）
	rf.snapShot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	// 更新Log的Index0，使其知道数组起始位置对应的虚拟索引
	rf.log.Index0 = args.LastIncludedIndex

	// 需要检查lastApplied和commitIndex 是否小于LastIncludedIndex, 如果是, 更新为LastIncludedIndex
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	reply.Term = rf.currentTerm
	rf.applyCh <- *msg
	rf.persist()
}
