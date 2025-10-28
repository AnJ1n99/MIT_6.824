package raft

import (
	"sync"

	"6.5840/raftapi"
)

func (rf *Raft) candidateRequestVote(serverId int, args *RequestVoteArgs,
	voteCounter *int, becomeLeader *sync.Once) {
	DPrintf("[%d]: term %v send vote request to %d\n", rf.me, args.Term, serverId)

	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, args, &reply)
	if !ok { // timeout
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//正常的 Raft 行为：
	//• 当服务器收到带有 args.Term 的 RequestVote 时，如果 args.Term 高于其当前任期，它应在回复之前更新为 args.Term
	//• 因此，通常情况下 reply.Term >= args.Term

	if reply.Term > args.Term { // 已经有人当选
		DPrintf("[%d]: %d 在新的term，更新term，结束\n", rf.me, serverId)
		rf.setNewTerm(reply.Term)
		return
	}

	if reply.Term < args.Term {
		DPrintf("[%d]: %d 的term %d 已经失效，结束\n", rf.me, serverId, reply.Term)
		return
	}

	if !reply.VoteGranted {
		DPrintf("[%d]: %d 没有投给me，结束\n", rf.me, serverId)
		return
	}
	DPrintf("[%d]: from %d term一致，且投给%d\n", rf.me, serverId, rf.me)

	*voteCounter++

	if *voteCounter > len(rf.peers)/2 && rf.currentTerm == args.Term && rf.state == raftapi.Candidate {
		DPrintf("[%d]: 获得多数选票，可以提前结束\n", rf.me)

		becomeLeader.Do(func() {
			DPrintf("[%d]: 当前term %d 结束\n", rf.me, rf.currentTerm)

			rf.state = raftapi.Leader
			lastLogIndex := rf.log.LastLog().Index

			for i, _ := range rf.peers {
				rf.nextIndex[i] = lastLogIndex + 1
				rf.matchIndex[i] = 0
			}
			DPrintf("[%d]: leader - nextIndex %#v", rf.me, rf.nextIndex)
			rf.appendEntries(true)
		})
	}
}
