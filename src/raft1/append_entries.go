package raft

import "6.5840/raftapi"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []raftapi.Entry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	// fast backup
	Conflict bool
	XTerm    int
	XIndex   int
	XLen     int
}

func (rf *Raft) appendEntries(heartbeat bool) {
	lastLog := rf.log.LastLog()

	for peer, _ := range rf.peers {
		if peer == rf.me {
			rf.resetElectionTimer()
			continue
		}

		// rules for leader,即使是心跳也要进行f2的检查，既不应区别对待
		// 发送从nextIndex开始的appendEntry
		if lastLog.Index >= rf.nextIndex[peer] || heartbeat {
			nextIndex := rf.nextIndex[peer]

			// 初始化
			if nextIndex <= 0 {
				nextIndex = 1
			}
			if lastLog.Index+1 < nextIndex {
				nextIndex = lastLog.Index
			}

			prevLog := rf.log.At(nextIndex - 1) // peer's last log
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				Entries:      make([]raftapi.Entry, lastLog.Index-nextIndex+1),
				LeaderCommit: rf.commitIndex,
			}
			copy(args.Entries, rf.log.Slice(nextIndex))

			go rf.leaderSendEntries(peer, &args)
		}
	}
}

func (rf *Raft) leaderSendEntries(peer int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(peer, args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rules for all server 2
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
	if args.Term == rf.currentTerm {
		// rules for leader 3.1
		if reply.Success {
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.nextIndex[peer] = max(rf.nextIndex[peer], next)
			rf.matchIndex[peer] = max(rf.matchIndex[peer], match)
			DPrintf("[%v]: %v append success next %v match %v", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer])
		} else if reply.Conflict {
			// fast backup
			
		} else if rf.nextIndex[peer] > 1 {
			rf.nextIndex[peer]--
		}

		rf.leaderCommitRule()
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
