package raft

import (
	"6.5840/raftapi"
)

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
		// rules for leader 3
		if reply.Success {
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			// 防止因网络延迟而导致的回退
			rf.nextIndex[peer] = max(rf.nextIndex[peer], next)
			rf.matchIndex[peer] = max(rf.matchIndex[peer], match)
			DPrintf("[%v]: %v append success next %v match %v", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer])
		} else if reply.Conflict {
			// fast backup to do

		} else if rf.nextIndex[peer] > 1 {
			rf.nextIndex[peer]--
		}

		rf.leaderCommitRule()
	}
}

// Receiver implementation of AppendEntries RPC (Handler)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d]: (term %d) follower 收到 [%v] AppendEntries %v, prevIndex %v, prevTerm %v", rf.me, rf.currentTerm, args.LeaderId, args.Entries, args.PrevLogIndex, args.PrevLogTerm)

	// rules for servers --> all server 2
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		return
	}
	// Receiver implementation 1
	if args.Term < rf.currentTerm {
		return // ？ 为什么不成功不重置呢
	}

	rf.resetElectionTimer()
	// candidate rule 3
	if rf.state == raftapi.Candidate {
		rf.state = raftapi.Follower
	}
	// append entries rpc 2
	// fast backup info
	if rf.log.LastLog().Index < args.PrevLogIndex {
		reply.Conflict = true
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.log.Len()
		DPrintf("[%v]: Conflict XTerm %v, XIndex %v, XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}
	if rf.log.At(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Conflict = true
		// to do fast backup
		return
	}

	for idx, entry := range args.Entries {
		// Receiver implementation 3
		if entry.Index <= rf.log.LastLog().Index && rf.log.At(entry.Index).Term != entry.Term {
			rf.log.Truncate(entry.Index)
			rf.persist()
		}
		// Receiver implementation 4
		// is new command exist仅需判断是否大于lastLog.index
		if entry.Index > rf.log.LastLog().Index {
			rf.log.Append(entry)
			DPrintf("[%d]: follower append [%v]", rf.me, args.Entries[idx:])
			rf.persist()
			break
		}
	}
	// Receiver implementation 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.LastLog().Index)
		rf.apply()
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) leaderCommitRule() {
	// leader rule 4
	if rf.state != raftapi.Leader {
		return
	}

	for n := rf.commitIndex + 1; n <= rf.log.LastLog().Index; n++ {
		// cannot commit entries from old term
		if rf.log.At(n).Term != rf.currentTerm {
			continue
		}
		// count how many matchIndex >= n
		counter := 1 // include self
		for serverId := 0; serverId < len(rf.peers); serverId++ {
			if serverId != rf.me && rf.matchIndex[serverId] >= n {
				counter++
			}
			if counter > len(rf.peers)/2 {
				rf.commitIndex = n
				DPrintf("[%v] leader尝试提交 index %v", rf.me, rf.commitIndex)
				rf.apply()
				break
			}
		}
	}
}
