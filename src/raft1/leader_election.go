package raft

import (
	"math/rand"
	"sync"
	"time"

	"6.5840/raftapi"
)

func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}

func (rf *Raft) leaderElection() {
	//一个任期内不能有超过一个Leader
	//所以为了成为一个新的Leader，这里需要开启一个新的任期
	rf.currentTerm++
	rf.state = raftapi.Candidate
	rf.voteFor = rf.me

	//应在所有修改持久化状态（currentTerm, voteFor, log）的地方调用persist()
	rf.persist()
	rf.resetElectionTimer()

	term := rf.currentTerm
	// 自己已经给自己投了一票了
	voteCounter := 1
	lastLog := rf.log.LastLog()
	DPrintf("[%v]: start leader election, term %d\n", rf.me, rf.currentTerm)

	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogTerm:  lastLog.Term,
		LastLogIndex: lastLog.Index,
	}
	var becomeLeader sync.Once

	for serverId, _ := range rf.peers {
		if serverId != rf.me {
			go rf.candidateRequestVote(serverId, &args, &voteCounter, &becomeLeader)
		}
	}
}

// if RPC request or response contains term T > currentTerm:
// set New Term and Convert to follower
func (rf *Raft) setNewTerm(newTerm int) {
	if newTerm > rf.currentTerm || rf.currentTerm == 0 {
		rf.state = raftapi.Follower
		rf.currentTerm = newTerm
		rf.voteFor = -1
		// 是否要重置计时器
		DPrintf("[%d]: set term %v\n", rf.me, rf.currentTerm)
		rf.persist()
	}
}
