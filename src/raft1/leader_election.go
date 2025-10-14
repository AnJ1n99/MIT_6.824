package raft

import (
	"sync"

	"6.5840/raftapi"
)

func (rf *Raft) resetElectionTimer() {

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
	voteCounter := 1
	lastLog := rf.log.lastLog()
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
			go rf.candidateRequestVote()
		}
	}
}
