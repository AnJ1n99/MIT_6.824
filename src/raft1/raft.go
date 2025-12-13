package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state         raftapi.RaftState
	appendEntryCh chan *raftapi.Entry

	// persistent State
	currentTerm int
	voteFor     int
	log         raftapi.Log // Entries

	// volatile State
	commitIndex int
	lastApplied int

	// Volatile State On Leader    (Reinitialized after election)
	//该字段仅在领导者节点上有效，记录了每个跟随者已成功复制的最高日志条目索引，用于判断何时可以安全提交日志。
	nextIndex  []int
	matchIndex []int

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond
	//记录下次选举超时的时间点
	electionTime time.Time

	snapShot          []byte // 快照
	lastIncludedIndex int    // 快照的最后一条日志的索引
	lastIncludedTerm  int    // 快照的最后一条日志的任期号
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.state == raftapi.Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	DPrintVerbose("[%v]: STATE: %v", rf.me, rf.log.String())
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	data := w.Bytes()
	rf.persister.SaveRaftState(data, rf.snapShot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var logs raftapi.Log
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		log.Fatal("Failed to read persist\n")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = votedFor
		rf.log = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintVerbose("[%v]: Snapshot: index %v, snapshot %v", rf.me, index, snapshot)

	// 快照不能包含未提交的日志
	// 快照不能包含重复的快照请求
	if index > rf.commitIndex || index <= rf.log.Index0 {
		DPrintf("server[%v] : 拒绝snapshot请求 (index=%d, commitIndex=%d, lastIncludedIndex=%d)",
			rf.me, index, rf.commitIndex, rf.lastIncludedIndex)
		return
	}
	if index > rf.log.LastLog().Index {
		DPrintf("server[%v] : 拒绝snapshot请求 (index=%d, lastLogIndex=%d)",
			rf.me, index, rf.log.LastLog().Index)
		return
	}
	DPrintf("server[%v]: 接受snapshot请求 (index=%d)", rf.me, index)

	// 保存快照数据
	rf.snapShot = snapshot

	// 截断日志：只保留index之后的日志条目
	// 将index对应的日志转换为真实数组索引后进行截断
	realIdx := index - rf.log.Index0
	if realIdx < 0 || realIdx >= len(rf.log.Entries) {
		DPrintf("server[%v] : 拒绝snapshot请求 (index=%d, index0=%d, entries=%d)",
			rf.me, index, rf.log.Index0, len(rf.log.Entries))
		return
	}

	// 获取快照最后一条日志的任期号
	rf.lastIncludedTerm = rf.log.Entries[realIdx].Term

	rf.log.Entries = append([]raftapi.Entry{}, rf.log.Entries[realIdx:]...)
	rf.lastIncludedIndex = index
	// 更新Log的Index0，使其知道数组起始位置对应的虚拟索引
	rf.log.Index0 = index
	if rf.lastApplied < index {
		// 能被提交快照之后的日志肯定是已经被应用的
		rf.lastApplied = index
	}
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// Election Restriction: (防止commited log被覆盖)--只有拥有最新日志的节点才能成为 Leader。
	//候选人最后一条Log条目的任期号大于本地最后一条Log条目的任期号；
	//或者，候选人最后一条Log条目的任期号等于本地最后一条Log条目的任期号，且候选人的Log记录长度大于等于本地Log记录的长度
	myLastLog := rf.log.LastLog()
	upToDate := args.LastLogTerm > myLastLog.Term || (args.LastLogTerm == myLastLog.Term && args.LastLogIndex >= myLastLog.Index)
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer()
		DPrintf("[%v]: term %v vote %v", rf.me, rf.currentTerm, rf.voteFor)
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != raftapi.Leader {
		return -1, rf.currentTerm, false
	}

	index := rf.log.LastLog().Index + 1
	term := rf.currentTerm

	log := raftapi.Entry{
		Command: command,
		Term:    term,
		Index:   index,
	}
	rf.log.Append(log)
	rf.persist()
	DPrintf("[%v]: term %v Start %v", rf.me, term, log)
	rf.appendEntries(false)

	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350 milliseconds.
		// Raft使用随机的选举超时，以确保分裂投票很少发生，并能迅速解决
		ms := 50 // + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		if rf.state == raftapi.Leader {
			// 如果是leader则会立马发送心跳包
			rf.appendEntries(true)
		}
		if time.Now().After(rf.electionTime) {
			// 开启选举
			rf.leaderElection()
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = raftapi.Follower
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.resetElectionTimer()

	rf.log = raftapi.MakeEmptyLog()
	// 添加一条哨兵日志，index从1开始
	rf.log.Append(raftapi.Entry{Command: -1, Term: 0, Index: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	// 将command 应用到statemachine中 。push committed logs into applyCh exactly once
	go rf.applier()

	return rf
}

func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
	DPrintf("[%v]: rf.applyCond.Broadcast()", rf.me)
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		// all server rule 1
		if rf.commitIndex > rf.lastApplied && rf.log.LastLog().Index > rf.lastApplied {
			rf.lastApplied++
			applyMsg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log.At(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			DPrintVerbose("[%v]: COMMIT %d: %v", rf.me, rf.lastApplied, rf.commits())
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
			DPrintf("[%v]: rf.applyCond.Wait()", rf.me)
		}
	}
	// 要关闭 chan 防止影响上层的 service
	close(rf.applyCh)
}

func (rf *Raft) commits() string {
	nums := []string{}
	for i := rf.log.Index0; i <= rf.lastApplied; i++ {
		nums = append(nums, fmt.Sprintf("%4d", rf.log.At(i).Command))
	}
	return fmt.Sprint(strings.Join(nums, "|"))
}
