package raftapi

// The Raft interface
type Raft interface {
	// Start agreement on a new log entry, and return the log index
	// for that entry, the term, and whether the peer is the leader.
	Start(command interface{}) (int, int, bool)

	// Ask a Raft for its current term, and whether it thinks it is
	// leader
	GetState() (int, bool)

	// For Snaphots (3D)
	Snapshot(index int, snapshot []byte)
	PersistBytes() int

	// For the tester to indicate to your code that is should cleanup
	// any long-running go routines.
	Kill()
}

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the server (or
// tester), via the applyCh passed to Make(). Set CommandValid to true
// to indicate that the ApplyMsg contains a newly committed log entry.
//
// In Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 这些log struct是自己定义的

// peers仅会有三种状态
type RaftState string

const (
	Leader    RaftState = "Leader"
	Follower            = "Follower"
	Candidate           = "Candidate"
)

type Log struct {
	Entries []Entry
	Index0  int
}

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

// method
func MakeEmptyLog() Log {
	log := Log{
		Entries: make([]Entry, 0),
		Index0:  0,
	}
	return log
}

func (l *Log) Append(entries ...Entry) {
	l.Entries = append(l.Entries, entries...)
}

func (l *Log) LastLog() *Entry {
	return l.At(l.Len() - 1)
}

func (l *Log) At(index int) *Entry {
	return &l.Entries[index]
}

func (l *Log) Len() int {
	return len(l.Entries)
}

func (l *Log) Slice(idx int) []Entry {
	return l.Entries[idx:]
}
func (l *Log) Truncate(index int) {
	l.Entries = l.Entries[:index]
}
