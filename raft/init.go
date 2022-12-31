package raft

import (
	"6.824/labrpc"
	"sync"
	"time"
)

const (
	LEADER = iota
	CANDIDATE
	FOLLOWER
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm    int
	votedFor       int
	state          int   //0:leader 1: candidata 2: Follower
	lastApplied    int   //已经被应用到状态机的最高的日志条目的索引
	commitIndex    int   //已知已提交的最高的日志条目的索引
	nextIndex      []int //nextIndex[i] represent the next index of entry that need to sync to peer[i]
	matchIndex     []int //matchIndex[i] represents the highest index that has been successsfully repicated in peer[i]
	logs           []LogEntry
	applierCh      chan struct{}
	applyCh        chan ApplyMsg
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		currentTerm:    0,
		votedFor:       -1,
		dead:           0,
		lastApplied:    0,
		commitIndex:    0,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		logs:           make([]LogEntry, 0),
		applyCh:        applyCh,
		applierCh:      make(chan struct{}),
		state:          FOLLOWER,
		heartbeatTimer: time.NewTimer(StableHeartBeatTimeOut()),
		electionTimer:  time.NewTimer(randomElectionTimeout()),
	}
	rf.logs = append(rf.logs,
		LogEntry{
			Index:   0,
			Term:    0,
			Command: nil,
		},
	)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, 1
	}

	go rf.ticker()
	go rf.applier()

	return rf
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int //紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int //紧邻新日志条目之前的那个日志条目的任期
	LeaderCommit int //领导者的已知已提交的最高的日志条目的索引
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
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
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.lock("start")
	defer rf.unlock("start")
	// Your code here (2B).
	if rf.state != LEADER {
		return -1, -1, false
	}
	/*	defer func() {
		if r := recover(); r != nil {
			// 处理越界错误
			fmt.Println("Start数组越界")
		}
	}()*/
	term := rf.currentTerm
	index := len(rf.logs)
	rf.logs = append(rf.logs,
		LogEntry{
			Index:   index,
			Term:    term,
			Command: command,
		},
	)

	isLeader := true
	Debug(dDrop, "S%d new log index=%v, log term=%v, log command=%v", rf.me, index, term, command)

	rf.sendAppendsL(false)

	return index, term, isLeader
}
