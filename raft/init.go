package raft

import (
	"6.824/labrpc"
	"sync"
	"time"
)

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
	currentTerm  int
	votedFor     int
	state        int   //0:leader 1: candidata 2: Follower
	lastApplied  int   //已经被应用到状态机的最高的日志条目的索引
	commitIndex  int   //已知已提交的最高的日志条目的索引
	nextIndex    []int //nextIndex[i] represent the next index of entry that need to sync to peer[i]
	matchIndex   []int //matchIndex[i] represents the highest index that has been successsfully repicated in peer[i]
	logs         []Entry
	applyCh      chan ApplyMsg
	applyCond    *sync.Cond
	electionTime time.Time

	snapshot []byte

	waitingSnapshot []byte
	waitingIndex    int
	waitingTerm     int
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.state = FOLLOWER
	rf.setElectionTime()

	rf.votedFor = -1
	rf.mkLogEmpty()

	rf.readPersist(persister.ReadRaftState())

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = rf.start(), rf.lastindex()+1
	}

	go rf.ticker()
	go rf.applier()

	return rf
}

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

	e := Entry{rf.currentTerm, command}
	index := rf.lastindex() + 1
	rf.append(e)
	rf.persist()

	Debug(dDrop, "S%d new log index=%v, log term=%v, log command=%v", rf.me, index, rf.currentTerm, command)

	rf.sendAppendsL(false)

	return index, rf.currentTerm, true
}

const (
	LEADER = iota
	CANDIDATE
	FOLLOWER
)

const electionTimeout = 300 * time.Millisecond

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
