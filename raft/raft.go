package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Leader = iota
	Candidate
	Follower
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
	state          int //0:leader 1: candidata 2: Follower
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
		applyCh:        applyCh,
		state:          Follower,
		heartbeatTimer: time.NewTimer(StableHeartBeatTimeOut()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeOut()),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func StableHeartBeatTimeOut() time.Duration { //固定返回100毫秒
	return time.Duration(1e8) //100ms
}

func RandomizedElectionTimeOut() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(200)+500) * time.Millisecond //从毫秒区间[500,700]中随机选取
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
	Term int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(request *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if request.Term < rf.currentTerm {
		// Reply false if term < currentTerm
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		fmt.Println("server[", rf.me, "] refuse RequestVote to server[", request.CandidateID, "]")
		return
	}

	if request.Term > rf.currentTerm {
		// If RPC request contains term T > currentTerm:
		// set currentTerm = T, convert to follower
		rf.currentTerm = request.Term
		rf.votedFor = -1
		fmt.Println("server[", rf.me, "] receive RequestVote from server[", request.CandidateID, "] and become Follower")
		rf.state = Follower
	}

	if rf.votedFor != -1 && rf.votedFor != request.CandidateID {
		// If votedFor is null or candidateId, grant vote; otherwise reject
		reply.VoteGranted = false
		fmt.Println("server[", rf.me, "] has voted for server[", rf.votedFor, "]  and refuse RequestVote to server[", request.CandidateID, "]")
		reply.Term = rf.currentTerm
		return
	}

	// grant vote to candidate, reset election timer
	rf.electionTimer.Reset(RandomizedElectionTimeOut())
	rf.votedFor = request.CandidateID
	fmt.Println("server[", rf.me, "] votes for server[", rf.votedFor, "]")

	reply.VoteGranted = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(request *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println("server[", rf.me, "] receive AppendEntries in term[", rf.currentTerm, "] from term[", request.Term, "]")
	if request.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm = request.Term
		rf.votedFor = -1
		fmt.Println("server[", rf.me, "] receive AppendEntries become Follower")
		rf.state = Follower
	}
	fmt.Println("server[", rf.me, "] receive AppendEntries and reset electionTime")
	rf.electionTimer.Reset(RandomizedElectionTimeOut())
	reply.Success = true
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
	fmt.Println("server[", rf.me, "] send RequestVote to server[", server, "] in term[", rf.currentTerm, "]")
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	fmt.Println("server[", rf.me, "] send AppendEntries to server[", server, "] in term[", rf.currentTerm, "]")
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) BroadcastAppendEntries() {
	fmt.Println("server[", rf.me, "] begin BroadcastAppendEntries in term[", rf.currentTerm, "]")
	request := rf.genAppendEntriesArgs()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			response := new(AppendEntriesReply)
			rf.sendAppendEntries(peer, request, response)
		}(peer)
	}
	rf.heartbeatTimer.Reset(StableHeartBeatTimeOut())
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			fmt.Println("server:[", rf.me, "] electionTimer in term[", rf.currentTerm, "]")
			if rf.state == Leader {
				rf.mu.Unlock()
				break
			}
			fmt.Println("server[", rf.me, "] become Candidate in term[", rf.currentTerm, "]")
			rf.state = Candidate
			rf.mu.Unlock()

			go rf.startElection()
		case <-rf.heartbeatTimer.C:
			if rf.state == Leader {
				fmt.Println("server[", rf.me, "] send heartbeat in term[", rf.currentTerm, "]")
				rf.BroadcastAppendEntries()
				rf.heartbeatTimer.Reset(StableHeartBeatTimeOut())
			}
		}

	}
}

func (rf *Raft) genRequestVoteRequest() *RequestVoteArgs {
	request := new(RequestVoteArgs)
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	request.CandidateID = rf.me
	request.Term = rf.currentTerm
	return request
}

func (rf *Raft) genAppendEntriesArgs() *AppendEntriesArgs {
	request := new(AppendEntriesArgs)
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	request.Term = rf.currentTerm

	return request
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	fmt.Println("server[", rf.me, "] begin election with term[", rf.currentTerm+1, "]")
	rf.currentTerm++                                    // Increment currentTerm
	rf.votedFor = rf.me                                 // Vote for self
	rf.electionTimer.Reset(RandomizedElectionTimeOut()) // Reset election timer
	rf.mu.Unlock()

	args := RequestVoteArgs{CandidateID: rf.me}
	rf.mu.RLock()
	args.Term = rf.currentTerm
	rf.mu.RUnlock()

	voteCh := make(chan bool, len(rf.peers)-1)
	for i := range rf.peers { // Send RequestVote RPCs to all other servers
		if i == rf.me { // in PARALLEL
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(i, &args, &reply); !ok {
				voteCh <- false
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				// If RPC response contains term T > currentTerm:
				// set currentTerm = T, convert to follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				fmt.Println("server[", rf.me, "] become Follower")
				rf.state = Follower
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			voteCh <- reply.VoteGranted
		}(i)
	}

	voteCnt := 1
	voteGrantedCnt := 1
	for voteGranted := range voteCh {
		rf.mu.RLock()
		state := rf.state
		rf.mu.RUnlock()
		if state != Candidate {
			break
		}
		if voteGranted {
			voteGrantedCnt++
		}
		if voteGrantedCnt > len(rf.peers)/2 {
			// gain over a half votes, switch to leader
			rf.mu.Lock()
			rf.state = Leader
			rf.mu.Unlock()
			fmt.Println("server[", rf.me, "] become leader in term[", rf.currentTerm, "]")
			go rf.BroadcastAppendEntries()
			break
		}

		voteCnt++
		if voteCnt == len(rf.peers) {
			// election completed without getting enough votes, break
			break
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	index := -1
	Term := -1
	isLeader := true

	// Your code here (2B).

	return index, Term, isLeader
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
