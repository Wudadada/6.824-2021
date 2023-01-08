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
	"6.824/labgob"
	"bytes"
	"math/rand"
	"sync/atomic"
	"time"
)

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//fmt.Println("server[", rf.me, "] send RequestVote to server[", server, "] in term[", rf.currentTerm, "]")
	//Debug(dVote, "S%d send RequestVote to S% in T%d", rf.me, server, rf.currentTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Println("server[", rf.me, "] send AppendEntries to server[", server, "] in term[", rf.currentTerm, "]")
	//Debug(dVote, "S%d send AppendEntries to S% in T%d", rf.me, server, rf.currentTerm)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) tick() {
	rf.lock("tick")
	defer rf.unlock("tick")

	if rf.state == LEADER {
		Debug(dLeader, "S%d reset election time and send heart beat in T%d", rf.me, rf.currentTerm)
		rf.setElectionTime()
		rf.sendAppendsL(true)
	}
	if time.Now().After(rf.electionTime) {
		rf.setElectionTime()
		Debug(dLog, "S%d reset election time and start election in T%d", rf.me, rf.currentTerm+1)
		rf.startElectionL()
	}
}

func (rf *Raft) setElectionTime() {
	t := time.Now()
	t = t.Add(electionTimeout)
	ms := rand.Int63() % 200
	t = t.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = t
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.tick()
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	rf.lock("applier")
	defer rf.unlock("applier")

	for rf.killed() == false {
		if rf.lastApplied+1 <= rf.commitIndex && rf.lastApplied+1 <= rf.LastLogIndex() {
			rf.lastApplied += 1
			am := ApplyMsg{}
			am.CommandValid = true
			am.CommandIndex = rf.lastApplied
			am.Command = rf.logs[rf.lastApplied].Command
			Debug(dLog, "S%d apply commandindex:%d", rf.me, am.CommandIndex)
			rf.unlock("applier")
			rf.applyCh <- am
			rf.lock("applier")
		} else {
			rf.applyCond.Wait()
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == LEADER
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		panic("readPersist decode fail")
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = log
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

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
