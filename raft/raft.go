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
	"sync/atomic"
	"time"
)

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

	rf.lastApplied = 0

	if rf.lastApplied+1 <= rf.start() {
		rf.lastApplied = rf.start()
	}

	for rf.killed() == false {
		if rf.waitingSnapshot != nil {
			am := ApplyMsg{}

			am.SnapshotValid = true
			am.Snapshot = rf.waitingSnapshot
			am.SnapshotIndex = rf.waitingIndex
			am.SnapshotTerm = rf.waitingTerm

			rf.waitingSnapshot = nil

			Debug(dCommit, "S%d apply Snapshot, now logs:%v in T%d", rf.me, rf.logs, rf.currentTerm)
			rf.unlock("applier")
			rf.applyCh <- am
			rf.lock("applier")
		} else if rf.lastApplied+1 <= rf.commitIndex && rf.lastApplied+1 <= rf.lastindex() && rf.lastApplied+1 > rf.start() {
			rf.lastApplied += 1
			am := ApplyMsg{}
			am.CommandValid = true
			am.CommandIndex = rf.lastApplied
			am.Command = rf.entry(rf.lastApplied).Command
			Debug(dCommit, "S%d apply command:%v commandindex:%d, now logs:%v in T%d", rf.me, am.Command, am.CommandIndex, rf.logs, rf.currentTerm)
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

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
