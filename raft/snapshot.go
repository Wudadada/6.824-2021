package raft

import (
	"6.824/labgob"
	"bytes"
)

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.lock("Snapshot")
	defer rf.unlock("Snapshot")
	if index <= rf.waitingIndex {
		// outdated request
		return
	}
	defer rf.persister.SaveStateAndSnapshot(SerilizeState(rf), snapshot)

	rf.snapshot = snapshot
	rf.waitingTerm = rf.entry(index).Term
	rf.logs = append([]Entry{{Term: rf.waitingTerm}}, rf.logs[rf.logIndex(index)+1:]...)
	rf.waitingIndex = index
	Debug(dSnap, "S%d receive snapshot index:%d, logs:%v in T%d", rf.me, index, rf.logs, rf.currentTerm)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock("InstallSnapshot")
	defer rf.unlock("InstallSnapshot")
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	Debug(dSnap, "S%d receive InstallSnapshot in T%d", rf.me, rf.currentTerm)
	if args.Term > rf.currentTerm {
		rf.newTermL(args.Term)
	}

	rf.setElectionTime()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.waitingIndex || args.LastIncludedIndex <= rf.lastApplied {
		rf.persist()
		return
	}

	defer func() {
		rf.waitingIndex = args.LastIncludedIndex
		rf.waitingTerm = args.LastIncludedTerm
		rf.waitingSnapshot = args.Snapshot

		rf.snapshot = args.Snapshot
		rf.lastApplied = rf.waitingIndex
		rf.commitIndex = max(rf.commitIndex, rf.waitingIndex)

		rf.persister.SaveStateAndSnapshot(SerilizeState(rf), args.Snapshot)
		rf.applyCond.Broadcast()
	}()

	Debug(dTest, "gg")
	for i := 1; i < len(rf.logs); i++ {
		if rf.realIndex(i) == args.LastIncludedIndex && rf.logs[i].Term == args.LastIncludedTerm {
			rf.logs = append([]Entry{{Term: rf.waitingTerm}}, rf.logs[i+1:]...)
			return
		}
	}

	rf.logs = []Entry{{args.LastIncludedTerm, nil}}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

func (rf *Raft) sendSnapshot(peer int) {
	args := &InstallSnapshotArgs{rf.currentTerm, rf.me, rf.waitingIndex, rf.waitingTerm, rf.snapshot}
	lastIncludedIndex := rf.waitingIndex

	go func() {
		var reply InstallSnapshotReply
		ok := rf.sendInstallSnapshot(peer, args, &reply)
		Debug(dSnap, "S%d sendSnapshot to S%d(not sure successful) in T%d", rf.me, peer, rf.currentTerm)
		if ok {
			rf.lock("sendSnapshot")
			defer rf.unlock("sendSnapshot")
			Debug(dSnap, "S%d sendSnapshot to S%d successfully in T%d", rf.me, peer, rf.currentTerm)
			if reply.Term < rf.currentTerm {
				Debug(dTest, "bbbbbbbbbbbbbbbbbbbbbbbbb")
				return
			}
			if reply.Term > rf.currentTerm {
				// get bigger term
				rf.newTermL(reply.Term)
				rf.setElectionTime()
				Debug(dTest, "nnnnnnnnnnnnnnnnnnnnn")
				return
			}

			// check args.Term and curTerm
			if args.Term != rf.currentTerm {
				// outofdate reply!
				Debug(dTest, "mmmmmmmmmmmmmmmmm")
				return
			}

			// when reach hear
			// args.Term == rf.currentTerm == reply.Term
			Debug(dSnap, "S%d Receive Snap Reply from S%d", rf.me, peer)
			if lastIncludedIndex > rf.matchIndex[peer] {
				rf.matchIndex[peer] = lastIncludedIndex
				rf.nextIndex[peer] = lastIncludedIndex + 1
			}
		}
	}()
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
	e.Encode(rf.waitingTerm)
	e.Encode(rf.waitingIndex)
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
	var log []Entry
	var waitingTerm int
	var waitingIndex int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&waitingTerm) != nil || d.Decode(&waitingIndex) != nil {
		panic("readPersist decode fail")
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = log
	rf.waitingTerm = waitingTerm
	rf.waitingIndex = waitingIndex
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
