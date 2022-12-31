package raft

import "log"

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("AppendEntries")
	defer rf.unlock("AppendEntries")
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if rf.state != FOLLOWER {
		rf.state = FOLLOWER
	}
	Debug(dLog, "S%d receive AppendEntries and reset electionTimer in T%d", rf.me, rf.currentTerm)
	rf.electionTimer.Reset(randomElectionTimeout())
	if args.PrevLogIndex > rf.LastLogIndex() || args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.Success = false
		reply.Term = rf.currentTerm
		// accelerated log backtracking optimization
		if args.PrevLogIndex > rf.LastLogIndex() {
			reply.ConflictTerm = -1
			reply.ConflictIndex = rf.LastLogIndex() + 1
		} else {
			reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
			index := args.PrevLogIndex - 1
			for index > 0 && rf.logs[index].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index + 1
		}
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index > rf.LastLogIndex() || rf.logs[index].Term != entry.Term {
			rf.logs = rf.logs[:index]
			rf.logs = append(rf.logs, append([]LogEntry{}, args.Entries[i:]...)...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs))
		go func() { rf.applierCh <- struct{}{} }()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendsL(heartBeat bool) {
	for i, _ := range rf.peers {
		if i != rf.me {
			if rf.LastLogIndex() > rf.nextIndex[i] || heartBeat {
				rf.sendAppendL(i)
			}
		}
	}
}

func (rf *Raft) sendAppendL(peer int) {
	Debug(dLeader, "S%d sendAppendL to S%d in T%d", rf.me, peer, rf.currentTerm)
	next := rf.nextIndex[peer]
	if next > rf.LastLogIndex()+1 {
		Debug(dLeader, "S%d nextIndex[%d]:%d bigger than lastLogIndex%d", rf.me, peer, rf.nextIndex[peer], rf.LastLogIndex())
		next = rf.LastLogIndex()
	}

	args := &AppendEntriesArgs{rf.currentTerm, rf.me, next - 1, rf.logs[next-1].Term, rf.commitIndex, nil}
	args.Entries = rf.logs[next:]

	go func() {
		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(peer, args, &reply)
		if ok {
			rf.lock("sendAppendL")
			defer rf.unlock("sendAppendL")
			rf.processAppendReplyL(peer, args, &reply)
		}
	}()
}

func (rf *Raft) processAppendReplyL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	Debug(dLeader, "S%d processAppendReply from S%d in T%d", rf.me, peer, rf.currentTerm)
	if reply.Term > rf.currentTerm {
		rf.newTermL(reply.Term)
	} else if rf.currentTerm == args.Term {
		rf.processAppendReplyTermL(peer, args, reply)
	}
}

func (rf *Raft) newTermL(term int) {
	Debug(dLog, "S%v newTerm %v to Follower", rf.me, term)
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.electionTimer.Reset(randomElectionTimeout())
	rf.persist()
}

func (rf *Raft) processAppendReplyTermL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Success {
		newNext := args.PrevLogIndex + len(args.Entries) + 1
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newNext > rf.nextIndex[peer] {
			rf.nextIndex[peer] = newNext
		}
		if newMatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatch
		}
		Debug(dLeader, "S%d processAppendReplyL from S%v reply.term:%v reply.ConflictIndex:%v reply.ConflictTerm:%v reply.Success:%v",
			rf.me, peer, reply.Term, reply.ConflictIndex, reply.ConflictTerm, reply.Success)
	} else {
		index := -1
		found := false
		for i, entry := range rf.logs {
			if entry.Term == reply.ConflictTerm {
				index = i
				found = true
			} else if found {
				break
			}
		}
		if found {
			rf.nextIndex[peer] = index + 1
		} else {
			rf.nextIndex[peer] = reply.ConflictIndex
		}
	}
	rf.advanceCommitL()
}

func (rf *Raft) advanceCommitL() {
	if rf.state != LEADER {
		log.Fatalf("advanceCommit: state %v", rf.state)
	}

	start := rf.commitIndex + 1

	for index := start; index <= rf.LastLogIndex(); index++ {
		if rf.logs[index].Term != rf.currentTerm {
			continue
		}

		n := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] >= index {
				n += 1
			}
		}
		if n > len(rf.peers)/2 {
			Debug(dLeader, "S%v commit %v", rf.me, index)
			rf.commitIndex = index
		}
	}
	go func() { rf.applierCh <- struct{}{} }()
}
