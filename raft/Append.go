package raft

import "log"

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("AppendEntries")
	defer rf.unlock("AppendEntries")
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	if rf.state != FOLLOWER {
		rf.state = FOLLOWER
	}

	Debug(dLog, "S%d receive AppendEntries, args.LeaderCommit:%d, args.PrevLogIndex:%d, args.PrevLogTerm:%d, args.Entries:%v in T%d", rf.me, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, args.Entries, rf.currentTerm)
	rf.setElectionTime()

	if args.PrevLogIndex > rf.lastindex() || args.PrevLogIndex > rf.start() && rf.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.Success = false
		reply.Term = rf.currentTerm
		Debug(dInfo, "S%d lastIndex:%d in T%d", rf.me, rf.lastindex(), rf.currentTerm)
		// accelerated log backtracking optimization
		if args.PrevLogIndex > rf.lastindex() {
			reply.ConflictTerm = -1
			reply.ConflictIndex = rf.lastindex() + 1
			reply.ConflictValid = false
		} else { //找到term为args.PrevLogIndex.Term的最小index
			reply.ConflictValid = true
			reply.ConflictTerm = rf.entry(args.PrevLogIndex).Term
			index := args.PrevLogIndex - 1
			for index > rf.waitingIndex && rf.entry(index).Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index + 1
		}
		Debug(dInfo, "S%d waitingIndex:%d, waitingTerm:%d conflictTerm:%d, conflictIndex:%d, conflictValid:%v in T%d", rf.me, rf.waitingIndex, rf.waitingTerm, reply.ConflictTerm, reply.ConflictIndex, reply.ConflictValid, rf.currentTerm)
		//Debug(dInfo, "S%d waitingIndex:%d, waitingTerm:%d conflictTerm:%d, conflictIndex:%d, conflictValid:%v logs:%v in T%d", rf.me, rf.waitingIndex, rf.waitingTerm, reply.ConflictTerm, reply.ConflictIndex, reply.ConflictValid, rf.logs, rf.currentTerm)
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index > rf.lastindex() || (index > rf.start() && rf.entry(index).Term != entry.Term) {
			rf.logs = rf.logs[:rf.logIndex(index)]
			rf.logs = append(rf.logs, append([]Entry{}, args.Entries[i:]...)...)
			//Debug(dLog, "S%d cut logs begin at index:%d, now logs:%v in T%d", rf.me, index, rf.logs, rf.currentTerm)
			Debug(dLog, "S%d cut logs begin at index:%d, now logs:%v in T%d", rf.me, index, rf.logs, rf.currentTerm)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		rf.commitIndex = min(args.LeaderCommit, rf.lastindex())
		Debug(dCommit, "S%d update commitIndex to %d in T%d", rf.me, rf.commitIndex, rf.currentTerm)
		rf.applyCond.Broadcast()
	}
	rf.persist()

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendsL(heartBeat bool) {
	for i, _ := range rf.peers {
		if rf.state != LEADER {
			return
		}
		if i != rf.me {
			if rf.lastindex() > rf.nextIndex[i] || heartBeat {
				rf.sendAppendL(i)
			}
		}
	}
}

func (rf *Raft) sendAppendL(peer int) {
	Debug(dLeader, "S%d sendAppendL to S%d, logs:%v in T%d", rf.me, peer, rf.logs, rf.currentTerm)
	next := rf.nextIndex[peer]
	if next <= rf.start() {
		next = rf.start() + 1
	}

	if next > rf.lastindex()+1 {
		Debug(dLeader, "S%d nextIndex[%d]:%d bigger than lastLogIndex%d", rf.me, peer, rf.nextIndex[peer], rf.lastindex())
		next = rf.lastindex()
	}

	//Debug(dInfo, "next:%d, rf.logIndex(next-1) = (next - 1)(%d) - rf.waitingIndex(%d) = %d", next, next-1, rf.waitingIndex, rf.logIndex(next-1))
	args := &AppendEntriesArgs{rf.currentTerm, rf.me, next - 1, rf.logs[rf.logIndex(next-1)].Term, rf.commitIndex, nil}
	args.Entries = rf.logs[rf.logIndex(next):]

	go func() {
		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(peer, args, &reply)
		if ok {
			rf.lock("sendAppendL")
			defer rf.unlock("sendAppendL")
			if rf.currentTerm != args.Term || rf.state != LEADER {
				return
			}
			rf.processAppendReplyL(peer, args, &reply)
		}
	}()
}

func (rf *Raft) processAppendReplyL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	Debug(dLeader, "S%d processAppendReply from S%d, reply.Term:%d, reply.ConflictTerm:%d, reply.ConflictIndex:%d, reply.Success:%v, reply.ConflictValid:%v in T%d", rf.me, peer,
		reply.Term, reply.ConflictTerm, reply.ConflictIndex, reply.Success, reply.ConflictValid, rf.currentTerm)
	if reply.Term > rf.currentTerm {
		rf.newTermL(reply.Term)
	} else if rf.currentTerm == args.Term {
		rf.processAppendReplyTermL(peer, args, reply)
	}
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
	} else if reply.ConflictValid {
		Debug(dLeader, "S%d processAppendReplyL from S%v reply.ConflictValid is true and processConflictTermL",
			rf.me, peer)
		rf.processConflictTermL(peer, args, reply)
	} else if rf.nextIndex[peer] >= 1 {
		if rf.nextIndex[peer] > 1 {
			rf.nextIndex[peer] -= 1
		}
		Debug(dLeader, "S%d processAppendReplyTermL rf.nextIndex[%d]:%d",
			rf.me, peer, rf.nextIndex[peer])
		if rf.nextIndex[peer] < rf.start()+1 {
			Debug(dSnap, "S%d processAppendReplyTermL sendSnapshot to S%d",
				rf.me, peer)
			rf.sendSnapshot(peer)
		}
	}
	rf.advanceCommitL()
}

// 如果peer的ConlictTerm存在，rf的该ConlictTerm也存在，nextIndex[peer] = 最后一个ConlictTerm的下一个index
// 如果peer的ConlictTerm存在，rf的该ConlictTerm不存在，nextIndex[peer] = peer第一个为ConlictTerm的 index
// 如果peer的ConlictTerm不存在，nextIndex[peer] = peer的最后一个index + 1
func (rf *Raft) processConflictTermL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.ConflictIndex <= rf.waitingIndex {
		Debug(dSnap, "S%d processConflictTermL reply.ConflictIndex <= rf.waitingIndex and sendSnapshot to S%d",
			rf.me, peer)
		rf.sendSnapshot(peer)
		return
	}
	index := -1
	found := false
	for i, entry := range rf.logs {
		if entry.Term == reply.ConflictTerm {
			index = rf.realIndex(i)
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
	Debug(dLeader, "S%d processConflictTermL from S%v reply.term:%v reply.ConflictIndex:%v reply.ConflictTerm:%v reply.ConflictValid:%v, set nextIndex[%v] to %v in T%v",
		rf.me, peer, reply.Term, reply.ConflictIndex, reply.ConflictTerm, reply.ConflictValid, peer, rf.nextIndex[peer], rf.currentTerm)
}

func (rf *Raft) advanceCommitL() {
	if rf.state != LEADER {
		log.Fatalf("advanceCommit: state %v", rf.state)
	}

	start := rf.commitIndex + 1
	if start < rf.start() {
		start = rf.start()
	}

	for index := start; index <= rf.lastindex(); index++ {
		if rf.entry(index).Term != rf.currentTerm {
			continue
		}

		n := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] >= index {
				n += 1
			}
		}
		if n > len(rf.peers)/2 {
			Debug(dCommit, "S%v commit %v", rf.me, index)
			rf.commitIndex = index
		}
	}

	Debug(dCommit, "S%d advanceCommitL applyCond.Broadcast() in T%d", rf.me, rf.currentTerm)
	rf.applyCond.Broadcast()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int //紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int //紧邻新日志条目之前的那个日志条目的任期
	LeaderCommit int //领导者的已知已提交的最高的日志条目的索引
	Entries      []Entry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
	ConflictValid bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Println("server[", rf.me, "] send AppendEntries to server[", server, "] in term[", rf.currentTerm, "]")
	//Debug(dVote, "S%d send AppendEntries to S% in T%d", rf.me, server, rf.currentTerm)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
