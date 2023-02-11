package raft

func (rf *Raft) requestVotesL() {
	args := &RequestVoteArgs{rf.currentTerm, rf.me, rf.lastindex(), rf.lastentry().Term}
	votes := 1
	for i, _ := range rf.peers {
		if rf.state != CANDIDATE {
			return
		}
		if i != rf.me {
			go rf.requestVote(i, args, &votes)
		}
	}
}

func (rf *Raft) requestVote(peer int, args *RequestVoteArgs, votes *int) {
	var reply RequestVoteReply
	ok := rf.sendRequestVote(peer, args, &reply)
	if ok {
		rf.lock("requestVote")
		defer rf.unlock("requestVote")

		if rf.currentTerm != args.Term || rf.state != CANDIDATE {
			return
		}

		Debug(dVote, "S%d processAppendReplyL from S%v reply.term:%v reply.VoteGranted%v",
			rf.me, peer, reply.Term, reply.VoteGranted)
		if reply.Term > rf.currentTerm {
			rf.newTermL(reply.Term)
		}
		if reply.VoteGranted {
			*votes += 1
			if *votes > len(rf.peers)/2 {
				if rf.currentTerm == args.Term {
					rf.becomeLeaderL()
					rf.sendAppendsL(true)
				}
			}
		}
	}
}

func (rf *Raft) becomeLeaderL() {
	Debug(dLeader, "S%d becomeLeader in T%v", rf.me, rf.currentTerm)
	rf.state = LEADER
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastindex() + 1
	}
}

func (rf *Raft) startElectionL() {
	rf.currentTerm += 1
	rf.state = CANDIDATE

	rf.votedFor = rf.me
	rf.persist()

	Debug(dVote, "S%d startElection in T%v", rf.me, rf.currentTerm)

	rf.requestVotesL()
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.lock("RequestVote")
	defer rf.unlock("RequestVote")

	if args.Term > rf.currentTerm {
		rf.newTermL(args.Term)
	}

	myIndex := rf.lastindex()
	myTerm := rf.entry(myIndex).Term
	uptodate := (args.LastLogTerm == myTerm && args.LastLogIndex >= myIndex) || args.LastLogTerm > myTerm

	Debug(dLog, "S%d RequestVote args%v reply%v uptodate:%v (myIndex %v, myTerm %v)", rf.me, args, reply, uptodate, myIndex, myTerm)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && uptodate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.persist()
		Debug(dLog, "S%d vote for S%d in T%d", rf.me, args.CandidateID, args.Term)
		rf.setElectionTime()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//fmt.Println("server[", rf.me, "] send RequestVote to server[", server, "] in term[", rf.currentTerm, "]")
	//Debug(dVote, "S%d send RequestVote to S% in T%d", rf.me, server, rf.currentTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
