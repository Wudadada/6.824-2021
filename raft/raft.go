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
	"fmt"
	"sync/atomic"
	"time"
)

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(request *RequestVoteArgs, reply *RequestVoteReply) {
	rf.lock("RequestVote")
	defer rf.unlock("RequestVote")
	defer func() {
		if r := recover(); r != nil {
			// 处理越界错误
			fmt.Println("RequestVote数组越界")
		}
	}()
	if request.Term < rf.currentTerm {
		// Reply false if term < currentTerm
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		//fmt.Println("server[", rf.me, "] refuse RequestVote to server[", request.CandidateID, "]")
		Debug(dVote, "S%d refuse RequestVote to S%d", rf.me, request.CandidateID)

		return
	}

	if request.Term > rf.currentTerm {
		// If RPC request contains term T > currentTerm:
		// set currentTerm = T, convert to follower
		rf.currentTerm = request.Term
		rf.votedFor = -1
		//fmt.Println("server[", rf.me, "] receive RequestVote from server[", request.CandidateID, "] and become Follower")
		Debug(dVote, "S%d receive RequestVote from S%d and become Follower", rf.me, request.CandidateID)
		rf.state = FOLLOWER
	}

	if rf.votedFor != -1 && rf.votedFor != request.CandidateID {
		// If votedFor is null or candidateId, grant vote; otherwise reject
		reply.VoteGranted = false
		//fmt.Println("server[", rf.me, "] has voted for server[", rf.votedFor, "]  and refuse RequestVote to server[", request.CandidateID, "]")
		Debug(dVote, "S%d has voted for S%d and refuse to RequestVote to S%d", rf.me, rf.votedFor, request.CandidateID)
		reply.Term = rf.currentTerm
		return
	}

	lastLogIndex := len(rf.logs) - 1
	if lastLogIndex != -1 && (rf.logs[lastLogIndex].Term > request.LastLogTerm || (rf.logs[lastLogIndex].Term == request.LastLogTerm && rf.logs[lastLogIndex].Index > request.LastLogIndex)) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// grant vote to candidate, reset election timer
	rf.electionTimer.Reset(randomElectionTimeout())
	rf.votedFor = request.CandidateID
	//fmt.Println("server[", rf.me, "] votes for server[", rf.votedFor, "]")

	reply.VoteGranted = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(request *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("AppendEntries")
	defer rf.unlock("AppendEntries")
	/*	defer func() {
		if r := recover(); r != nil {
			// 处理越界错误
			fmt.Println("AppendEntries数组越界")
		}
	}()*/
	//fmt.Println("server[", rf.me, "] receive AppendEntries in term[", rf.currentTerm, "] from term[", request.Term, "]")
	Debug(dLog, "S%d receive AppendEntries in T%d from T%d", rf.me, rf.currentTerm, request.Term)
	if request.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm = request.Term
		rf.votedFor = -1
		//fmt.Println("server[", rf.me, "] receive AppendEntries become Follower")
		Debug(dLog, "S%d receive AppendEntries and become Follower", rf.me)
		rf.state = FOLLOWER
	}
	Debug(dLog, "S%d receive AppendEntries and reset electionTime", rf.me)
	rf.electionTimer.Reset(randomElectionTimeout())

	if len(request.Entries) == 0 { //如果是心跳则返回
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}

	lastLogIndex := len(rf.logs) - 1                                                                                                            //日志一致性检查
	if lastLogIndex != 0 && (request.PrevLogIndex > rf.logs[lastLogIndex].Index || rf.logs[request.PrevLogIndex].Term != request.PrevLogTerm) { //缺少日志或任期不同：都是相同的处理（request减小PrevLogIndex)
		reply.Success = false
		Debug(dLog, "S%d 日志一致性检查失败", rf.me)
		return
	}

	reply.Success = true

	index := lastLogIndex
	for i, entry := range request.Entries { //补充缺少的日志、删除多余的日志
		index++
		if index < len(rf.logs) { //删除多余日志
			if rf.logs[index].Term == entry.Term {
				continue
			}

			rf.logs = rf.logs[:index]
		}

		rf.logs = append(rf.logs, request.Entries[i:]...) //补充缺少日志
		break
	}

	if rf.commitIndex < request.LeaderCommit {
		lastLogIndex = rf.getLastLogIndex()
		if request.LeaderCommit > lastLogIndex {
			rf.commitIndex = lastLogIndex
		} else {
			rf.commitIndex = request.LeaderCommit
		}
		rf.applierCh <- struct{}{}
	}

	rf.electionTimer.Reset(randomElectionTimeout())
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

func (rf *Raft) BroadcastAppendEntries(isHeartBeat bool) {
	//fmt.Println("server[", rf.me, "] begin BroadcastAppendEntries in term[", rf.currentTerm, "]")
	Debug(dLeader, "S%d start BoardcastAppendEntries int T%d, is %t heartbeat", rf.me, rf.currentTerm, isHeartBeat)
	request := rf.genHeartBeatAppendEntriesArgs()
	/*	defer func() {
		if r := recover(); r != nil {
			// 处理越界错误
			fmt.Println("BroadcastAppendEntries数组越界")
		}
	}()*/
	if isHeartBeat {
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}

			go func(peer int) {
				response := new(AppendEntriesReply)
				Debug(dLeader, "S%d send heartBeat to S%d in T%d", rf.me, peer, rf.currentTerm)
				rf.sendAppendEntries(peer, request, response)
			}(peer)

		}
	} else { //不是心跳，需要分别复制日志
		totalPeers := len(rf.peers)
		successPeers := 1

		Debug(dTest, "S%d bf for peer", rf.me)
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}

			Debug(dTest, "S%d bf gofunc", peer)

			//发送AppendEntries,如果日志一致性检查失败（缺少日志），重新发送,直到Follower返回success
			response := new(AppendEntriesReply)
			Debug(dTest, "S%d gofunc begin", peer)
			rf.rlock("BroadcastAppendEntries")
			request.PrevLogIndex = rf.nextIndex[peer] - 1
			request.PrevLogTerm = rf.logs[request.PrevLogIndex].Term
			if rf.nextIndex[peer] <= rf.getLastLogIndex() {
				request.Entries = rf.logs[rf.nextIndex[peer]:]
			}
			Debug(dDrop, "S%d -> S%d send Entries index:%d  in T%d", rf.me, peer, rf.nextIndex[peer], rf.currentTerm)
			rf.runlock("BroadcastAppendEntries")

			rf.sendAppendEntries(peer, request, response)

			for response.Success == false && len(request.Entries) != 0 { //如果是日志复制失败
				rf.nextIndex[peer]-- //需要发送的日志位置向前移动一位

				//重新生成需要发送的日志
				request.PrevLogIndex = rf.nextIndex[peer]
				var tmpEntry LogEntry
				request.Entries = append(request.Entries, tmpEntry)
				copy(request.Entries[1:], request.Entries[0:])
				tmpEntry = rf.logs[rf.nextIndex[peer]]
				request.Entries[0] = tmpEntry

				Debug(dLeader, "S%d -> S%d retry Entries index:%d in T%d", rf.me, peer, rf.nextIndex[peer], rf.currentTerm)

				rf.sendAppendEntries(peer, request, response)
			}

			rf.lock("BroadcastAppendEntries")
			if response.Success {
				successPeers++
				Debug(dTest, "S%d response true", peer)
				rf.nextIndex[peer] = rf.getLastLogIndex() + 1
				rf.matchIndex[peer] = rf.nextIndex[peer] - 1
				Debug(dLeader, "S%d  T%d BoardcastAppendEntries, response and nextIndex[%d]:%d, matchIndex[%d]:%d ", rf.me, rf.currentTerm, peer, rf.nextIndex[peer], peer, rf.matchIndex[peer])
			}
			rf.unlock("BroadcastAppendEntries")
			Debug(dTest, "S%d gofunc end", peer)

			Debug(dTest, "S%d aft gofunc", peer)
		}
		Debug(dTest, "S%d aft for peer", rf.me)

		Debug(dTest, "S%d successPeers:%d in T%d", rf.me, successPeers, rf.currentTerm)
		if successPeers > totalPeers/2 {
			rf.applierCh <- struct{}{}
		}
	}

	Debug(dLeader, "S%d reset heartbeatTimer in T%d", rf.me, rf.currentTerm)
	rf.heartbeatTimer.Reset(StableHeartBeatTimeOut())
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.lock("ticker electionTimer")
			//fmt.Println("server:[", rf.me, "] electionTimer in term[", rf.currentTerm, "]")
			Debug(dTimer, "S%d electionTimer in T%d", rf.me, rf.currentTerm)
			if rf.state == LEADER {
				rf.mu.Unlock()
				break
			}
			Debug(dVote, "S%d become candidate in T%d", rf.me, rf.currentTerm)
			//fmt.Println("server[", rf.me, "] become Candidate in term[", rf.currentTerm, "]")
			rf.state = CANDIDATE
			rf.unlock("ticker electionTimer")
			go rf.startElection()
		case <-rf.heartbeatTimer.C:
			if rf.state == LEADER {
				//fmt.Println("server[", rf.me, "] send heartbeat in term[", rf.currentTerm, "]")
				Debug(dLeader, "S%d heartbeatTimer and begin Boardcastheartbeat in T%d and reset heartbeat timer", rf.me, rf.currentTerm)
				rf.BroadcastAppendEntries(true)
				rf.heartbeatTimer.Reset(StableHeartBeatTimeOut())
			}
		case <-rf.applierCh:
			Debug(dDrop, "S%d applierCh in T%d", rf.me, rf.currentTerm)
			rf.lock("applier")
			if rf.lastApplied >= rf.commitIndex {
				rf.unlock("applier")
				break
			}
			lastApplied := rf.lastApplied
			entries := append([]LogEntry{}, rf.logs[lastApplied+1:rf.commitIndex+1]...)
			rf.lastApplied = rf.commitIndex
			rf.unlock("applier")

			for i, entry := range entries {
				command := entry.Command
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      command,
					CommandIndex: lastApplied + i + 1,
				}
				Debug(dDrop, "S%d, apply log, log index=%v, log term=%v, log command=%v", rf.me, entry.Index, entry.Term, command)
			}

		}
		time.Sleep(10 * time.Millisecond)
	}
}

/*func (rf *Raft) genRequestVoteRequest() *RequestVoteArgs {
	request := new(RequestVoteArgs)
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	request.CandidateID = rf.me
	request.Term = rf.currentTerm
	return request
}*/

func (rf *Raft) genHeartBeatAppendEntriesArgs() *AppendEntriesArgs {
	request := new(AppendEntriesArgs)
	rf.mu.RLock()
	request.LeaderCommit = rf.commitIndex
	request.PrevLogTerm = rf.getLastLog().Term
	request.PrevLogIndex = rf.getLastLog().Index
	request.LeaderId = rf.me
	request.Term = rf.currentTerm
	rf.mu.RUnlock()

	return request
}

func (rf *Raft) startElection() {
	defer func() {
		if r := recover(); r != nil {
			// 处理越界错误
			fmt.Println("startElection数组越界")
		}
	}()
	rf.lock("startElection()")
	//fmt.Println("server[", rf.me, "] begin election with term[", rf.currentTerm+1, "]")
	Debug(dVote, "S%d startElection() in T%d", rf.me, rf.currentTerm)
	rf.currentTerm++    // Increment currentTerm
	rf.votedFor = rf.me // Vote for self
	Debug(dVote, "S%d startElection() and reset election timer in T%d", rf.me, rf.currentTerm)
	rf.electionTimer.Reset(randomElectionTimeout()) // Reset election timer
	rf.unlock("startElection()")

	rf.rlock("startElection()")
	var args RequestVoteArgs
	args.CandidateID = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex = len(rf.logs) - 1
	if args.LastLogIndex >= 0 {
		args.LastLogTerm = rf.logs[args.LastLogIndex].Term
	} else {
		args.LastLogTerm = rf.currentTerm
	}
	rf.runlock("startElection()")

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
			rf.lock("startElection()")
			if reply.Term > rf.currentTerm {
				// If RPC response contains term T > currentTerm:
				// set currentTerm = T, convert to follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				//fmt.Println("server[", rf.me, "] become Follower")
				Debug(dVote, "S%d become follower during its elecition and change term to T%d", rf.me, rf.currentTerm)
				rf.state = FOLLOWER
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
		state := rf.state
		if state != CANDIDATE {
			break
		}
		if voteGranted {
			voteGrantedCnt++
		}
		if voteGrantedCnt > len(rf.peers)/2 {
			// gain over a half votes, switch to leader
			rf.state = LEADER
			rf.lock("voteGrantedCnt")
			for i := 0; i < len(rf.peers); i++ {
				// reinitialize upon winning the election
				rf.nextIndex[i] = len(rf.logs)
				rf.matchIndex[i] = 0
			}
			rf.unlock("voteGrantedCnt")
			Debug(dLeader, "S%d become Leader in T%d and BroadHeartBeat", rf.me, rf.currentTerm)
			go rf.BroadcastAppendEntries(true)
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

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
