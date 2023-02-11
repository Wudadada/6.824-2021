package raft

import (
	"6.824/labgob"
	"bytes"
	"math/rand"
	"sync/atomic"
	"time"
)

type Log []Entry

type Entry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) setElectionTime() {
	t := time.Now()
	t = t.Add(electionTimeout)
	ms := rand.Int63() % 200
	t = t.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = t
}

func SerilizeState(rf *Raft) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.waitingTerm)
	e.Encode(rf.waitingIndex)
	return w.Bytes()
}

// switch real index to log index
func (rf *Raft) logIndex(realIndex int) int {
	return realIndex - rf.waitingIndex
}

// switch log index to real index
func (rf *Raft) realIndex(logIndex int) int {
	return logIndex + rf.waitingIndex
}

func (rf *Raft) mkLogEmpty() {
	rf.logs = []Entry{{Term: rf.waitingTerm}}
}

func (rf *Raft) append(e Entry) {
	rf.logs = append(rf.logs, e)
}

func (rf *Raft) start() int {
	return rf.waitingIndex
}

func (rf *Raft) lastindex() int {
	return rf.waitingIndex + len(rf.logs) - 1
}

func (rf *Raft) entry(index int) *Entry {
	return &(rf.logs[index-rf.waitingIndex])
}

func (rf *Raft) lastentry() *Entry {
	return rf.entry(rf.lastindex())
}

func (rf *Raft) lock(where string) {
	//Debug(dLock, "S%d locked %s", rf.me, where)
	rf.mu.Lock()
}

func (rf *Raft) unlock(where string) {
	//Debug(dLock, "S%d unlocked %s", rf.me, where)
	rf.mu.Unlock()
}

func (rf *Raft) rlock(where string) {
	//Debug(dLock, "S%d rlocked %s", rf.me, where)
	rf.mu.RLock()
}

func (rf *Raft) runlock(where string) {
	//Debug(dLock, "S%d runlocked %s", rf.me, where)
	rf.mu.RUnlock()
}

func (rf *Raft) newTermL(term int) {
	Debug(dLog, "S%v newTerm %v to Follower", rf.me, term)
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.persist()
}

func min(x int, y int) int {
	if x >= y {
		return y
	}
	return x
}

func max(x int, y int) int {
	if x <= y {
		return y
	}
	return x
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

}

/*
	defer func() {
		if r := recover(); r != nil {
			// 处理越界错误
			fmt.Println("BroadcastAppendEntries数组越界")
		}
	}()
*/

// python3 dstest.py xx -p 100 -n 100  -v
//python3 dslogs.py 20230125_110814/TestSnapshotInstall2D_0.log -c 3
//VERBOSE=1 go test -run TestSnapshotInstall2D | python3 dslogs.py -c 3
