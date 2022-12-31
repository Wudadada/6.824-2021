package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

func StableHeartBeatTimeOut() time.Duration { //固定返回100毫秒
	return time.Duration(1e8) //100ms
}

func randomElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(200)+500) * time.Millisecond //从毫秒区间[500,700]中随机选取
}

func (rf *Raft) LastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) LastLogIndex() int {
	return len(rf.logs) - 1
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
	rf.electionTimer.Reset(randomElectionTimeout())
	rf.persist()
}

func min(x int, y int) int {
	if x >= y {
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
