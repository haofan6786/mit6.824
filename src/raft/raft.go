package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type State string

const (
	Follower  State = "follower"
	Candidate       = "candidate"
	Leader          = "Leader"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	snapshot  []byte
	me        int   // this peer's index into peers[]
	dead      int32 // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	state       State
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastReceiveTime time.Time //超时判断
	overTime        int

	applyChan chan ApplyMsg
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == "Leader"
	return term, isleader
}

//with lock
func (rf *Raft) GetTargetLogIndex(index int) int {
	startIndex := rf.logs[0].Index
	return index - startIndex
}

//with lock
func (rf *Raft) GetLastLogIndex() int {
	return rf.logs[len(rf.logs)-1].Index
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
	//rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
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
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		DPrintf("[%d-%v-%d-%d]persist Error", rf.me, rf.state, rf.currentTerm, len(rf.logs))
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
	rf.lastApplied = rf.logs[0].Index
	rf.commitIndex = rf.logs[0].Index
	DPrintf("[%d-%v-%d-%d] readPersist", rf.me, rf.state, rf.currentTerm, len(rf.logs))
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//

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

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() //因为修改了log所以要进行persist
	if rf.logs[0].Term > lastIncludedTerm || (rf.logs[0].Term == lastIncludedTerm && rf.logs[0].Index >= lastIncludedIndex) {
		return false
	}
	//fmt.Printf("%d,%v\n", lastIncludedIndex, rf.logs)
	DPrintf("[%d-%v-%d-%d] cond install snapshot, lastIncludedIndex=%d", rf.me, rf.state, rf.currentTerm, len(rf.logs), lastIncludedIndex)
	DPrintf("[%d-%v-%d-%d] old logs:%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.logs)
	rf.logs[0].Index = lastIncludedIndex
	rf.logs[0].Term = lastIncludedTerm
	rf.logs = []LogEntry{rf.logs[0]}
	rf.snapshot = snapshot
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	DPrintf("[%d-%v-%d-%d] new logs:%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.logs)
	return true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	DPrintf("[%d-%v-%d-%d] receive installSnapshot from args %v", rf.me, rf.state, rf.currentTerm, len(rf.logs), args)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.toFollower(args.Term)
	rf.reSetTimer()
	if args.LastIncludedIndex < rf.logs[0].Index {
		rf.mu.Unlock()
		return
	}
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	DPrintf("[%d-%v-%d-%d] Install Snapshot %v", rf.me, rf.state, rf.currentTerm, len(rf.logs), msg)
	rf.mu.Unlock()
	rf.applyChan <- msg
	//DPrintf("[%d-%v-%d-%d] Install Snapshot success", rf.me, rf.state, rf.currentTerm, len(rf.logs))
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// 通知server状态机生成了快照，server需要抛弃旧日志
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() //因为修改了log所以要进行persist
	DPrintf("[%d-%v-%d-%d] snapshot, index=%d, snapshot=%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), index, snapshot)
	DPrintf("[%d-%v-%d-%d] old logs:%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.logs)
	logIndex := rf.GetTargetLogIndex(index)
	rf.logs[0].Index = index
	rf.logs[0].Term = rf.logs[logIndex].Term
	rf.logs = append([]LogEntry{rf.logs[0]}, rf.logs[logIndex+1:]...)
	rf.snapshot = snapshot
	DPrintf("[%d-%v-%d-%d] new logs:%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.logs)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //2a
	CandidateId  int //2a
	LastLogIndex int //2b
	LastLogTerm  int //2b
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("[%d-%v-%d-%d] received request vote from %v", rf.me, rf.state, rf.currentTerm, len(rf.logs), args)
	//DPrintf("%d,%d", rf.votedFor, args.CandidateId)
	reply.Term = rf.currentTerm //让candidate更新自己
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		//term太小，不投
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}
	//voteFor为空,进行election restriction 检查
	if args.LastLogTerm > rf.logs[len(rf.logs)-1].Term ||
		(args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= rf.GetLastLogIndex()) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		DPrintf("[%d-%v-%d-%d] vote to %d", rf.me, rf.state, rf.currentTerm, len(rf.logs), args.CandidateId)
		rf.reSetTimer() //投了票才会重置timer
		return
	}
}

type AppendEntriesArgs struct {
	Term         int        // leader的任期号
	LeaderId     int        // leaderID 便于进行重定向
	PrevLogTerm  int        // 新日志之前日志的Term
	PrevLogIndex int        // 新日志之前日志的索引值
	Entries      []LogEntry // 存储的日志条目 为空时是心跳包
	LeaderCommit int        // leader已经提交的日志的索引
}
type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

//with lock
func (rf *Raft) toFollower(term int) {
	if rf.state != Follower || rf.currentTerm != term {
		DPrintf("[%d-%v-%d-%d]-%v:%d become follower:%d", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.state, rf.currentTerm, term)
	}
	rf.state = Follower
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term
}

//with lock
func (rf *Raft) toLeader() {
	//DPrintf("[%d-%v-%d-%d] become leader", rf.me,rf.state,rf.currentTerm,len(rf.logs))
	DPrintf("[%d-%v-%d-%d] become leader term=%d,log=%v\n", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.currentTerm, rf.logs)
	rf.state = Leader
	for index, _ := range rf.peers {
		//初始化nextIndex、和matchIndex
		rf.nextIndex[index] = rf.GetLastLogIndex() + 1 //log从下标1开始计数,初始时len(log)=1,nextIndex=1
		rf.matchIndex[index] = 0
	}
	for server, _ := range rf.peers {
		if rf.me == server {
			continue
		}
		go rf.sendHeartBeat(server)
	}
	//DPrintf("[%d-%v-%d-%d] next%v,%v", rf.nextIndex, rf.matchIndex)
}

//with lock
func (rf *Raft) reSetTimer() {
	rf.lastReceiveTime = time.Now()
	rf.overTime = randInt(150) + 150
}

func min(x int, y int) int {
	if x > y {
		return y
	}
	return x
}

func max(x int, y int) int {
	if x < y {
		return y
	}
	return x
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	reply.NextIndex = -1
	if rf.currentTerm > args.Term {
		//1.旧的term，拒绝请求
		reply.Success = false
		return
	}
	//当rf.currentTerm<=term，说明有以下两种情况：
	//有新的Leader，变为Follower(term>current) 设置term、并重置选举超时
	//或者是term相等，此时也要重置选举计时器（toFollower()这里直接连超时时间也重置了，应该问题不大）
	if rf.currentTerm <= args.Term {
		rf.toFollower(args.Term)
	}
	rf.reSetTimer()

	//重置计时后，开始处理日志信息
	if args.PrevLogIndex > rf.GetLastLogIndex() {
		//2.log中没有PrevLogIndex这一条，返回false
		reply.Success = false
		reply.NextIndex = rf.GetLastLogIndex() + 1
		return
	}
	if rf.logs[rf.GetTargetLogIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		//2.对应PrevLogIndex的log的term不匹配，返回false
		reply.Success = false
		if args.PrevLogIndex <= rf.GetLastLogIndex() {
			reply.NextIndex = args.PrevLogIndex
			for rf.logs[rf.GetTargetLogIndex(reply.NextIndex-1)].Term == rf.logs[rf.GetTargetLogIndex(args.PrevLogIndex)].Term {
				reply.NextIndex--
			}
		}
		return
	}
	reply.Success = true
	//3-4.删除正确节点之后的日志,并添加新的日志
	matchIndex := -1
	for index := 0; args.PrevLogIndex+1+index < rf.GetLastLogIndex() && index < len(args.Entries)-1; index++ {
		if rf.logs[rf.GetTargetLogIndex(args.PrevLogIndex+1+index)].Term == args.Entries[index].Term {
			matchIndex = index
		} else {
			break
		}
	}
	if len(args.Entries) > 0 {
		DPrintf("[%d-%v-%d-%d] old logs:%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.logs)
	}
	rf.logs = rf.logs[:rf.GetTargetLogIndex(args.PrevLogIndex+2+matchIndex)]
	rf.logs = append(rf.logs, args.Entries[matchIndex+1:]...)
	if len(args.Entries) > 0 {
		DPrintf("[%d-%v-%d-%d] new logs:%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.logs)
	}

	if args.LeaderCommit > rf.commitIndex { //5.修改commitIndex，并将新的log进行apply
		newCommitIndex := min(args.LeaderCommit, rf.GetLastLogIndex())
		//if rf.logs[newCommitIndex].Term == rf.currentTerm {
		DPrintf("[%d-%v-%d-%d] !!!!!!!update commitIndex from %d to %d,log=%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.commitIndex, newCommitIndex, rf.logs)
		rf.commitIndex = newCommitIndex
		//}
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
//the first return value is the index that the command will appear at
//if it's ever committed. the second return value is the current
//term. the third return value is true if this server believes it is
//the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.state == Leader
	if isLeader {
		term = rf.currentTerm
		index = rf.GetLastLogIndex() + 1
		rf.logs = append(rf.logs, LogEntry{command,
			term, index})
		DPrintf("[%d-%v-%d-%d] add a log :%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.logs[rf.GetTargetLogIndex(index)])
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.state != Leader {
			overTime := rf.lastReceiveTime.Add(time.Duration(rf.overTime) * time.Millisecond)
			if time.Now().After(overTime) {
				DPrintf("[%d-%v-%d-%d] OutTime %v-%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.state, rf.overTime)
				rf.reSetTimer()
				go rf.startElection()
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(1) * time.Millisecond)
	}
}

//func (rf *Raft) ticker() {
//	for rf.killed() == false {
//
//		// Your code here to check if a leader election should
//		// be started and to randomize sleeping time using
//		// time.Sleep().
//		rf.mu.Lock()
//		if rf.state != Leader {
//			overTime := rf.lastReceiveTime.Add(time.Duration(rf.overTime) * time.Millisecond)
//			if time.Now().After(overTime) {
//				DPrintf("[%d-%v-%d-%d] OutTime %v-%v", rf.me,rf.state,rf.currentTerm,len(rf.logs), rf.state, rf.overTime)
//				rf.lastReceiveTime = time.Now()
//				go rf.startElection()
//			}
//		}
//		rf.mu.Unlock()
//		time.Sleep(time.Duration(1) * time.Millisecond)
//	}
//}

func (rf *Raft) heartbeatTicker() {
	for rf.killed() == false {
		_, state := rf.GetState()
		if state {
			//是leader
			for server, _ := range rf.peers {
				if rf.me == server {
					continue
				}
				go rf.sendHeartBeat(server)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) applyTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex { //&& rf.logs[rf.commitIndex].Term == rf.currentTerm
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{}
				//DPrintf("")
				msg.Command = rf.logs[rf.GetTargetLogIndex(i)].Command
				msg.CommandIndex = rf.logs[rf.GetTargetLogIndex(i)].Index
				msg.CommandValid = true
				rf.mu.Unlock()
				rf.applyChan <- msg
				rf.mu.Lock()
				rf.lastApplied = i
				DPrintf("[%d-%v-%d-%d] apply %v", rf.me, rf.state, rf.currentTerm, len(rf.logs), msg)
			}
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	//rf.lastReceiveTime = time.Now()
	//rf.overTime = randInt(150) + 150
	rf.state = Candidate
	rf.currentTerm += 1
	term := rf.currentTerm
	//id := rf.me
	rf.votedFor = rf.me //给自己投一票
	DPrintf("[%d-%v-%d-%d] try an election with term %d", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.currentTerm)
	rf.persist()
	rf.mu.Unlock()

	mu := sync.Mutex{} //对vote、finished加锁
	cond := sync.NewCond(&mu)
	votes, finished := 1, 1 //给自己投一票
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(index int) {
			//发起一个RequestVote请求
			args, reply := RequestVoteArgs{}, RequestVoteReply{}
			rf.mu.Lock()
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = rf.logs[len(rf.logs)-1].Index
			args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
			rf.persist()
			rf.mu.Unlock()
			resultVote := rf.sendRequestVote(index, &args, &reply)
			//DPrintf("[%d-%v-%d-%d] args:%v,reply:%v", rf.me,rf.state,rf.currentTerm,len(rf.logs), args, reply)
			//根据RequestVote的结果更新票数
			mu.Lock()
			defer mu.Unlock()
			rf.mu.Lock()
			defer rf.mu.Unlock()
			defer rf.persist()
			if resultVote { //请求成功
				if rf.currentTerm == args.Term { //查看投票结果
					if rf.currentTerm < reply.Term { //发现更大的term，变为Follower
						rf.toFollower(reply.Term)
						rf.reSetTimer()
					} else if reply.VoteGranted && rf.state == Candidate {
						votes++
						DPrintf("[%d-%v-%d-%d] get votes %d", rf.me, rf.state, rf.currentTerm, len(rf.logs), votes)
					}
				}
			}
			finished++
			cond.Broadcast()
		}(i)
	}

	mu.Lock()
	for votes < (len(rf.peers)+1)/2 && finished != len(rf.peers) { //票数超过一般或者全部请求完毕
		cond.Wait()
	}
	if votes >= (len(rf.peers)+1)/2 {
		rf.mu.Lock()
		if rf.state == Candidate {
			//DPrintf("[%d-%v-%d-%d] become leader(%v) - %d", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.state, term)
			rf.toLeader()
		} else {
			DPrintf("[%d-%v-%d-%d] cannot be leader %v - %d", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.state, term)
		}
		rf.persist()
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		DPrintf("[%d-%v-%d-%d] lose - %d", rf.me, rf.state, rf.currentTerm, len(rf.logs), term)
		rf.mu.Unlock()
	}
	mu.Unlock()
	//在这里不管candidate的状态也是可以的，总有办法重新出发选举超时
	//rf.mu.Lock()
	//if rf.state != Leader {
	//	rf.toFollower(-1)
	//}
	//rf.mu.Unlock()
	//DPrintf("elction end")
}

func (rf *Raft) sendHeartBeat(server int) {
	rf.mu.Lock()
	if rf.state != Leader { //发送之前检查自己是不是leader
		rf.mu.Unlock()
		return
	}
	for rf.nextIndex[server] <= rf.logs[0].Index { //发送installSnapshot
		DPrintf("[%d-%v-%d-%d] sendInstallSnapshot to %d", rf.me, rf.state, rf.currentTerm, len(rf.logs), server)
		args := InstallSnapshotArgs{
			rf.currentTerm,
			rf.me,
			rf.logs[0].Index,
			rf.logs[0].Term,
			rf.snapshot,
		}
		reply := InstallSnapshotReply{}
		rf.mu.Unlock()
		ok := rf.sendInstallSnapshot(server, &args, &reply)
		rf.mu.Lock()
		if !ok || reply.Term > rf.currentTerm {
			if reply.Term > rf.currentTerm {
				rf.toFollower(reply.Term)
			}
			rf.mu.Unlock()
			return
		}
		rf.nextIndex[server] = args.LastIncludedIndex + 1 //成功发送快照，继续发送心跳包
	}
	nextIndex := rf.nextIndex[server] //初始为Leader的log长度，最低为1
	entries := make([]LogEntry, len(rf.logs[rf.GetTargetLogIndex(nextIndex):]))
	copy(entries, rf.logs[rf.GetTargetLogIndex(nextIndex):]) //发送从nextIndex开始的所有log
	args := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		rf.logs[rf.GetTargetLogIndex(nextIndex-1)].Term, nextIndex - 1, //当nextIndex为0时可以生效
		entries,
		rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()
	//send:
	if rf.sendAppendEntries(server, &args, &reply) { //RPC成功
		rf.mu.Lock()
		DPrintf("[%d-%v-%d-%d] send HB to %d %v", rf.me, rf.state, rf.currentTerm, len(rf.logs), server, args)
		DPrintf("[%d-%v-%d-%d] HB reply from %d:%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), server, reply)
		if rf.currentTerm != args.Term { //首先检查是不是这个term的rpc请求
			rf.mu.Unlock()
			return
		} else if reply.Term > rf.currentTerm { //发现更大的term,转变为follower
			rf.toFollower(reply.Term)
			rf.reSetTimer()
		} else if rf.state != Leader { //此时已经不是Leader了，返回
			rf.mu.Unlock()
			return
		} else if reply.Success { //心跳成功,日志同步成功,检查并更新nextIndex、matchIndex、commitIndex
			//rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			if len(args.Entries) == 0 {
				rf.matchIndex[server] = args.PrevLogIndex
			} else {
				rf.matchIndex[server] = args.Entries[len(args.Entries)-1].Index
			}
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			//更新commitIndex
			for N := len(rf.logs) - 1; N > 0 && rf.logs[N].Term == rf.currentTerm; N-- {
				count := 0
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me || rf.matchIndex[i] >= rf.logs[N].Index {
						count++
					}
				}
				if count >= (len(rf.peers)+1)/2 && rf.commitIndex < rf.logs[N].Index {
					//DPrintf("[%d-%v-%d-%d] !!!!!!!!!!!!!!update commitIndex=%d,log=%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), N, rf.logs)
					//DPrintf("!!!!!!!!!!!!!!update commitIndex=,%d", )
					if rf.logs[N].Term == rf.currentTerm {
						DPrintf("[%d-%v-%d-%d] !!!!!!!update commitIndex from %d to %d,log=%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.commitIndex, rf.logs[N].Index, rf.logs)
						rf.commitIndex = rf.logs[N].Index
					}
					break
				}
			}
		} else {
			//更新nextIndex即可，等着下次心跳的发送
			//或者直接重新发送，直接调用自身即可
			rf.nextIndex[server] = max(rf.logs[0].Index+1, rf.nextIndex[server]-1)
			if reply.NextIndex != -1 {
				rf.nextIndex[server] = max(1, reply.NextIndex)
			}
			//rf.nextIndex[server] = 1
		}
		rf.persist()
		rf.mu.Unlock()
	}
}

//func startElection() {
//	println("start election....")
//}

func randInt(n int) int {
	return rand.Intn(n)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rand.Seed(int64(me) + time.Now().Unix())
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	//rf.Snapshot()

	// Your initialization code here (2A, 2B, 2C).
	rf.toFollower(0)
	rf.reSetTimer()
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.me = me
	rf.logs = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyChan = applyCh

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeatTicker()
	go rf.applyTicker()

	return rf
}
