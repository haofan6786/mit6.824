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
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

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

	lastReceiveTime time.Time //????????????
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
	rf.persister.SaveRaftState(data)
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
	DPrintf("[%d-%v-%d-%d] readPersist", rf.me, rf.state, rf.currentTerm, len(rf.logs))
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("[%d-%v-%d-%d] received request vote from %v", rf.me, rf.state, rf.currentTerm, len(rf.logs), args)
	//DPrintf("%d,%d", rf.votedFor, args.CandidateId)
	reply.Term = rf.currentTerm //???candidate????????????
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		//term???????????????
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}
	//voteFor??????,??????election restriction ??????
	if args.LastLogTerm > rf.logs[len(rf.logs)-1].Term ||
		(args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= len(rf.logs)-1) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		DPrintf("[%d-%v-%d-%d] vote to %d", rf.me, rf.state, rf.currentTerm, len(rf.logs), args.CandidateId)
		rf.reSetTimer() //?????????????????????timer
		return
	}
}

type AppendEntriesArgs struct {
	Term         int        // leader????????????
	LeaderId     int        // leaderID ?????????????????????
	PrevLogTerm  int        // ????????????????????????Term
	PrevLogIndex int        // ?????????????????????????????????
	Entries      []LogEntry // ????????????????????? ?????????????????????
	LeaderCommit int        // leader??????????????????????????????
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
		//?????????nextIndex??????matchIndex
		rf.nextIndex[index] = len(rf.logs) //log?????????1????????????,?????????len(log)=1,nextIndex=1
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
		//1.??????term???????????????
		reply.Success = false
		return
	}
	//???rf.currentTerm<=term?????????????????????????????????
	//?????????Leader?????????Follower(term>current) ??????term????????????????????????
	//?????????term?????????????????????????????????????????????toFollower()???????????????????????????????????????????????????????????????
	if rf.currentTerm <= args.Term {
		rf.toFollower(args.Term)
	}
	rf.reSetTimer()

	//??????????????????????????????????????????
	if args.PrevLogIndex > len(rf.logs)-1 {
		//2.log?????????PrevLogIndex??????????????????false
		reply.Success = false
		reply.NextIndex = len(rf.logs)
		return
	}
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		//2.??????PrevLogIndex???log???term??????????????????false
		reply.Success = false
		if args.PrevLogIndex <= len(rf.logs)-1 {
			reply.NextIndex = args.PrevLogIndex
			for rf.logs[reply.NextIndex-1].Term == rf.logs[args.PrevLogIndex].Term {
				reply.NextIndex--
			}
		}
		return
	}
	reply.Success = true
	//3-4.?????????????????????????????????,?????????????????????
	matchIndex := -1
	for index := 0; args.PrevLogIndex+2+index < len(rf.logs) && index < len(args.Entries)-1; index++ {
		if rf.logs[args.PrevLogIndex+1+index].Term == args.Entries[index].Term {
			matchIndex = index
		} else {
			break
		}
	}
	if len(args.Entries) > 0 {
		DPrintf("[%d-%v-%d-%d] old logs:%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.logs)
	}
	rf.logs = rf.logs[:args.PrevLogIndex+2+matchIndex]
	rf.logs = append(rf.logs, args.Entries[matchIndex+1:]...)
	if len(args.Entries) > 0 {
		DPrintf("[%d-%v-%d-%d] new logs:%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.logs)
	}

	if args.LeaderCommit > rf.commitIndex { //5.??????commitIndex???????????????log??????apply
		newCommitIndex := min(args.LeaderCommit, len(rf.logs)-1)
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
		index = len(rf.logs)
		rf.logs = append(rf.logs, LogEntry{command,
			term, index})
		DPrintf("[%d-%v-%d-%d] add a log :%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.logs[index])
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
			//???leader
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
				msg.Command = rf.logs[i].Command
				msg.CommandIndex = rf.logs[i].Index
				msg.CommandValid = true
				rf.mu.Unlock()
				rf.applyChan <- msg
				rf.mu.Lock()
				rf.lastApplied = i
				//DPrintf("[%d-%v-%d-%d] commit %v", rf.me, rf.state, rf.currentTerm, len(rf.logs), msg)
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
	rf.votedFor = rf.me //??????????????????
	DPrintf("[%d-%v-%d-%d] try an election with term %d", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.currentTerm)
	rf.persist()
	rf.mu.Unlock()

	mu := sync.Mutex{} //???vote???finished??????
	cond := sync.NewCond(&mu)
	votes, finished := 1, 1 //??????????????????
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(index int) {
			//????????????RequestVote??????
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
			//??????RequestVote?????????????????????
			mu.Lock()
			defer mu.Unlock()
			rf.mu.Lock()
			defer rf.mu.Unlock()
			defer rf.persist()
			if resultVote { //????????????
				if rf.currentTerm == args.Term { //??????????????????
					if rf.currentTerm < reply.Term { //???????????????term?????????Follower
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
	for votes < (len(rf.peers)+1)/2 && finished != len(rf.peers) { //??????????????????????????????????????????
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
	//???????????????candidate???????????????????????????????????????????????????????????????
	//rf.mu.Lock()
	//if rf.state != Leader {
	//	rf.toFollower(-1)
	//}
	//rf.mu.Unlock()
	//DPrintf("elction end")
}

func (rf *Raft) sendHeartBeat(server int) {
	rf.mu.Lock()
	//DPrintf("[%d-%v-%d-%d] send heart to %d", rf.me,rf.state,rf.currentTerm,len(rf.logs), server)
	//args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.logs[rf.nextIndex[server]-1].Term,
	//	rf.nextIndex[server] - 1, []LogEntry{}, -1}
	if rf.state != Leader { //?????????????????????????????????leader
		rf.mu.Unlock()
		return
	}
	nextIndex := rf.nextIndex[server] //?????????Leader???log??????????????????1
	entries := make([]LogEntry, len(rf.logs[nextIndex:]))
	copy(entries, rf.logs[nextIndex:]) //?????????nextIndex???????????????log

	args := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		rf.logs[nextIndex-1].Term, nextIndex - 1, //???nextIndex???0???????????????
		entries,
		rf.commitIndex,
	}
	if args.PrevLogIndex > len(rf.logs)-1 {
		_ = 0
	}
	reply := AppendEntriesReply{}
	//DPrintf("[%d-%v-%d-%d] send HB to %d %v", rf.me, rf.state, rf.currentTerm, len(rf.logs), server, args)
	rf.mu.Unlock()
	//send:
	if rf.sendAppendEntries(server, &args, &reply) { //RPC??????
		//DPrintf("[%d-%v-%d-%d] HB reply from %d:%v", args.LeaderId, server, reply)
		rf.mu.Lock()
		if rf.currentTerm != args.Term { //???????????????????????????term???rpc??????
			rf.mu.Unlock()
			return
		} else if reply.Term > rf.currentTerm { //???????????????term,?????????follower
			rf.toFollower(reply.Term)
			rf.reSetTimer()
		} else if rf.state != Leader { //??????????????????Leader????????????
			rf.mu.Unlock()
			return
		} else if reply.Success { //????????????,??????????????????,???????????????nextIndex???matchIndex???commitIndex
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			//??????commitIndex
			for N := len(rf.logs) - 1; N > 0 && rf.logs[N].Term == rf.currentTerm; N-- {
				count := 0
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me || rf.matchIndex[i] >= N {
						count++
					}
				}
				if count >= (len(rf.peers)+1)/2 && rf.commitIndex < N {
					//DPrintf("[%d-%v-%d-%d] !!!!!!!!!!!!!!update commitIndex=%d,log=%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), N, rf.logs)
					//DPrintf("!!!!!!!!!!!!!!update commitIndex=,%d", )
					if rf.logs[N].Term == rf.currentTerm {
						DPrintf("[%d-%v-%d-%d] !!!!!!!update commitIndex from %d to %d,log=%v", rf.me, rf.state, rf.currentTerm, len(rf.logs), rf.commitIndex, N, rf.logs)
						rf.commitIndex = N
					}
					break
				}
			}
		} else {
			//??????nextIndex????????????????????????????????????
			//???????????????????????????????????????????????????
			rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)
			if reply.NextIndex != -1 {
				rf.nextIndex[server] = max(1, reply.NextIndex)
			}
			//rf.nextIndex[server] = 1
		}
		rf.persist()
		rf.mu.Unlock()
	}
	//else {
	//??????????????????????????????,????????????,?????????????????????????????????????????????
	//goto send
	//rf.sendHeartBeat(server)
	//DPrintf("[%d-%v-%d-%d] loss HeartBeat to %d:%v", args.LeaderId, server, args)
	//}
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
	//rf.persist()

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
