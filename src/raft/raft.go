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
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
const (
	candidate = 0
	leader    = 1
	follower  = 2
)

type ApplyMsg struct {
	CommandValid bool // true表示当前log entry是一个newly committed
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type Entry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	timeTicker      *time.Ticker // 选举计时器
	heartBeatTicker *time.Ticker // 心跳计时器
	state           int          // 0表示candidate，1表示leader，2表示follower
	curTerm         int          // 当前的任期
	votedFor        int          // 给谁投票了
	votes           int          // 获得了多少票
	log             []Entry      // log
	commitIndex     int          // 已被提交的log中最高的index -> 被提交后再进行执行
	lastApplied     int          // 被执行的log中的最高的index -> 被执行在被提交之后
	nextIndex       []int        // 对于每个server，应该发送的下一个索引
	matchIndex      []int        // 对于每个server，已知已经被commit到server的最高的索引
	applyChan       chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	term := rf.curTerm
	state := rf.state
	return term, state == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 发送请求的时间戳
	CandidateID  int // 需要投票的ID
	LastLogIndex int // candidate的最后一个log entry的索引 -> 选举限制
	LastLogTerm  int // candidate的最后一个log entry的任期 -> 选举限制
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 投票的时间
	VoteGranted bool // 是否投票
}

type AppendEntriesArgs struct {
	Term         int     // 时间戳
	LeaderID     int     // leader的ID，以至于follower可以给client的请求重定向
	PrevLogIndex int     // 新log之前的log的索引（因为可能会有冲突，定义为新log之前的log）
	PrevLogTerm  int     // 新log之前的log的时间戳
	LeaderCommit int     // leader的commit的index
	Entries      []Entry // log
}

type AppendEntriesReply struct {
	Term    int  // 回复的时间戳
	Success bool // 是否添加成功
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) convertToLeader() { // 转换到leader
	rf.state = leader
	rf.votedFor = rf.me
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}
	rf.heartBeatTicker.Reset(time.Millisecond * 50) // 开启心跳计时器
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = max(args.Term, rf.curTerm)
	fmt.Printf("%v收到了来自%v的投票请求，<%v的term是%v，状态是%v>，<%v的term是%v>\n", rf.me, args.CandidateID, rf.me, rf.curTerm, rf.state, args.CandidateID, args.Term)
	reply.VoteGranted = false
	defer func() { rf.curTerm = reply.Term }()
	if args.Term <= rf.curTerm {
		return
	} else { // 当前request有效
		if len(rf.log) > 0 {
			lastLog := rf.log[len(rf.log)-1]
			if (args.LastLogTerm < lastLog.Term) || ((args.LastLogTerm == lastLog.Term) && (args.LastLogIndex < len(rf.log))) { // 选举限制
				fmt.Printf("args.LastLogTerm = %v, lastLog.Term = %v,args.LastLogIndex = %v ,lastLogIndex = %v\n ", args.LastLogTerm, lastLog.Term, args.LastLogIndex, len(rf.log))
				fmt.Printf("%v受到选举限制，%v不给%v投票\n", args.CandidateID, rf.me, args.CandidateID)
				return
			}
		}
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		if rf.state <= 1 { // candidate 或者 leader
			rf.state = follower
			fmt.Printf("第%v个raft被重置成follower\n", rf.me)
		} else { // follower 此时一定得投这个票，因为curTerm > Term
			fmt.Printf("第%v个raft的计时器被重置了\n", rf.me)
		}
		rf.resetTimeTicker()
	}
}
func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	fmt.Printf("%v收到了来自%v的ppendEntries请求，<%v的term是%v，状态是%v>，<%v的term是%v>\n", rf.me, args.LeaderID, rf.me, rf.curTerm, rf.state, args.LeaderID, args.Term)
	reply.Term = max(rf.curTerm, args.Term)
	defer func() {
		rf.curTerm = reply.Term
		rf.mu.Unlock()
	}()
	if args.Term < rf.curTerm {
		fmt.Printf("%v发送给%v的AppendEntries的任期已经过时\n", args.LeaderID, rf.me)
		reply.Success = false
		return
	}
	if args.PrevLogIndex > 0 && (args.PrevLogIndex > len(rf.log) || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) { // 如果当前发生冲突
		reply.Success = false

		rf.resetTimeTicker() // 重置计时器
		return
	}
	reply.Success = true
	if len(args.Entries) == 0 { // 当前raft比candidate还新 或者 心跳
		// 不做任何操作
	} else { // 删除匹配之后的log
		rf.log = rf.log[0:args.PrevLogIndex] // append新的log
		for i, _ := range args.Entries {
			rf.log = append(rf.log, args.Entries[i])
		}
		fmt.Printf("%v 接受了 %v发送的log，当前log的长度为%v\n", rf.me, args.LeaderID, len(rf.log))
	}
	// 接受
	if rf.state <= 1 { // candidate and leader
		rf.state = follower
		rf.votedFor = args.LeaderID
	}
	if args.LeaderCommit > rf.commitIndex { // leader多提交了log
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
	}
	rf.resetTimeTicker()
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok == false {
		return ok
	}
	rf.mu.Lock()
	if reply.Term > rf.curTerm {
		rf.curTerm = reply.Term
		rf.state = follower
		rf.votes = 0
		rf.votedFor = -1
		rf.resetTimeTicker()
	}
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok == false {
		return ok
	}
	rf.mu.Lock()
	if reply.Term > rf.curTerm {
		rf.curTerm = reply.Term
		rf.state = follower
		rf.votes = 0
		rf.votedFor = -1
		rf.resetTimeTicker()
		rf.mu.Unlock()
		return ok
	}
	if reply.Success == false {
		//if len(args.Entries) > 0 {
		rf.nextIndex[server] = max(rf.nextIndex[server]-1, 1)
		fmt.Printf("%v一直性检查失败，新的nextIndex是%v\n", server, rf.nextIndex[server])
		//}
	} else {
		rf.matchIndex[server] = max(len(args.Entries)+args.PrevLogIndex, rf.matchIndex[server]) // 不能用nextIndex 更新，原因是发过去的和发回来的nextIndex不一定相等
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		if len(args.Entries) > 0 {
			fmt.Printf("发送了一个长度为%v的log成功，当前的%v的matchIndex是%v,nextIndex是%v\n", len(args.Entries), server, rf.matchIndex[server], rf.nextIndex[server])
		} else {
			fmt.Printf("发送了一个心跳成功，当前的%v的nextIndex是%v,matchIndex是%v\n", server, rf.nextIndex[server], rf.matchIndex[server])
		}
	}
	rf.mu.Unlock()
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) { // 接受来自client的命令
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader { // 如果不是leader，直接返回，不能接受来自client的请求
		return -1, -1, false
	}
	log := Entry{command, rf.curTerm}
	rf.log = append(rf.log, log)
	index := len(rf.log)
	term := rf.curTerm
	fmt.Printf("%v接受了一个命令，当前命令term是%v，index是%v\n", rf.me, rf.curTerm, index)
	return index, term, true
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) resetTimeTicker() { // 重置选举计时器
	ms := 150 + (rand.Int63() % 150)
	rf.timeTicker.Reset(time.Duration(ms) * time.Millisecond)
	return
}
func (rf *Raft) applyMsg() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			fmt.Printf("%v 当前应用命令，commitIndex是%v，lastApplied是%v\n", rf.me, rf.commitIndex, rf.lastApplied)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				rf.lastApplied++
				log := rf.log[rf.lastApplied-1]
				msg := ApplyMsg{
					Command:      log.Command,
					CommandTerm:  log.Term,
					CommandIndex: rf.lastApplied,
					CommandValid: true,
				}
				rf.applyChan <- msg
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 50)
	}
}

func (rf *Raft) sendHeartBeats(term int) { // 发送心跳 or log entry
	rf.mu.Lock()
	if rf.state != leader || rf.curTerm != term {
		rf.mu.Unlock()
		return
	}
	fmt.Printf("leader%v 开始发送心跳请求，当前term是%v\n", rf.me, term)
	args := make([]AppendEntriesArgs, len(rf.peers))
	reply := make([]AppendEntriesReply, len(rf.peers))
	for i := range rf.peers { // 不把这个循环和下面的合并的原因是rpc不能加锁
		args[i].Term = rf.curTerm
		args[i].LeaderCommit = rf.commitIndex
		args[i].LeaderID = rf.me
		args[i].PrevLogIndex = rf.nextIndex[i] - 1
		if i == rf.me {
			continue
		}
		if args[i].PrevLogIndex < len(rf.log) { // 如果当前存在要发送的entry
			fmt.Printf("leader：%v，%v的prevlogindex是%v，当前共有%v个log\n", rf.me, i, args[i].PrevLogIndex, len(rf.log))
			n := len(rf.log[args[i].PrevLogIndex:])
			args[i].Entries = make([]Entry, n)
			copy(args[i].Entries, rf.log[args[i].PrevLogIndex:]) // 出现的奇奇怪怪的bug，只能用拷贝，不然会在AppendEntries中会有竞争错误
		}
		if args[i].PrevLogIndex > 0 {
			args[i].PrevLogTerm = rf.log[args[i].PrevLogIndex-1].Term
		}
	}
	rf.mu.Unlock()
	replyChan := make(chan int)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			//if len(args[i].Entries) > 0 {
			fmt.Printf("%v给%v发送了一个log，log长度%v\n", rf.me, i, len(args[i].Entries))
			//}
			rf.sendAppendEntries(i, &args[i], &reply[i])
			replyChan <- i
		}(i)
	}
	for i := range replyChan {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		N := rf.matchIndex[i]
		if N <= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}
		cnt := 1
		for i, v := range rf.matchIndex {
			if i == rf.me {
				continue
			}
			if v >= N {
				cnt++
			}
		}
		//fmt.Printf("%v的matchIndex = %v, cnt = %v, N = %v, (len(rf.peers) >> 1)=%v, rf.log[N-1].Term = %v, rf.curTerm = %v\n", rf.me, rf.matchIndex, cnt, N, (len(rf.peers) >> 1), rf.log[N-1].Term, rf.curTerm)
		if (cnt > (len(rf.peers) >> 1)) && (rf.log[N-1].Term == rf.curTerm) {
			rf.commitIndex = N
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) startHeartBeats(term int) { // 开启心跳
	for !rf.killed() {
		select {
		case <-rf.heartBeatTicker.C:
			go rf.sendHeartBeats(term)
			break
		}
	}
}

func (rf *Raft) startElection(lastTerm int) { // 开启选举过程
	n := len(rf.peers)
	replys := make([]RequestVoteReply, n)
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:        rf.curTerm,
		CandidateID: rf.me,
	}
	if len(rf.log) > 0 {
		args.LastLogIndex = len(rf.log)
		args.LastLogTerm = rf.log[args.LastLogIndex-1].Term
	}
	if lastTerm != rf.curTerm || rf.state != candidate { // 任期改变
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	var mutex sync.Mutex
	cond := sync.NewCond(&mutex)
	cnt := 1
	for id := range rf.peers { // 请求投票
		if id == rf.me {
			continue
		}
		fmt.Printf("%v请求%v投票\n", rf.me, id)
		go func(id int) { // 投票
			ok := rf.sendRequestVote(id, &args, &replys[id])
			if ok == false {
				return
			}
			mutex.Lock()
			if replys[id].VoteGranted {
				cnt++
			}
			mutex.Unlock()
			cond.Broadcast()
			return
		}(id)
	}
	mutex.Lock()
	for {
		rf.mu.Lock()
		if cnt <= (n>>1) && rf.state == candidate {
			rf.mu.Unlock()
			cond.Wait()
		} else {
			fmt.Printf("跳出循环，当前票数%v\n", cnt)
			rf.mu.Unlock()
			break
		}
	}
	rf.mu.Lock()
	rf.votes = cnt
	rf.mu.Unlock()
	mutex.Unlock()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastTerm != rf.curTerm || rf.state != candidate {
		return
	}
	fmt.Printf("第%v个raft当选了leader，当前term为%v\n", rf.me, rf.curTerm)
	rf.convertToLeader()
	go rf.startHeartBeats(rf.curTerm)
}
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.timeTicker.C:
			rf.resetTimeTicker()
			rf.mu.Lock()
			if rf.state == follower || rf.state == candidate { // 变成candidate
				rf.curTerm++
				rf.state = candidate
				rf.votedFor = rf.me
				rf.votes = 1
				rf.mu.Unlock()
			} else if rf.state == leader { // 若当前已经变成leader无视此过程
				rf.mu.Unlock()
				break
			}
			// 开启选举过程
			rf.mu.Lock()
			fmt.Printf("当前第%v个raft开启选举过程，当前term是%v\n", rf.me, rf.curTerm)
			go rf.startElection(rf.curTerm)
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.timeTicker = time.NewTicker(time.Millisecond * 350)
	rf.heartBeatTicker = time.NewTicker(time.Millisecond * 50)
	rf.state = follower
	rf.votedFor = -1
	rf.log = []Entry{}
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.applyChan = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.resetTimeTicker()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyMsg()
	return rf
}
