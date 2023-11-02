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
	"6.5840/labgob"
	"bytes"
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
	candidate        = 0
	leader           = 1
	follower         = 2
	ElectionTimeout  = 300
	HeartBeatTimeout = 150
	ApplyTimeout     = 30
)

type ApplyMsg struct {
	CommandValid bool // true表示当前l是一个newly committed log
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	// For 2D:
	SnapshotValid bool // true表示当前是一个snapshot
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

	timeTicker       *time.Ticker // 选举计时器
	heartBeatTicker  *time.Ticker // 心跳计时器
	state            int          // 0表示candidate，1表示leader，2表示follower
	curTerm          int          // 当前的任期
	votedFor         int          // 给谁投票了
	votes            int          // 获得了多少票
	log              []Entry      // log
	commitIndex      int          // 已被提交的log中最高的index -> 被提交后再进行执行
	lastApplied      int          // 被执行的log中的最高的index -> 被执行在被提交之后
	lastIncludeIndex int          // 在快照中的最高的索引
	lastIncludeTerm  int          // 在快照中最高的索引对应log的任期
	nextIndex        []int        // 对于每个server，应该发送的下一个索引
	matchIndex       []int        // 对于每个server，已知已经被commit到server的最高的索引
	snapshot         []byte       // 快照
	applyChan        chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) GetRaftStateSize() int { // 给kvserver调用的接口
	return rf.persister.RaftStateSize()
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.curTerm)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	e.Encode(rf.log)
	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var voteFor int
	var curTerm int
	var lastIncludeIndex int
	var lastIncludeTerm int
	var log []Entry

	if d.Decode(&voteFor) != nil || d.Decode(&curTerm) != nil || d.Decode(&lastIncludeIndex) != nil || d.Decode(&lastIncludeTerm) != nil || d.Decode(&log) != nil {
		fmt.Printf("%v读取持久化错误\n", rf.me)
	} else {
		rf.votedFor = voteFor
		rf.curTerm = curTerm
		rf.log = log
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
		rf.lastApplied = rf.lastIncludeIndex
		rf.commitIndex = rf.lastIncludeIndex
		rf.snapshot = rf.persister.ReadSnapshot()
		fmt.Printf("%v 读取持久化了 voteFor = %v,curTerm = %v, log = %v，lastIncludeIndex = %v, lastIncludeTerm = %v\n", rf.me, rf.votedFor, rf.curTerm, rf.log, lastIncludeIndex, lastIncludeTerm)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	fmt.Printf("snapshot: %v截断从%v位置之前（包括%v）的log，lastIndex = %v\n", rf.me, index, index, rf.lastIncludeIndex)
	if index <= rf.lastIncludeIndex {
		rf.mu.Unlock()
		return
	}
	back := make([]Entry, len(rf.log[index-rf.lastIncludeIndex:]))
	copy(back, rf.log[index-rf.lastIncludeIndex:])
	rf.snapshot = clone(snapshot)
	rf.lastIncludeTerm = rf.log[index-rf.lastIncludeIndex-1].Term
	rf.log = back
	rf.lastIncludeIndex = index
	rf.lastApplied = max(rf.lastApplied, index)
	rf.commitIndex = max(rf.commitIndex, index)
	rf.persist()
	rf.mu.Unlock()
	//if rf.state == leader {
	//	args := InstallSnapshotArgs{
	//		Term:             rf.curTerm,
	//		LeaderID:         rf.me,
	//		LastIncludeIndex: rf.lastIncludeIndex,
	//		LastIncludeTerm:  rf.lastIncludeTerm,
	//		Data:             rf.snapshot,
	//	}
	//	rf.mu.Unlock()
	//	for i := range rf.peers {
	//		if i == rf.me {
	//			continue
	//		}
	//		go func(i int) {
	//			reply := InstallSnapshotReply{}
	//			rf.sendInstallSnapshot(i, &args, &reply)
	//		}(i)
	//	}
	//} else {
	//	rf.mu.Unlock()
	//}
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
	Term          int  // 回复的时间戳
	Success       bool // 是否添加成功
	ConflictIndex int  // 冲突的索引
	ConflictTerm  int  // 冲突的term
}

type InstallSnapshotArgs struct {
	Term             int    // leader的term
	LeaderID         int    // leader的id
	LastIncludeIndex int    // 上次快照最后一个的索引
	LastIncludeTerm  int    // 上次快照最后一个的任期
	Data             []byte // snapshot
}

type InstallSnapshotReply struct {
	Term int // 回复follower的任期
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
		rf.nextIndex[i] = len(rf.log) + 1 + rf.lastIncludeIndex
		rf.matchIndex[i] = 0
	}
	rf.persist()
	rf.heartBeatTicker.Reset(HeartBeatTimeout * time.Millisecond) // 开启心跳计时器
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if args.Term < rf.curTerm {
		reply.Term = rf.curTerm
		rf.mu.Unlock()
		return
	}
	if rf.curTerm < args.Term {
		rf.curTerm = args.Term
		if rf.state <= 1 { // 如果term比它大，而且还是leader or candidate，转变成follower
			rf.state = follower
			rf.votedFor = args.LeaderID
		}
	}
	if args.LastIncludeIndex < rf.lastIncludeIndex {
		if rf.curTerm < args.Term {
			rf.persist()
		}
		reply.Term = rf.curTerm
		rf.resetTimeTicker()
		rf.mu.Unlock()
		return
	}

	if rf.lastIncludeIndex == args.LastIncludeIndex && rf.lastIncludeTerm == args.LastIncludeTerm {
		if rf.curTerm < args.Term {
			rf.persist()
		}
		reply.Term = rf.curTerm
		rf.resetTimeTicker()
		rf.mu.Unlock()
		return
	}
	lastLogIndex := rf.lastIncludeIndex + len(rf.log)
	if lastLogIndex <= args.LastIncludeIndex {
		rf.log = make([]Entry, 0)
	} else {
		n := len(rf.log[args.LastIncludeIndex-rf.lastIncludeIndex:])
		back := make([]Entry, n)
		copy(back, rf.log[args.LastIncludeIndex-rf.lastIncludeIndex:])
		rf.log = back
	}
	rf.snapshot = clone(args.Data)
	rf.lastIncludeIndex = args.LastIncludeIndex
	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludeIndex)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludeIndex)
	rf.persist()
	rf.resetTimeTicker()
	applymsg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      clone(rf.snapshot),
		SnapshotIndex: rf.lastIncludeIndex,
		SnapshotTerm:  rf.lastIncludeTerm,
	}
	rf.mu.Unlock()
	rf.applyChan <- applymsg
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = max(args.Term, rf.curTerm)
	fmt.Printf("%v收到了来自%v的投票请求，<%v的term是%v，状态是%v>，<%v的term是%v>\n", rf.me, args.CandidateID, rf.me, rf.curTerm, rf.state, args.CandidateID, args.Term)
	if args.Term <= rf.curTerm {
		reply.VoteGranted = false
		return
	} else { // 当前request有效
		if len(rf.log) > 0 || rf.lastIncludeIndex > 0 {
			var lastLog Entry
			if len(rf.log) > 0 {
				lastLog = rf.log[len(rf.log)-1]
			} else {
				lastLog = Entry{Term: rf.lastIncludeTerm}
			}
			if (args.LastLogTerm < lastLog.Term) || ((args.LastLogTerm == lastLog.Term) && (args.LastLogIndex < len(rf.log)+rf.lastIncludeIndex)) { // 选举限制
				fmt.Printf("args.LastLogTerm = %v, lastLog.Term = %v,args.LastLogIndex = %v ,lastLogIndex = %v\n ", args.LastLogTerm, lastLog.Term, args.LastLogIndex, len(rf.log))
				fmt.Printf("%v受到选举限制，%v不给%v投票\n", args.CandidateID, rf.me, args.CandidateID)
				reply.VoteGranted = false
				if rf.state <= 1 { // candidate or leader
					rf.votedFor = -1
					rf.state = follower
					rf.curTerm = reply.Term
					rf.resetTimeTicker()
				} else if rf.state == follower {
					rf.curTerm = reply.Term
				}
				rf.persist()
				return
			}
		}
		rf.curTerm = reply.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		if rf.state <= 1 { // candidate 或者 leader
			rf.state = follower
			fmt.Printf("第%v个raft被重置成follower\n", rf.me)
		} else { // follower 此时一定得投这个票，因为curTerm > Term
			fmt.Printf("第%v个raft的计时器被重置了\n", rf.me)
		}
		rf.persist()
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
		rf.mu.Unlock()
	}()
	if args.Term < rf.curTerm {
		fmt.Printf("%v发送给%v的AppendEntries的任期已经过时\n", args.LeaderID, rf.me)
		reply.Success = false
		return
	}
	if rf.curTerm < args.Term {
		rf.curTerm = reply.Term
		if rf.state <= 1 { // 如果term比它大，而且还是leader or candidate，转变成follower
			rf.state = follower
			rf.votedFor = args.LeaderID
		}
	}
	/*
		是否会出现args.PrevLogIndex < rf.lastIncludeIndex 的情况？
		Lab2中：
		正常情况：
		假设会出现，由rf.lastIncludeIndex，当前follower至少是已经被之前leader过提交的
		则新的leader一定会有之前leader的commit之前的结果
		则matchIndex一定大于commitIndex
		有 leader.nextIndex[i]-1 >= leader.matchIndex[i] >= follower(i).commitIndex >=follower(i).applyIndex > = follower(i).lastIncludeIndex
		与假设不符
		是否会出现args.PrevLogIndex == rf.lastIncludeIndex && args.PrevLogTerm != rf.lastIncludeTerm 的情况？
		同理，leader一定有follower的快照的信息，故不会出现这种情况
		稀少情况：
		rpc乱序，会有此种情况发生
		Lab3中：
		一定会出现，因为可能会有follower自发snapshot，如leader 当前发送一个snapshot快照，lastIncludeIndex=10，接着又发送了[11,20]的命令
		此时leader又接收一条新的命令，将它发给follower，发送了[11,21]的命令
		此时follwer收到了，当运行到index=15的命令后，自己进行snapshot，此时follower的lastIncludeIndex = 15
		如果此时leader未收到之前[11,20]的命令的回复，会一直重发[11,21]的
		此时就会一直出现args.PrevLogIndex <  rf.lastIncludeIndex的情况
		这种情况需要再特殊处理
	*/
	if (args.PrevLogIndex-rf.lastIncludeIndex) > 0 && ((args.PrevLogIndex > (len(rf.log))+rf.lastIncludeIndex) || (rf.log[args.PrevLogIndex-rf.lastIncludeIndex-1].Term != args.PrevLogTerm)) { // 如果一致性检查失败
		reply.Success = false
		if args.PrevLogIndex > (len(rf.log) + rf.lastIncludeIndex) {
			fmt.Printf("PrevLogIndex = %v, PrevLogTerm = %v，未在%v的log中找到\n", args.PrevLogIndex, args.PrevLogTerm, rf.me)
			reply.ConflictIndex = len(rf.log) + rf.lastIncludeIndex + 1
			reply.ConflictTerm = -1
		} else {
			fmt.Printf("PrevLogIndex = %v, PrevLogTerm = %v,  rf.log.Index = %v, rf.log[args.PrevLogIndex-1].Term = %v\n", args.PrevLogIndex, args.PrevLogTerm, len(rf.log), rf.log[args.PrevLogIndex-1-rf.lastIncludeIndex].Term)
			reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.lastIncludeIndex-1].Term
			for i, v := range rf.log {
				if v.Term == reply.ConflictTerm {
					reply.ConflictIndex = i + 1 + rf.lastIncludeIndex
					break
				}
			}
		}
		rf.persist()
		rf.resetTimeTicker() // 重置计时器
		return
	} else if (args.PrevLogIndex == rf.lastIncludeIndex) && (args.PrevLogTerm != rf.lastIncludeTerm) {
		reply.Success = false
		reply.ConflictIndex = args.PrevLogIndex
		reply.ConflictTerm = -1
	}
	if args.PrevLogIndex < rf.lastIncludeIndex {
		if args.PrevLogIndex+len(args.Entries) <= rf.lastIncludeIndex {
			fmt.Printf("[%d]收到一条过期的log, args.PrevLogIndex=%v,len(args.Entries)=%v, rf.lastIncludeIndex=%v\n", rf.me, args.PrevLogIndex, len(args.Entries), rf.lastIncludeIndex)
			reply.Success = true
			return
		}
		trim := rf.lastIncludeIndex - args.PrevLogIndex // 多余的部分
		args.PrevLogIndex = rf.lastIncludeIndex
		args.PrevLogTerm = args.Entries[trim-1].Term
		args.Entries = args.Entries[trim:]
	}

	reply.Success = true
	if len(args.Entries) == 0 { // 当前raft比candidate还新 或者 心跳
		// 不做任何操作
	} else { // 删除匹配之后的log
		var lastIndex int
		lastIndex = len(rf.log) + rf.lastIncludeIndex
		if ((args.PrevLogIndex + len(args.Entries)) <= rf.lastIncludeIndex) || (((args.PrevLogIndex + len(args.Entries)) <= lastIndex) && rf.log[args.PrevLogIndex+len(args.Entries)-rf.lastIncludeIndex-1].Term == args.Entries[len(args.Entries)-1].Term) { // 接受了一个老的
			// 先接受1234
			// 再收到12，此时12丢弃，原来的版本错误了
		} else { //
			if rf.lastIncludeIndex <= args.PrevLogIndex {
				rf.log = rf.log[0 : args.PrevLogIndex-rf.lastIncludeIndex] // append新的log
				for i, _ := range args.Entries {
					rf.log = append(rf.log, args.Entries[i])
				}
			} else {
			}
			fmt.Printf("%v 接受了 %v发送的log，当前log的长度为%v\n", rf.me, args.LeaderID, len(rf.log))
		}
	}
	if args.LeaderCommit > rf.commitIndex { // leader多提交了log
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)+rf.lastIncludeIndex)
	}
	rf.persist()
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
		rf.persist()
		rf.resetTimeTicker()
	}
	rf.mu.Unlock()
	return ok
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok == false {
		return ok
	}
	rf.mu.Lock()
	if reply.Term > rf.curTerm {
		rf.curTerm = reply.Term
		rf.state = follower
		rf.votes = 0
		rf.votedFor = -1
		rf.persist()
		rf.resetTimeTicker()
		rf.mu.Unlock()
		return false
	}
	rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludeIndex)
	rf.nextIndex[server] = rf.matchIndex[server] + 1
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
		rf.persist()
		rf.resetTimeTicker()
		rf.mu.Unlock()
		return ok
	}
	if reply.Success == false {
		index := 0
		ok := false
		for i, v := range rf.log {
			if v.Term == reply.ConflictTerm {
				ok = true
				index = i + 1
				continue
			}
			if ok {
				break
			}
		}
		if index > 0 && ok { // 找到了
			rf.nextIndex[server] = max(index+rf.lastIncludeIndex, 1)
		} else {
			rf.nextIndex[server] = max(1, reply.ConflictIndex)
		}
		if rf.nextIndex[server] <= rf.matchIndex[server] { // 可能两次并发不一样
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
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
	if rf.state != leader { // 如果不是leader，直接返回，不能接受来自client的请求
		rf.mu.Unlock()
		return -1, -1, false
	}
	log := Entry{command, rf.curTerm}
	rf.log = append(rf.log, log)
	index := len(rf.log) + rf.lastIncludeIndex
	term := rf.curTerm
	rf.persist()
	go rf.sendHeartBeats(term)
	rf.resetHeartBeats()
	fmt.Printf("%v接受了一个命令，当前命令term是%v，index是%v\n", rf.me, rf.curTerm, index)
	rf.mu.Unlock()
	return index, term, true
}

func (rf *Raft) resetHeartBeats() {
	rf.heartBeatTicker.Reset(time.Millisecond * HeartBeatTimeout)
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
	fmt.Printf("raft:%v 死掉了\n", rf.me)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) resetTimeTicker() { // 重置选举计时器
	ms := ElectionTimeout + (rand.Int63() % ElectionTimeout)
	rf.timeTicker.Reset(time.Duration(ms) * time.Millisecond)
	return
}
func (rf *Raft) applyMsg() {
	for rf.killed() == false {
		rf.mu.Lock()
		fmt.Printf("%v 当前应用命令，commitIndex是%v，lastApplied是%v,lastIncludeIndex: %v\n", rf.me, rf.commitIndex, rf.lastApplied, rf.lastIncludeIndex)
		if rf.commitIndex > rf.lastApplied {
			fmt.Printf("%v 当前应用命令，commitIndex是%v，lastApplied是%v，lastIncludeIndex: %v,log的长度是%v\n", rf.me, rf.commitIndex, rf.lastApplied, rf.lastIncludeIndex, len(rf.log))
			msgList := []ApplyMsg{}
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				rf.lastApplied++
				log := rf.log[rf.lastApplied-rf.lastIncludeIndex-1]
				msg := ApplyMsg{
					Command:       log.Command,
					CommandTerm:   log.Term,
					CommandIndex:  rf.lastApplied,
					CommandValid:  true,
					SnapshotValid: false,
				}
				msgList = append(msgList, msg)
			}
			rf.mu.Unlock()
			for _, msg := range msgList {
				rf.applyChan <- msg
			}
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(time.Millisecond * ApplyTimeout)
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
	isFall := make([]bool, len(rf.peers))
	snapShotArgs := InstallSnapshotArgs{
		Term:             rf.curTerm,
		LeaderID:         rf.me,
		LastIncludeIndex: rf.lastIncludeIndex,
		LastIncludeTerm:  rf.lastIncludeTerm,
		Data:             rf.snapshot,
	}
	for i := range rf.peers { // 不把这个循环和下面的合并的原因是rpc不能加锁
		args[i].Term = rf.curTerm
		args[i].LeaderCommit = rf.commitIndex
		args[i].LeaderID = rf.me
		args[i].PrevLogIndex = rf.nextIndex[i] - 1
		if i == rf.me {
			continue
		}
		if args[i].PrevLogIndex < rf.lastIncludeIndex {
			isFall[i] = true
			continue
		}
		if (args[i].PrevLogIndex - rf.lastIncludeIndex) < len(rf.log) { // 如果当前存在要发送的entry
			fmt.Printf("leader：%v，%v的prevlogindex是%v，lastIncludeInde是%v，当前共有%v个log\n", rf.me, i, args[i].PrevLogIndex, rf.lastIncludeIndex, len(rf.log))
			n := len(rf.log[args[i].PrevLogIndex-rf.lastIncludeIndex:])
			args[i].Entries = make([]Entry, n)
			copy(args[i].Entries, rf.log[args[i].PrevLogIndex-rf.lastIncludeIndex:]) // 出现的奇奇怪怪的bug，只能用拷贝，不然会在AppendEntries中会有竞争错误
		}
		if (args[i].PrevLogIndex - rf.lastIncludeIndex) > 0 {
			args[i].PrevLogTerm = rf.log[args[i].PrevLogIndex-1-rf.lastIncludeIndex].Term
		} else if args[i].PrevLogIndex == rf.lastIncludeIndex { // 坑点：当前上一个log找不到的话，表明当前快照已经保存完毕，此时应该将prevlogterm置为rf.lastIncludeTerm
			args[i].PrevLogTerm = rf.lastIncludeTerm
		}
	}
	rf.mu.Unlock()
	replyChan := make(chan int, len(rf.peers)-1)
	var mutex sync.Mutex
	cnt := 0
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if isFall[i] == false {
			go func(i int) {
				fmt.Printf("%v给%v发送了一个log，log长度%v，args[i].PrevLogIndex = %v\n", rf.me, i, len(args[i].Entries), args[i].PrevLogIndex)
				rf.sendAppendEntries(i, &args[i], &reply[i])
				replyChan <- i
				mutex.Lock()
				cnt++
				if cnt == len(rf.peers)-1 {
					close(replyChan)
				}
				mutex.Unlock()
			}(i)
		} else {
			go func(i int) {
				reply := InstallSnapshotReply{}
				rf.sendInstallSnapshot(i, &snapShotArgs, &reply)
				replyChan <- i
			}(i)
		}
	}
	for i := range replyChan {
		if i == rf.me || isFall[i] == true {
			continue
		}
		rf.mu.Lock()
		N := rf.matchIndex[i]
		if N <= rf.commitIndex || reply[i].Success == false {
			rf.mu.Unlock()
			continue
		}
		cnt1 := 1
		for j, v := range rf.matchIndex {
			if j == rf.me {
				continue
			}
			if v >= N {
				cnt1++
			}
		}
		if (cnt1 > (len(rf.peers) >> 1)) && (rf.log[N-rf.lastIncludeIndex-1].Term == rf.curTerm) {
			rf.commitIndex = N
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startHeartBeats(term int) { // 开启心跳
	for !rf.killed() {
		select {
		case <-rf.heartBeatTicker.C:
			rf.mu.Lock()
			if rf.state != leader || term != rf.curTerm {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
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
		args.LastLogIndex = len(rf.log) + rf.lastIncludeIndex
		args.LastLogTerm = rf.log[args.LastLogIndex-rf.lastIncludeIndex-1].Term
	} else { // 选举限制需要加的
		args.LastLogTerm = rf.lastIncludeTerm
		args.LastLogIndex = rf.lastIncludeIndex
	}
	if lastTerm != rf.curTerm || rf.state != candidate { // 任期改变
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	var mutex sync.Mutex
	cond := sync.NewCond(&mutex)
	cnt := 1
	count := 0
	for id := range rf.peers { // 请求投票
		if id == rf.me {
			continue
		}
		fmt.Printf("%v请求%v投票\n", rf.me, id)
		go func(id int) { // 投票
			ok := rf.sendRequestVote(id, &args, &replys[id])
			if ok == false {
				mutex.Lock()
				count++
				mutex.Unlock()
				return
			}
			mutex.Lock()
			count++
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
			if count == len(rf.peers)-1 {
				fmt.Printf("%v 在%v任期选举失败，退出选举\n", rf.me, rf.curTerm)
				rf.mu.Unlock()
				mutex.Unlock()
				return
			}
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
				rf.votedFor = -1
				rf.votes = 1
				rf.persist()
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
	rf.timeTicker = time.NewTicker(time.Millisecond * ElectionTimeout)
	rf.heartBeatTicker = time.NewTicker(time.Millisecond * HeartBeatTimeout)
	rf.state = follower
	rf.votedFor = -1
	rf.log = []Entry{}
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.snapshot = make([]byte, 0)
	rf.lastApplied = 0
	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0
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
