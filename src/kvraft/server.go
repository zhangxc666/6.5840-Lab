package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ClientID  int64
	CommandID int64
	Key       string
	Value     string
	Op        string
}

type Result struct {
	value string
	err   Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	dataBase         *KvDataBase
	indexChan        map[int]chan Result // <index,chan Result> 对每个command单开一个channel，得到执行命令的返回值
	userMaxCommandID map[int64]int64     // 存储每个user的最大commandID
}

func (kv *KVServer) listenApplyChan() {
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid == true {
				op := msg.Command.(Op)
				fmt.Printf("[kv:%v] 从 [op:%v] <- Applychan \n", kv.me, op)
				res, err := kv.applyMsg(op)
				if term, isLeader := kv.rf.GetState(); isLeader == true && term == msg.CommandTerm { // 不是leader的话，就没有任何rpc等待这个结果
					// 关于这个解释看readme文档 term == msg.CommandTerm
					response := Result{
						value: res, err: err,
					}
					ch := kv.getIndexChan(msg.CommandIndex)
					ch <- response
					fmt.Printf("[kv:%v] 把 [op:%v] -> RpcHandle \n", kv.me, op)
				}
			}
			break
		}
	}
}
func (kv *KVServer) applyMsg(op Op) (string, Err) { // 一定是顺序执行的不用加锁
	res := ""
	var err Err
	if op.Op == "GET" {
		res, err = kv.dataBase.get(op.Key)
		return res, err
	} else {
		if op.CommandID <= kv.userMaxCommandID[op.ClientID] {
			return res, OK
		}
		kv.userMaxCommandID[op.ClientID] = op.CommandID
		if op.Op == "Append" {
			err = kv.dataBase.append(op.Key, op.Value)
		} else {
			err = kv.dataBase.put(op.Key, op.Value)
		}
		return res, err
	}
}
func (kv *KVServer) getIndexChan(index int) chan Result {
	kv.mu.Lock()
	var ch chan Result
	if _, ok := kv.indexChan[index]; ok == false {
		kv.indexChan[index] = make(chan Result, 1)
	}
	ch = kv.indexChan[index]
	kv.mu.Unlock()
	return ch
}

func (kv *KVServer) delIndexChan(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	close(kv.indexChan[index])
	delete(kv.indexChan, index)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		ClientID: args.ClientID,
		Key:      args.Key,
		Op:       "GET",
	}
	index, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.getIndexChan(index)
	timer := time.NewTimer(time.Millisecond * 1000)
	defer func() {
		kv.delIndexChan(index)
		timer.Stop()
	}()
	select {
	case res := <-ch:
		reply.Value = res.value
		reply.Err = res.err
		break
	case <-timer.C:
		reply.Value = ""
		reply.Err = ErrTimeOut
		break
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		ClientID:  args.ClientID,
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Op,
		CommandID: args.CommandID,
	}
	index, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.getIndexChan(index)
	timer := time.NewTimer(time.Millisecond * 1000)
	defer func() {
		kv.delIndexChan(index)
		timer.Stop()
	}()
	select {
	case res := <-ch:
		reply.Err = res.err
		break
	case <-timer.C:
		reply.Err = ErrTimeOut
		break
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dataBase = NewKvDataBase()
	kv.indexChan = make(map[int]chan Result)
	kv.userMaxCommandID = make(map[int64]int64)
	go kv.listenApplyChan()
	// You may need initialization code here.

	return kv
}
