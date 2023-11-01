package kvraft

import (
	"6.5840/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers   []*labrpc.ClientEnd
	commandID int64 // 表示当前的命令ID值
	clientId  int64 // 当前client的ID
	mutex     sync.Mutex
	leaderID  int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.commandID = 0
	ck.leaderID = -1
	ck.clientId = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{key, ck.clientId}
	ck.mutex.Lock()
	var leaderID int
	if ck.leaderID == -1 {
		leaderID = 0
	} else {
		leaderID = ck.leaderID
	}
	ck.mutex.Unlock()
	for i := leaderID; ; {
		server := ck.servers[i]
		reply := GetReply{}
		//fmt.Printf("client[%d] -> Server[%d] [GET请求] [key:%v]\n", ck.clientId, i, key)
		ok := server.Call("KVServer.Get", &args, &reply)
		if reply.Err == ErrWrongLeader || ok == false || reply.Err == ErrTimeOut {
			i++
			i %= len(ck.servers)
			if i == leaderID {
				time.Sleep(time.Millisecond * 100)
			}
			continue
		}
		ck.mutex.Lock()
		ck.leaderID = i
		//fmt.Printf("client[%d] <- Server[%d] [Get响应] [status:%s] [LeaderID:%d] [key:%v] [value:%v]\n", ck.clientId, i, reply.Err, reply.LeaderID, key, reply.Value)
		ck.mutex.Unlock()
		if reply.Err == ErrNoKey {
			return ""
		}
		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mutex.Lock()
	ck.commandID++
	CommandID := ck.commandID
	var leaderID int
	if ck.leaderID == -1 {
		leaderID = 0
	} else {
		leaderID = ck.leaderID
	}
	ck.mutex.Unlock()
	args := PutAppendArgs{key, value, op, ck.clientId, CommandID}

	for i := leaderID; ; {
		server := ck.servers[i]
		reply := PutAppendReply{}
		//fmt.Printf("client[%d] -> Server[%d] [%s请求] [命令id = %d] [key:%v] [value:%v]\n", ck.clientId, i, op, CommandID, key, value)
		ok := server.Call("KVServer.PutAppend", &args, &reply)
		if reply.Err == ErrWrongLeader || ok == false || reply.Err == ErrTimeOut {
			i++
			i %= len(ck.servers)
			if i == leaderID {
				time.Sleep(time.Millisecond * 100)
			}
			continue
		}
		ck.mutex.Lock()
		ck.leaderID = i
		//fmt.Printf("client[%d] <- Server[%d] [%s响应]，[status:%s] [LeaderID:%d] [key:%v]\n", ck.clientId, i, op, reply.Err, ck.leaderID, key)
		ck.mutex.Unlock()
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
