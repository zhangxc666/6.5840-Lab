package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientID  int64  // 表示当前的命令ID值
	CommandID int64  // 当前client的ID
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err      Err
	LeaderID int
}

type GetArgs struct {
	Key      string
	ClientID int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err      Err
	Value    string
	LeaderID int
}
