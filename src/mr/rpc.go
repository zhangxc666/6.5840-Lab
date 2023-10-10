package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Request struct {
}

type Task struct {
	Type          int    // 0是map，1是reduce，2是无任务，让worker等待,3是结束
	ID            int    // 当前任务的序号
	FileName      string // 当前任务的文件名
	MapTaskNum    int    // maptask的总数量
	ReduceTaskNum int    // reducetask的总数量
	TimeStamp     int64  // 获取当前任务的时间戳
}

type Ask struct {
	ID        int   // 完成的任务id
	TimeStamp int64 // 完成任务的时间戳
}

type Response struct { // master返回worker的确认
	OK bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
