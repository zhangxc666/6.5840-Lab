package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type set map[int]struct{}

type queue chan int

type Coordinator struct {
	// Your definitions here.
	unMapFile     queue         // 未开始的map输入文件名的队列
	mappingFile   set           // 正在进行map的文件名集合
	mappedFile    queue         // 已经完成的map的文件名集合
	unReduceFile  queue         // 未开始的reduce输入文件名的集合
	reducingFile  set           // 正在进行reduce的文件名集合
	reducedFile   queue         // 已经完成的reduce的文件名集合
	recordTime    map[int]int64 // 记录文件开始的时间 <id->时间>
	fileMutex     sync.Mutex    // fileset的锁
	timeMutex     sync.Mutex    // recordTime的锁
	stateMutex    sync.Mutex    // state的锁
	reduceTaskNum int           // reduce任务的数量
	mapTaskNum    int           // map任务数量
	state         int           // 0表示map 1表示reduce 2表示当前已经结束了
	files         []string      // 保存文件对应id->filename

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.stateMutex.Lock()
	curTimeStamp := getTimeStamp()
	var startIng set
	var unStart queue
	if c.state == 0 {
		startIng = c.mappingFile
		unStart = c.unMapFile
	} else if c.state == 1 {
		startIng = c.reducingFile
		unStart = c.unReduceFile
	}
	c.fileMutex.Lock()
	c.timeMutex.Lock()
	idList := []int{}
	for id, lastTimeStamp := range c.recordTime {
		if curTimeStamp-lastTimeStamp > 10*1000 {
			idList = append(idList, id)
			delete(startIng, id)
			unStart <- id
		}
	}
	c.fileMutex.Unlock()
	for _, id := range idList {
		delete(c.recordTime, id)
	}
	c.timeMutex.Unlock()
	defer c.stateMutex.Unlock()
	return c.state == 2
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

func max(x, y int) int {
	if x < y {
		return y
	} else {
		return x
	}
}

func getTimeStamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTaskNum := len(files)
	c := Coordinator{
		unMapFile:     make(queue, mapTaskNum),
		mappedFile:    make(queue, mapTaskNum),
		mappingFile:   make(set, mapTaskNum),
		reducingFile:  make(set, nReduce),
		reducedFile:   make(queue, nReduce),
		unReduceFile:  make(queue, nReduce),
		recordTime:    make(map[int]int64, max(mapTaskNum, nReduce)),
		reduceTaskNum: nReduce,
		mapTaskNum:    mapTaskNum,
		state:         0,
		files:         files,
	}
	for i := 0; i < mapTaskNum; i++ {
		c.unMapFile <- i
	}
	for i := 0; i < nReduce; i++ {
		c.unReduceFile <- i
	}
	c.server()
	return &c
}

func (c *Coordinator) AssignTask(request *Request, reply *Task) error { // 分配任务
	var unStart queue
	var startIng set
	if c.state == 0 { // 当前在map阶段
		unStart = c.unMapFile
		startIng = c.mappingFile
	} else if c.state == 1 { // 当前在reduce阶段
		unStart = c.unReduceFile
		startIng = c.reducingFile
	} else { // 当前已经结束了
		reply.Type = 3
		return nil
	}
	if len(unStart) == 0 { // 无任务可分配
		reply.Type = 2
		return nil
	}
	{ // 分配一个任务出去
		ID := <-unStart
		reply.Type = c.state
		if c.state == 0 {
			reply.FileName = c.files[ID]
		}
		reply.ID = ID
		reply.MapTaskNum = c.mapTaskNum
		reply.ReduceTaskNum = c.reduceTaskNum
		reply.TimeStamp = getTimeStamp()
		fmt.Println(reply)
		c.fileMutex.Lock()
		startIng[ID] = struct{}{}
		c.fileMutex.Unlock()
		c.timeMutex.Lock()
		c.recordTime[ID] = reply.TimeStamp
		c.timeMutex.Unlock()
	}
	return nil
}

func (c *Coordinator) AskTask(ask *Ask, res *Response) error { // mater处理当前已完成的任务
	c.timeMutex.Lock()
	defer func() {
		c.timeMutex.Unlock()
	}()
	ID := ask.ID
	if timeStamp, ok := c.recordTime[ID]; ok {
		if timeStamp-ask.TimeStamp > 1000*10 {
			res.OK = false
		} else {
			res.OK = true
			delete(c.recordTime, ID)
			var mp set
			var ch queue
			if c.state == 0 {
				mp = c.mappingFile
				ch = c.mappedFile
			} else {
				mp = c.reducingFile
				ch = c.reducedFile
			}
			c.fileMutex.Lock()
			delete(mp, ID)
			c.fileMutex.Unlock()
			ch <- ID
		}
	} else {
		res.OK = false
	}
	c.stateMutex.Lock()
	if len(c.reducedFile) == c.reduceTaskNum {
		c.state = 2
	} else if len(c.mappedFile) == c.mapTaskNum {
		c.state = 1
	}
	c.stateMutex.Unlock()
	return nil
}
