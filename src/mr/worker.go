package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	for {
		req := Request{}
		reply := Task{}
		ok := CallGetTask(&req, &reply)
		if ok != nil {
			panic(ok)
		}
		if reply.Type == 3 { // 当前已经结束
			return
		} else if reply.Type == 2 { // 无任务等待
			time.Sleep(time.Second)
			continue
		} else {
			ID := reply.ID
			if reply.Type == 0 { // map任务
				fileName := reply.FileName
				data, err := ioutil.ReadFile(fileName)
				if err != nil {
					fmt.Println("文件读取失败")
					panic(err)
				}
				kva := mapf(fileName, string(data))
				tempFileList := []*os.File{}
				encList := []*json.Encoder{}
				for i := 0; i < reply.ReduceTaskNum; i++ { // 创建临时文件，做准备工作
					tempFile, err := ioutil.TempFile("", "zxc666")
					if err != nil {
						panic(err)
					}
					tempFileList = append(tempFileList, tempFile)
					enc := json.NewEncoder(tempFile)
					encList = append(encList, enc)
				}
				for _, v := range kva {
					idx := ihash(v.Key) % reply.ReduceTaskNum
					enc := encList[idx]
					if err := enc.Encode(v); err != nil {
						fmt.Println("写入临时文件失败")
						panic(err)
					}
				}
				for _, file := range tempFileList {
					file.Close()
				}
				timeStamp := getTimeStamp()
				if timeStamp-reply.TimeStamp > 10*1000 {
					fmt.Printf("ID为 %v 任务已超时\n", ID)
					for _, v := range tempFileList {
						os.Remove(v.Name())
					}
					time.Sleep(time.Second)
					continue
				}
				ask := Ask{
					ID:        ID,
					TimeStamp: getTimeStamp(),
				}
				res := Response{}
				err = CallTaskIsFinished(&ask, &res)
				if err != nil {
					panic(err)
				}
				if res.OK {
					for i, file := range tempFileList {
						newFileName := "mr-" + strconv.Itoa(ID) + "-" + strconv.Itoa(i)
						err = os.Rename(file.Name(), "./"+newFileName)
						if err != nil {
							panic(err)
						}
					}
				} else {
					fmt.Printf("ID为 %v 任务已超时\n", ID)
					for _, v := range tempFileList {
						os.Remove(v.Name())
					}
				}

			} else { // reduce任务
				intermediate := []KeyValue{}
				for i := 0; i < reply.MapTaskNum; i++ {
					fileName := "./mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(ID)
					file, err := os.Open(fileName)
					if err != nil {
						panic(err)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}
					file.Close()
				}
				sort.Sort(ByKey(intermediate))
				tempFile, err := ioutil.TempFile("", "zxc666")
				if err != nil {
					panic(err)
				}
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)
					fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
					i = j
				}
				tempFile.Close()
				timeStamp := getTimeStamp()
				ask := Ask{
					ID:        ID,
					TimeStamp: timeStamp,
				}
				res := Response{}
				err = CallTaskIsFinished(&ask, &res)
				if err != nil {
					panic(err)
				}
				if res.OK {
					oname := "./mr-out-" + strconv.Itoa(ID)
					os.Rename(tempFile.Name(), oname)
				} else {
					os.Remove(tempFile.Name())
				}
			}
			time.Sleep(time.Second)
		}

	}
}

func CallGetTask(request *Request, reply *Task) error { // 向coordinator获取任务
	ok := call("Coordinator.AssignTask", request, reply)
	if ok {
		// reply.Y should be 100.
		//fmt.Println(*reply)
		fmt.Printf("Get the task's name is %v\n", reply.FileName)
		return nil
	} else {
		fmt.Printf("Call failed!\n")
		return errors.New("Call failed!\n")
	}
}

func CallTaskIsFinished(ask *Ask, res *Response) error {
	ok := call("Coordinator.AskTask", ask, res)
	if ok {
		fmt.Printf("The %v task is finished\n", ask.ID)
		return nil
	} else {
		fmt.Printf("Call Task is finished failed!\n")
		return errors.New("Call Task is finished failed!\n")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
