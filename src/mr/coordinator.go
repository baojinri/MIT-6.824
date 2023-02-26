package mr

import (
	"log"
	"os"
	"time"
)

import "net"
import "net/rpc"
import "net/http"

type Coordinator struct {
	mapWait    chan TaskInfo
	mapRun     chan TaskInfo
	reduceWait chan TaskInfo
	reduceRun  chan TaskInfo
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AskTask(arg *TaskInfo, reply *TaskInfo) error {
	if len(c.mapWait) != 0 {
		*reply = <-c.mapWait
		reply.Status = "map"
		reply.Begin = getNowTime()
		c.mapRun <- *reply
		return nil
	}
	if len(c.mapWait) == 0 && len(c.mapRun) == 0 && len(c.reduceWait) != 0 {
		*reply = <-c.reduceWait
		reply.Status = "reduce"
		reply.Begin = getNowTime()
		c.reduceRun <- *reply
		return nil
	}
	reply.Status = "wait"
	return nil
}

func (c *Coordinator) TaskDone(arg *TaskInfo, reply *TaskInfo) error {
	if arg.Status == "map" {
		for {
			select {
			case item := <-c.mapRun:
				{
					if item.FileIndex != arg.FileIndex {
						c.mapRun <- item
					}
					if item.FileIndex == arg.FileIndex {
						return nil
					}
				}
			}
		}
	}
	if arg.Status == "reduce" {
		for {
			select {
			case item := <-c.reduceRun:
				{
					if item.NReduce != arg.NReduce {
						c.reduceRun <- item
					}
					if item.NReduce == arg.NReduce {
						return nil
					}
				}
			}
		}
	}
	return nil
}

//func (c *Coordinator) Echo(arg string, result *TaskInfo) error {
//	//*result = arg //在这里直接将第二个参数（也就是实际的返回值）赋值为arg
//	return nil //error返回nil，也就是没有发生异常
//}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		log.Fatal("error registering", err)
		return
	}
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	if len(c.mapWait) == 0 && len(c.mapRun) == 0 && len(c.reduceWait) == 0 && len(c.reduceRun) == 0 {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{mapWait: make(chan TaskInfo, nReduce), mapRun: make(chan TaskInfo, nReduce), reduceWait: make(chan TaskInfo, nReduce), reduceRun: make(chan TaskInfo, nReduce)}

	for i := 0; i < len(files); i++ {
		item := TaskInfo{FileName: files[i], FileIndex: i, NReduce: nReduce, NFiles: len(files)}
		c.mapWait <- item
	}
	for i := 0; i < nReduce; i++ {
		item := TaskInfo{FileName: "aaa", FileIndex: -1, NReduce: i + 1, NFiles: len(files)}
		c.reduceWait <- item
	}

	go c.removeTimeOut()

	c.server()
	return &c
}

func (c *Coordinator) removeTimeOut() {
	for {
		time.Sleep(time.Second)
		if len(c.mapRun) != 0 {
			select {
			case item := <-c.mapRun:
				{
					if getNowTime()-item.Begin < 10 {
						c.mapRun <- item
					} else {
						c.mapWait <- item
					}
				}
			}
		}
		if len(c.reduceRun) != 0 {
			select {
			case item := <-c.reduceRun:
				{
					if getNowTime()-item.Begin < 10 {
						c.reduceRun <- item
					} else {
						c.reduceWait <- item
					}
				}
			}
		}
	}
}

func getNowTime() int64 {
	return time.Now().UnixNano() / int64(time.Second)
}
