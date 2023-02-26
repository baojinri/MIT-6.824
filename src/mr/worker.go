package mr

import (
	"encoding/json"
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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	arg := TaskInfo{}
	reply := TaskInfo{}

	//err = c.Call(rpcname, args, reply)
	for true {
		_ = call("Coordinator.AskTask", &arg, &reply)

		switch reply.Status {
		case "map":
			workMap(mapf, &reply)
			break
		case "reduce":
			workReduce(reducef, &reply)
			break
		case "wait":
			time.Sleep(time.Second)
			break
		case "":
			fmt.Println("nothing")
			return
		}
	}
}

func workMap(mapf func(string, string) []KeyValue, taskinfo *TaskInfo) {

	intermediate := []KeyValue{}

	file, _ := os.Open(taskinfo.FileName)
	content, _ := ioutil.ReadAll(file)
	file.Close()
	kva := mapf(taskinfo.FileName, string(content))
	intermediate = append(intermediate, kva...)

	nReduce := taskinfo.NReduce
	kvas := make([][]KeyValue, nReduce)

	for i := 0; i < nReduce; i++ {
		kvas[i] = make([]KeyValue, 0)
	}
	for _, kv := range intermediate {
		index := ihash(kv.Key) % nReduce
		kvas[index] = append(kvas[index], kv)
	}
	for i := 0; i < nReduce; i++ {
		tempfile, err := os.CreateTemp(".", "mrtemp")
		if err != nil {
			fmt.Println("error")
		}
		enc := json.NewEncoder(tempfile)
		for _, kv := range kvas[i] {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println("Json encode failed")
			}
		}
		outname := fmt.Sprintf("mr-%v-%v", taskinfo.FileIndex, i)
		err = os.Rename(tempfile.Name(), outname)
		if err != nil {
			fmt.Println("error")
		}
	}

	arg := TaskInfo{}
	_ = call("Coordinator.TaskDone", &taskinfo, &arg)
	return
}

func workReduce(reducef func(string, []string) string, taskinfo *TaskInfo) {
	innameprefix := "mr-"
	innamesuffix := "-" + strconv.Itoa(taskinfo.NReduce-1)
	// read in all files as a kv array
	intermediate := []KeyValue{}
	for index := 0; index < taskinfo.NFiles; index++ {
		inname := innameprefix + strconv.Itoa(index) + innamesuffix
		file, err := os.Open(inname)
		if err != nil {
			fmt.Printf("Open intermediate file %v failed: %v\n", inname, err)
			panic("Open file error")
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
	tempfile, err := os.CreateTemp(".", "mrtemp")
	if err != nil {
		panic("Create file error")
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	outname := innameprefix + "out-" + strconv.Itoa(taskinfo.NReduce-1)
	os.Rename(tempfile.Name(), outname)

	arg := TaskInfo{}
	_ = call("Coordinator.TaskDone", &taskinfo, &arg)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
