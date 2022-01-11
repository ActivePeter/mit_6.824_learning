package mr

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

	// Your worker implementation here.
	for {
		task := RequestTask_Call()
		if task.task_type == TaskType_Map {
			mapworker(task, mapf)
		}

	}
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}
func mapworker(task *RequestTask_Reply,
	mapf func(string, string) []KeyValue) {
	filename := task.task_file_path
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	//kva为map完的一组数据
	kva := mapf(filename, string(content))
	//存储发生变更的文件
	save_set := make(map[string]int)
	for _, kv := range kva {
		save_set[mapworker_save_one_kv(&kv)] = 1
	}
	//结束，返回文件序列给coordinator
}
func mapworker_save_one_kv(kv *KeyValue) string {
	filename := "wsi_" + string(ihash(kv.Key))
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	} else {
		writer := bufio.NewWriter(file)
		_, err := writer.WriteString(kv.Key + "|" + kv.Value + "\n")
		if err != nil {
			log.Fatalf("cannot write string")
			return ""
		}
		err = writer.Flush()
		if err != nil {
			log.Fatalf("cannot flush")
			return ""
		}
	}
	return filename
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func RequestTask_Call() *RequestTask_Reply {
	args := RequestTask_Args{}
	reply := RequestTask_Reply{}
	call("Coordinator.RequestTask", &args, &reply)
	fmt.Printf("reply.task_type %v\n", reply.task_type)
	return &reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
