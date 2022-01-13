package mr

import (
	"bufio"
	"container/list"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

type _Pub struct{}

var pub _Pub

func (p *_Pub) handle_error(err error) {
	fmt.Println(err)
}

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
		} else if task.task_type == TaskType_Reduce {
			reduce_worker(task, reducef)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}
func reduce_worker(
	task *RequestTask_Reply,
	reducef func(string, []string) string,
) {
	key_values := reduce_worker_read_save(task.task_file_path)
	if key_values != nil {
		oname := "mr-out-" + strconv.Itoa(task.task_key)
		ofile, _ := os.Create(oname)

		for key, values := range key_values {
			values_arr := make([]string, values.Len())
			index := 0
			for i := values.Front(); i != nil; i = i.Next() {
				values_arr[index] = i.Value.(string)
				index++
			}
			output := reducef(key, values_arr)
			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", key, output)
		}
		ofile.Close()
	} else {
		log.Fatalf("error no key values read from the intermediate")
	}
}
func reduce_worker_read_save(saved_file_path string) map[string]*list.List {
	f, err := os.Open(saved_file_path)
	if err != nil {
		pub.handle_error(err)
	} else {
		br := bufio.NewReader(f)
		key_values := make(map[string]*list.List)
		for {

			l, e := br.ReadString('\n')

			if e != nil && len(l) == 0 {
				if e != nil {
					pub.handle_error(e)
				}
				break
			}
			kv := strings.Split(l, "|")
			if len(kv) == 2 {
				v, has := key_values[kv[0]]
				if has {
					v.PushBack(kv[1])
				} else {
					key_values[kv[0]] = list.New()
					key_values[kv[0]].PushBack(kv[1])
				}
			} else {
				panic("error kv split ")
			}
			//os.Stdout.Write(l)

		}
		return key_values
	}
	return nil
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
		save_set[mapworker_save_one_kv(&kv, task.n_reduce)] = 1
	}
	//结束，返回文件序列给coordinator
}
func mapworker_save_one_kv(kv *KeyValue, n_reduce int) string {
	filename := intermediate_file_pre + strconv.Itoa(ihash(kv.Key)%n_reduce)
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
