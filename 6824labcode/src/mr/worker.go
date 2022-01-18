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
		//fmt.Printf("reply.task_type %v\n", task.Vtask_type)
		if task.Vtask_type == TaskType_Map {
			mapworker(task, mapf)
		} else if task.Vtask_type == TaskType_Reduce {
			reduce_worker(task, reducef)
		} else if task.Vtask_type == TaskType_Done {
			break
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}
func reduce_worker(
	task *RequestTask_Reply,
	reducef func(string, []string) string,
) {
	log.Println("reduce started", task.Vtask_key)
	key_values := reduce_worker_read_save(task, task.Vtask_file_paths)
	log.Println("reduce_worker: cnt of intermediate:", len(key_values))
	if key_values != nil {
		oname := "mr-out-" + strconv.Itoa(task.Vtask_key)
		ofile, _ := os.Create(oname)
		//log.Println("reduce test1", task.Vtask_key)
		for key, values := range key_values {
			log.Println("reduce test in for", task.Vtask_key)
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
		log.Println("reduce task end", task.Vtask_key)
		ReduceDone_call(task.Vtask_key)
	} else {
		log.Fatalf("error no key values read from the intermediate")
	}
}
func reduce_worker_read_save(task *RequestTask_Reply, saved_file_paths []string) map[string]*list.List {
	key_values := make(map[string]*list.List)

	for _, saved_file_path := range saved_file_paths {
		//log.Println("reduce_worker", task.Vtask_key, "try open ", saved_file_path)

		f, err := os.Open(saved_file_path)
		if err != nil {
			log.Fatal(" err:", err)
		} else {
			br := bufio.NewReader(f)
			for {
				l, e := br.ReadString('\n')

				if e != nil && len(l) == 0 {
					//read to end
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
					log.Fatal("error kv split ")
				}
			}
		}
	}
	return key_values
}
func mapworker(task *RequestTask_Reply,
	mapf func(string, string) []KeyValue) {
	filename := task.Vtask_file_paths[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open original %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read original %v", filename)
	}
	file.Close()
	//log.Println("mapworker original read ok")
	//kva为map完的一组数据
	kva := mapf(filename, string(content))
	//存储发生变更的文件
	reduce_id_2_kv := mapworker_turn_kvs_into_map(kva, task.Vn_reduce)

	saved_files := make([]string, len(reduce_id_2_kv))
	i := 0
	for reduce_id, kv := range reduce_id_2_kv {
		saved_files[i] = mapworker_save_one_reduce(task.Vtask_key, reduce_id, kv)
		//log.Println("one intermediate file saved ", saved_files[i])
		i++
	}
	//结束，返回文件序列给coordinator
	MapDone_call(task.Vtask_key, saved_files)
}
func mapworker_turn_kvs_into_map(values []KeyValue, n_reduce int) map[int][]KeyValue {
	save_set__reduce_id_2_value := make(map[int][]KeyValue)
	for _, kv := range values {
		reduce_id := ihash(kv.Key) % n_reduce
		kvs, ok := save_set__reduce_id_2_value[reduce_id]
		if !ok {
			kvs = make([]KeyValue, 0)
			save_set__reduce_id_2_value[reduce_id] = kvs
		}
		save_set__reduce_id_2_value[reduce_id] = append(kvs, kv)
	}
	return save_set__reduce_id_2_value
}
func mapworker_save_one_reduce(map_id int, reduce_id int, values []KeyValue) string {
	filename := intermediate_file_pre +
		"_" + strconv.Itoa(map_id) +
		"_" + strconv.Itoa(reduce_id)

	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	for err != nil {
		log.Fatalf("weird, cannot open %v", filename)
	}

	{
		writer := bufio.NewWriter(file)
		for _, kv := range values {
			_, err := writer.WriteString(kv.Key + "|" + kv.Value + "\n")
			if err != nil {
				log.Fatalf("cannot write string")
				return ""
			}
		}
		err = writer.Flush()
		if err != nil {
			log.Fatalf("cannot flush")
			return ""
		}
	}
	file.Close()
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
	call("Coordinator.RequestTask_handler", &args, &reply)
	//fmt.Printf("reply.task_type %v\n", reply.Vtask_type)
	return &reply
}

func MapDone_call(map_task_key int, done_files []string) {
	args := MapDoneArgs{}
	args.Vtask_key = map_task_key
	args.Vdone_files = done_files
	reply := EmptyReply{}
	call("Coordinator.MapDone_handler", &args, &reply)
}

func ReduceDone_call(reduce_task_key int) {
	args := ReduceDoneArgs{Vtask_key: reduce_task_key}
	reply := EmptyReply{}
	call("Coordinator.ReduceDone_handler", &args, &reply)
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
