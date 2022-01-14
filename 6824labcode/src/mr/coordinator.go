package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	map_tasks    Tasks
	reduce_tasks Tasks

	map_done_files map[string]int
	mu             sync.Mutex
	reduce_mode    bool
	done           bool
}
type Task struct {
	task_key  int
	task_type TaskType
	task_str  string
}

func (c *Coordinator) take_one_map_task() *Task {
	if len(c.map_tasks.unhandled_tasks) > 0 {
		for i := range c.map_tasks.unhandled_tasks {
			task := c.map_tasks.unhandled_tasks[i] //暂存第i
			delete(c.map_tasks.unhandled_tasks, i) //移除第i
			c.map_tasks.handling_tasks[i] = task
			return &task
		}
	}
	return nil
}
func (c *Coordinator) take_one_reduce_task() *Task {
	if len(c.reduce_tasks.unhandled_tasks) > 0 {
		for i := range c.reduce_tasks.unhandled_tasks {
			task := c.reduce_tasks.unhandled_tasks[i] //暂存第i
			delete(c.reduce_tasks.unhandled_tasks, i) //移除第i
			c.reduce_tasks.handling_tasks[i] = task
			return &task
		}
	}
	return nil
}

type Tasks struct {
	n_sum int

	unhandled_tasks map[int]Task
	handling_tasks  map[int]Task
	handled_tasks   map[int]Task
}

func (mt *Tasks) load_tasks(files []string) {
	mt.n_sum = len(files)
	for i := 0; i < len(files); i++ {
		mt.unhandled_tasks[i] = Task{
			task_key:  i,
			task_type: TaskType_Map,
			task_str:  files[i],
		}
	}
}
func (mt *Tasks) is_all_done() bool {
	return mt.n_sum == len(mt.handled_tasks)
}

//type ReduceTasks struct {
//	n_sum int
//
//	unhandled_tasks map[int]Task
//	handling_tasks  map[int]Task
//	handled_tasks   map[int]Task
//}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) RequestTask_Handler(args *RequestTask_Args, reply *RequestTask_Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	maptask := c.take_one_map_task()

	//1. 有未完成的maptask,
	if maptask != nil {
		reply.task_type = TaskType_Map
		reply.task_file_path = maptask.task_str
		reply.task_key = maptask.task_key
		//reply one maptask
		return nil
	}
	//2. map 都完成了，就准备发送 reduce
	if c.map_tasks.is_all_done() {
		//1.检查是否切换到reduce状态，并作初始化
		c.switch_2_reduce_mode_if_not()
		reduce_task := c.take_one_reduce_task()
		if reduce_task != nil {
			reply.task_type = TaskType_Reduce
			reply.task_file_path = reduce_task.task_str
			reply.task_key = reduce_task.task_key
		} else {
			println("request task failed, " +
				"tasks were all finished")
			if c.reduce_tasks.is_all_done() {
				c.done = true
			}
		}
	} else { //3. 存在map未完成，但是都在执行中，那么就输出分派失败的信息
		println("request task failed, " +
			"no unhandled map task, " +
			"exist map task being handled")
	}
	return nil
}

//检查是否切换到reduce状态，并作初始化
func (c *Coordinator) switch_2_reduce_mode_if_not() {
	if !c.reduce_mode {
		for k, _ := range c.map_done_files {
			result := strings.Split(k, intermediate_file_pre)
			if len(result) == 2 {
				//从文件路径中提取对应的reduce任务编号
				v, _ := strconv.Atoi(result[1])
				t:=Task{
					task_type: TaskType_Reduce,
					task_str: k,
					task_key: v,
				}
				c.reduce_tasks.unhandled_tasks[v]=t

				fmt.Printf("reduce task load %+v",t)

			} else {
				println("error: wrong map_done_files record ",result)
			}
		}
		c.reduce_tasks.n_sum= len(c.reduce_tasks.unhandled_tasks)
		c.reduce_mode = true
	}
}
func (c *Coordinator) MapDoneArgs_handler(args *MapDoneArgs) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	//record handled files
	for k, v := range args.done_files {
		c.map_done_files[k] = v
	}

	bak := c.map_tasks.handling_tasks[args.task_key]
	delete(c.map_tasks.handling_tasks, args.task_key)
	c.map_tasks.handled_tasks[args.task_key] = bak

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	//ret := false

	// Your code here.

	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.map_tasks.load_tasks(files)
	c.done = false
	c.reduce_mode = false
	c.server()
	return &c
}
