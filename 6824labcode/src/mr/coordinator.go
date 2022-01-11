package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	map_tasks      MapTasks
	map_done_files map[string]int

	mu sync.Mutex

	reduce_tasks ReduceTasks
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

type MapTasks struct {
	n_sum int

	unhandled_tasks map[int]Task
	handling_tasks  map[int]Task
	handled_tasks   map[int]Task
}

func (mt *MapTasks) load_tasks(files []string) {
	mt.n_sum = len(files)
	for i := 0; i < len(files); i++ {
		mt.unhandled_tasks[i] = Task{
			task_key:  i,
			task_type: TaskType_Map,
			task_str:  files[i],
		}
	}
}
func (mt *MapTasks) is_all_done() bool {
	return mt.n_sum == len(mt.handled_tasks)
}

type ReduceTasks struct {
	n_sum int

	unhandled_tasks map[int]Task
	handling_tasks  map[int]Task
	handled_tasks   map[int]Task
}

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
	if maptask != nil {
		reply.task_type = TaskType_Map
		reply.task_file_path = maptask.task_str
		reply.task_key = maptask.task_key
		//reply one maptask
	}
	if c.map_tasks.is_all_done() {
		//map 都完成了，就准备发送 reduce

	} else {
		println("request task failed, " +
			"no unhandled map task, " +
			"exist map task being handled")
	}
	return nil
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.map_tasks.load_tasks(files)

	// Your code here.

	c.server()
	return &c
}
