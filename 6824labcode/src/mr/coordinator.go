package mr

import (
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
	map_tasks      Tasks
	reduce_tasks   Tasks
	n_reduce       int
	map_done_files map[int][]string //reduce_id 2 files
	mu             sync.Mutex
	reduce_mode    bool
	done           bool
}
type Task struct {
	task_key  int
	task_type TaskType
	task_strs []string
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
			log.Println("take one reduce task", task.task_key)
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

func (mt *Tasks) init_tasks() {
	mt.unhandled_tasks = make(map[int]Task)
	mt.handling_tasks = make(map[int]Task)
	mt.handled_tasks = make(map[int]Task)
}
func (mt *Tasks) load_tasks(files []string) {
	mt.n_sum = len(files)
	for i := 0; i < len(files); i++ {
		task_strs := make([]string, 1)
		task_strs[0] = files[i]
		mt.unhandled_tasks[i] = Task{
			task_key:  i,
			task_type: TaskType_Map,
			task_strs: task_strs,
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

//handlers
func (c *Coordinator) RequestTask_handler(args *RequestTask_Args, reply *RequestTask_Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	maptask := c.take_one_map_task()

	//1. 有未完成的maptask,
	if maptask != nil {
		reply.Vtask_type = TaskType_Map
		reply.Vtask_file_paths = make([]string, 1)
		reply.Vtask_file_paths = maptask.task_strs
		reply.Vtask_key = maptask.task_key
		reply.Vn_reduce = c.n_reduce
		//reply one maptask
		return nil
	}
	//2. map 都完成了，就准备发送 reduce
	if c.map_tasks.is_all_done() {
		//1.检查是否切换到reduce状态，并作初始化
		c.switch_2_reduce_mode_if_not()
		reduce_task := c.take_one_reduce_task()
		if reduce_task != nil {
			reply.Vtask_type = TaskType_Reduce
			reply.Vtask_file_paths = reduce_task.task_strs
			reply.Vtask_key = reduce_task.task_key
			reply.Vn_reduce = c.n_reduce
		} else {
			//println("request reduce task failed, " +
			//	"tasks were all finished")
			if c.reduce_tasks.is_all_done() {
				reply.Vtask_type = TaskType_Done
				c.done = true
			}
		}
	} else { //3. 存在map未完成，但是都在执行中，那么就输出分派失败的信息
		tasks:=""
		for _, task := range c.map_tasks.handling_tasks {
			tasks=tasks+" "+strconv.Itoa(task.task_key)
		}
		println("request task failed, " +
			"no unhandled map task, " +
			"exist map tasks are being handled\n   ",
			tasks,
		)
	}
	return nil
}
func (c *Coordinator) MapDone_handler(args *MapDoneArgs, reply *RequestTask_Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Println("MapDone_handler", args.Vtask_key)

	//record handled files
	for _, fname := range args.Vdone_files {
		result := strings.Split(fname, "_")
		if len(result) == 3 {
			reduce_id, err := strconv.Atoi(result[2])
			if err != nil {
				log.Fatal("wrong intermediate file name decode")
			}
			file_arr, ok := c.map_done_files[reduce_id]
			if !ok {
				file_arr = make([]string, 0)
				c.map_done_files[reduce_id] = file_arr
			}
			c.map_done_files[reduce_id] = append(file_arr, fname)

			//log.Println("reduce len1", len(file_arr))
			//log.Println("reduce len2", len())
		} else {
			log.Fatal("wrong intermediate file name split")
		}
		//c.map_done_files[k] = v
	}

	bak := c.map_tasks.handling_tasks[args.Vtask_key]
	delete(c.map_tasks.handling_tasks, args.Vtask_key)
	c.map_tasks.handled_tasks[args.Vtask_key] = bak

	return nil
}

func (c *Coordinator) ReduceDone_handler(args *ReduceDoneArgs, reply *EmptyReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	bak := c.reduce_tasks.handling_tasks[args.Vtask_key]
	delete(c.reduce_tasks.handling_tasks, args.Vtask_key)
	c.reduce_tasks.handled_tasks[args.Vtask_key] = bak

	log.Println("one reduce done, left", len(c.reduce_tasks.unhandled_tasks))

	return nil
}

//检查是否切换到reduce状态，并作初始化
func (c *Coordinator) switch_2_reduce_mode_if_not() {
	if !c.reduce_mode {
		//reduce_id2files:=make(map[string][]string)
		for reduce_id, files := range c.map_done_files {

			//从文件路径中提取对应的reduce任务编号
			t := Task{
				task_type: TaskType_Reduce,
				task_strs: files,
				task_key:  reduce_id,
			}
			c.reduce_tasks.unhandled_tasks[reduce_id] = t

			log.Printf("reduce task load %+v", t)
		}
		c.reduce_tasks.n_sum = len(c.reduce_tasks.unhandled_tasks)
		c.reduce_mode = true
	}
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

	//c.map_tasks =make(map[string]map[int]int)
	c.map_tasks.init_tasks()
	c.reduce_tasks.init_tasks()
	c.n_reduce = nReduce
	c.map_tasks.load_tasks(files)
	c.done = false
	c.reduce_mode = false
	c.map_done_files = make(map[int][]string)

	c.server()
	return &c
}
