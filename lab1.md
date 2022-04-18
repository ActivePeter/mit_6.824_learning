## lab1

todo

- [ ] 定时监测超时任务，重新派发

#### cordinator

```go
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

func (c *Coordinator) RequestTask_handler(args *RequestTask_Args, reply *RequestTask_Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
    maptask := c.take_one_map_task()//取出一个map task(如果有的话)

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


//map执行完后回调
func (c *Coordinator) MapDone_handler(args *MapDoneArgs, reply *RequestTask_Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Println("MapDone_handler", args.Vtask_key)

	//record handled files,
    //遍历中间文件名，对应reduceid存入map，便于后续reduce派发
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


//reduce 完成，移除task
func (c *Coordinator) ReduceDone_handler(args *ReduceDoneArgs, reply *EmptyReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	bak := c.reduce_tasks.handling_tasks[args.Vtask_key]
	delete(c.reduce_tasks.handling_tasks, args.Vtask_key)
	c.reduce_tasks.handled_tasks[args.Vtask_key] = bak

	log.Println("one reduce done, left", len(c.reduce_tasks.unhandled_tasks))

	return nil
}
```

#### worker轮询

```go
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
```

#### mapworker

```go
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
        //存储为taskid_reduceid文件名（防止不同task存向一个文件
		saved_files[i] = mapworker_save_one_reduce(task.Vtask_key, reduce_id, kv)
		//log.Println("one intermediate file saved ", saved_files[i])
		i++
	}
	//结束，返回文件名序列给coordinator
	MapDone_call(task.Vtask_key, saved_files)
}

//map[int][]KeyValue reduceid->kvs
func mapworker_turn_kvs_into_map(values []KeyValue, n_reduce int) map[int][]KeyValue {
	save_set__reduce_id_2_value := make(map[int][]KeyValue) 
	for _, kv := range values {
        //对key取hash散列到reduce数量范围上
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
```

#### reduceworker

```go
func reduce_worker(
   task *RequestTask_Reply,
   reducef func(string, []string) string,
) {
   log.Println("reduce started", task.Vtask_key)
   //读取所有reduce需要处理的中间文件存储的kv序列
   key_values := reduce_worker_read_save(task, task.Vtask_file_paths)
   log.Println("reduce_worker: cnt of intermediate:", len(key_values))
   if key_values != nil {
      oname := "mr-out-" + strconv.Itoa(task.Vtask_key)
      ofile, _ := os.Create(oname)
      //log.Println("reduce test1", task.Vtask_key)
      for key, values := range key_values {
         log.Println("reduce test in for", task.Vtask_key)
         //将values链表转为数组
         values_arr := make([]string, values.Len())
         index := 0
         for i := values.Front(); i != nil; i = i.Next() {
            values_arr[index] = i.Value.(string)
            index++
         }
         //reduce操作
         output := reducef(key, values_arr)
          
         //对应key的reduce结果写入文件
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
```