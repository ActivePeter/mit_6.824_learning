package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

//task type def->
type TaskType int

const (
	TaskType_Map    TaskType = 1
	TaskType_Reduce TaskType = 2
)

//<-

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

//request task->
type RequestTask_Args struct {
}
type RequestTask_Reply struct {
	task_type      TaskType
	task_file_path string
	task_key       int
}

//<-

//map done->
type MapDoneArgs struct {
	done_files map[string]int
	task_key   int
}
//<-

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
