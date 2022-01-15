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
	TaskType_Done   TaskType = 3

	intermediate_file_pre string = "wsi"
)

//<-

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type EmptyArgs struct{}
type EmptyReply struct{}

//request task->
type RequestTask_Args struct {
}
type RequestTask_Reply struct {
	Vtask_type       TaskType
	Vtask_file_paths []string
	Vtask_key        int
	Vn_reduce        int
}

//<-

//map done->
type MapDoneArgs struct {
	Vdone_files []string
	Vtask_key   int
}

//<-

//reduce done->
type ReduceDoneArgs struct {
	//Vdone_files []string
	Vtask_key int
}
type ReduceDoneReply struct {
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
