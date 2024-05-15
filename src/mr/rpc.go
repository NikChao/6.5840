package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.
type TaskType string

const (
	Map    TaskType = "Map"
	Reduce TaskType = "Reduce"
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Complete
)

type MapTask struct {
	TaskNumber int
	File       string
	NReduce    int
}

type ReduceTask struct {
	Files   []string
	OutFile string
}

type Task struct {
	Index int
	Type  TaskType
	State TaskState

	MapTask    MapTask
	ReduceTask ReduceTask
}

type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	Task Task
}

type MarkTaskAsCompleteArgs struct {
	Index int
}

type MarkTaskAsCompleteReply struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
