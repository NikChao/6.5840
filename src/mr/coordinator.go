package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

type Coordinator struct {
	// Your definitions here.
	Tasks []Task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	for index, task := range c.Tasks {
		if task.State == Idle {
			reply.Task = task
			c.Tasks[index].State = InProgress
			return nil
		}
	}

	return nil
}

func (c *Coordinator) MarkTaskAsComplete(args *MarkTaskAsCompleteArgs, reply *MarkTaskAsCompleteReply) error {
	c.Tasks[args.Index].State = Complete

	return nil
}

func (c *Coordinator) IsMapReduceDone(args *IsMapReduceDoneArgs, reply *IsMapReduceDoneReply) error {
	isDone := true
	for _, task := range c.Tasks {
		if task.State != Complete {
			isDone = false
		}
	}

	reply.IsDone = isDone
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for _, task := range c.Tasks {
		if task.State != Complete {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	nMap := len(files)
	nTasks := nMap + nReduce

	tasks := make([]Task, nTasks)
	for i, file := range files {
		mapTask := MapTask{}
		mapTask.File = file
		mapTask.TaskNumber = i
		mapTask.NReduce = nReduce

		task := Task{}
		task.Index = i
		task.State = Idle
		task.Type = Map
		task.MapTask = mapTask

		tasks[i] = task
	}

	for i := nMap; i < nTasks; i++ {
		reduceTask := ReduceTask{}

		reduceTaskNumber := i - nMap
		reduceTask.OutFile = "mr-out-" + strconv.Itoa(reduceTaskNumber)
		reduceTaskFiles := make([]string, nMap)
		for index := range reduceTaskFiles {
			reduceTaskFiles[index] = "mr-" + strconv.Itoa(index) + "-" + strconv.Itoa(reduceTaskNumber)
		}
		reduceTask.Files = reduceTaskFiles

		task := Task{}
		task.Index = i
		task.Type = Reduce
		task.ReduceTask = reduceTask

		tasks[i] = task
	}

	c.Tasks = tasks

	c.server()
	return &c
}
