package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"log/slog"
	"net/rpc"
	"os"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	hasMoreWork := true

	for hasMoreWork {
		hasMoreWork = RequestAndExecuteTask(mapf, reducef) && hasMoreWork
	}
}

func RequestAndExecuteTask(mapf func(string, string) []KeyValue, reducef func(string, []string) string) bool {
	task := CallRequestTask()

	switch taskType := task.Type; taskType {
	case Map:
		ExecuteMap(task.MapTask, mapf)
	case Reduce:
		ExecuteReduce(task.ReduceTask, reducef)
	default:
		isDone := CallIsMapReduceDone()
		return !isDone
	}

	go CallMarkTaskAsComplete(task.Index)
	return true
}

func ExecuteMap(mapTask MapTask, mapf func(string, string) []KeyValue) {
	interimFiles := make([]*os.File, mapTask.NReduce)
	for i := 0; i < mapTask.NReduce; i++ {
		interimFileName := "mr-" + strconv.Itoa(mapTask.TaskNumber) + "-" + strconv.Itoa(i)
		file, _ := os.Create(interimFileName)
		interimFiles[i] = file
	}

	file, _ := os.Open(mapTask.File)
	contentBytes, _ := io.ReadAll(file)
	content := string(contentBytes)

	result := mapf(mapTask.File, content)

	interimFileStrings := make([]map[string][]string, mapTask.NReduce)
	for i := 0; i < mapTask.NReduce; i++ {
		interimFileStrings[i] = map[string][]string{}
	}

	for _, kv := range result {
		reduceTask := ihash(kv.Key) % mapTask.NReduce
		interimFileStrings[reduceTask][kv.Key] = append(interimFileStrings[reduceTask][kv.Key], kv.Value)
	}

	for index, interimFile := range interimFiles {
		serializedMapBytes, _ := json.Marshal(interimFileStrings[index])
		serializedMap := string(serializedMapBytes)

		interimFile.WriteString(serializedMap + "\n")
		interimFile.Close()
	}
}

func ExecuteReduce(reduceTask ReduceTask, reducef func(string, []string) string) {
	output := map[string][]string{}

	for _, fileName := range reduceTask.Files {
		inFile, _ := os.Open(fileName)

		scanner := bufio.NewReader(inFile)

		for {
			kvText, err := scanner.ReadString('\n')

			if err != nil {
				break
			}

			var kv map[string][]string
			json.Unmarshal([]byte(kvText), &kv)

			for key, value := range kv {
				output[key] = append(output[key], value...)
			}
		}

		inFile.Close()
	}

	reduceOutput := make([]string, len(output))
	index := 0
	for key, value := range output {
		reduceOutput[index] = key + " " + reducef(key, value) + "\n"
		index++
	}

	outFile, _ := os.Create(reduceTask.OutFile)

	for _, line := range reduceOutput {
		outFile.WriteString(line)
	}

	outFile.Close()

}

func CallRequestTask() Task {

	// declare an argument structure.
	args := RequestTaskArgs{}

	// declare a reply structure.
	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)

	if ok {
		return reply.Task
	} else {
		slog.Error("call failed!\n")
		return reply.Task
	}
}

func CallMarkTaskAsComplete(index int) {
	args := MarkTaskAsCompleteArgs{}
	args.Index = index

	reply := MarkTaskAsCompleteReply{}

	ok := call("Coordinator.MarkTaskAsComplete", &args, &reply)

	if !ok {
		slog.Error("call failed!\n")
	}
}

func CallIsMapReduceDone() bool {
	args := IsMapReduceDoneArgs{}

	reply := IsMapReduceDoneReply{}

	ok := call("Coordinator.IsMapReduceDone", &args, &reply)

	if ok {
		return reply.IsDone
	} else {
		return false
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
