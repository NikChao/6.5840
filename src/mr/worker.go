package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	hasMoreWork := true

	for hasMoreWork {
		task := CallRequestTask()

		// reply.Y should be 100.
		switch taskType := task.Type; taskType {
		case Map:
			mapTask := task.MapTask

			interrimFiles := make([]*os.File, mapTask.NReduce)
			for i := 0; i < mapTask.NReduce; i++ {
				interrimFileName := "mr-" + strconv.Itoa(mapTask.TaskNumber) + "-" + strconv.Itoa(i)
				file, _ := os.Create(interrimFileName)
				interrimFiles[i] = file
			}

			file, _ := os.Open(mapTask.File)
			contentBytes, _ := io.ReadAll(file)
			content := string(contentBytes)

			result := mapf(mapTask.File, content)

			interrimFileStrings := make([]map[string][]string, mapTask.NReduce)
			for i := 0; i < mapTask.NReduce; i++ {
				interrimFileStrings[i] = map[string][]string{}
			}

			for _, kv := range result {
				reduceTask := ihash(kv.Key) % mapTask.NReduce
				interrimFileStrings[reduceTask][kv.Key] = append(interrimFileStrings[reduceTask][kv.Key], kv.Value)
			}

			for index, interrimFile := range interrimFiles {
				serializedMapBytes, _ := json.Marshal(interrimFileStrings[index])
				serializedMap := string(serializedMapBytes)

				interrimFile.WriteString(serializedMap + "\n")
				interrimFile.Close()
			}
		case Reduce:
			reduceTask := task.ReduceTask

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
		default:
			hasMoreWork = false
		}

		CallMarkTaskAsComplete(task.Index)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallRequestTask() Task {

	// declare an argument structure.
	args := RequestTaskArgs{}

	// declare a reply structure.
	reply := RequestTaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.RequestTask", &args, &reply)

	if ok {
		return reply.Task
	} else {
		log.Fatal("call failed!\n")
		return reply.Task
	}
}

func CallMarkTaskAsComplete(index int) {

	// declare an argument structure.
	args := MarkTaskAsCompleteArgs{}
	args.Index = index

	// declare a reply structure.
	reply := MarkTaskAsCompleteReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.MarkTaskAsComplete", &args, &reply)

	if !ok {
		log.Fatal("call failed!\n")
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
