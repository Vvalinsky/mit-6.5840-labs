package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const TaskInterval = 200

var nReduce int

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

func checkError(err error, format string, v ...interface{}) {
	if err != nil {
		log.Fatalf(format, v)
	}
}

func CallCoordinator() (int, bool) {
	args := GetReduceCountArgs{}
	reply := GetReduceCountReply{}

	ok := call("Coordinator.GetReduceCount", &args, &reply)

	return reply.ReduceCount, ok
}

func CallCoordinatorTask() (*GetTaskReply, bool) {
	args := GetTaskArgs{os.Getpid()}
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)

	return &reply, ok
}

func writeMapOutput(kva []KeyValue, mapID int) {
	// IO buffers to batch the data sent to the disk
	prefix := fmt.Sprintf("%v/mr-%v", TempDir, mapID)
	files := make([]*os.File, 0, nReduce)
	buffers := make([]*bufio.Writer, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)

	// Create temp files, use pid to identify workers
	for i := 0; i < nReduce; i++ {
		filePath := fmt.Sprintf("%v-%v-%v", prefix, i, os.Getpid())
		file, err := os.Create(filePath) // create file path on disk
		checkError(err, "Failed to create file mapoutput: %v\n", filePath)
		buf := bufio.NewWriter(file) // buffer for file to batch sends to minimize I/O operations (buf stored in RAM, file in Disk)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf)) // creates encoder that writes to buf
	}

	// Write map output to temp files
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		err := encoders[idx].Encode(&kv) // serializes data to json and then writes it to buffer
		checkError(err, "Failed to encode key-value pair %v\n", kv)
	}

	// flush file buffer to disk
	for i, buff := range buffers {
		err := buff.Flush()
		checkError(err, "Cannot flush buffer for file: %v\n", files[i].Name())
	}

	// atomically rename temp files to ensure no one observers partial file
	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		err := os.Rename(file.Name(), newPath)
		checkError(err, "Failed to rename file %v to %v\n", file.Name(), newPath)
	}

}

func writeReduceOutput(kvMap map[string][]string, reducef func(string, []string) string, reduceID int) {
	keys := make([]string, 0, len(kvMap))

	for k := range kvMap {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	// Create temp file
	filePath := fmt.Sprintf("%v/mr-out-%v-%v", TempDir, reduceID, os.Getpid())
	file, err := os.Create(filePath)
	checkError(err, "Failed to create file reduceoutput: %v\n", filePath)

	// call reduce and write to temp file
	for _, key := range keys {
		reduceVal := reducef(key, kvMap[key])
		_, err := fmt.Fprintf(file, "%v %v\n", key, reduceVal)
		checkError(err, "Cannot write mr output (%v, %v) to file", key, reduceVal)
	}

	// atomically rename temp files to ensure no one observers partial file
	file.Close()
	newPath := fmt.Sprintf("mr-out-%v", reduceID)
	err = os.Rename(filePath, newPath)
	checkError(err, "Failed to rename file %v to %v\n", filePath, newPath)
}

func executeMapTask(mapf func(string, string) []KeyValue, filePath string, TaskID int) {
	file, err := os.Open(filePath)
	checkError(err, "Failed to open file %s\n", filePath)

	content, err := ioutil.ReadAll(file)
	checkError(err, "Failed to read file %s\n", filePath)
	file.Close()

	kva := mapf(filePath, string(content))

	writeMapOutput(kva, TaskID)
}

func executeReduceTask(reducef func(string, []string) string, filePath string, reduceID int) {
	files, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TempDir, "*", reduceID))
	if err != nil {
		checkError(err, "Cannot list reduce files")
	}

	kvMap := make(map[string][]string)
	var kv KeyValue

	for _, file := range files {
		file, err := os.Open(file)
		checkError(err, "Failed to open file %s\n", file.Name())

		decoder := json.NewDecoder(file)

		for decoder.More() {
			err := decoder.Decode(&kv)
			checkError(err, "Failed to decode key-value pair from file: %v\n", filePath)
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	writeReduceOutput(kvMap, reducef, reduceID)
}

func reportTaskCompletion(taskType TaskType, TaskID int) (bool, bool) {
	ReportArgs := ReportTaskArgs{WorkerID: os.Getpid(), Type: taskType, TaskID: TaskID}
	ReportReply := ReportTaskReply{}

	ok := call("Coordinator.ReportTask", &ReportArgs, &ReportReply)

	return ReportReply.CanExit, ok
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	ReplyCount, ok := CallCoordinator()

	if !ok {
		fmt.Printf("Failed to get reduce count from coordinator\n")
		return
	}

	nReduce = ReplyCount

	for {
		// ask for a task
		reply, ok := CallCoordinatorTask()

		if !ok {
			// assume something went wrong with coordinator and stop, doesn't really account for fault-tolerance
			// in terms of coordinator failure
			fmt.Println("Failed to get task from coordinator") // need an exit condition or it'll go forever
			return
		}

		if reply.Type == ExitTask {
			fmt.Println("All tasks have completed, worker exiting")
			return
		}

		exit, success := false, true

		switch reply.Type {
		case NoTask:
			// MapReduce hasn't finished yet
			// All map or reduce tasks has already been assigned
		case MapTask:
			executeMapTask(mapf, reply.File, reply.TaskID)
			exit, success = reportTaskCompletion(MapTask, reply.TaskID)
		case ReduceTask:
			executeReduceTask(reducef, reply.File, reply.TaskID)
			exit, success = reportTaskCompletion(ReduceTask, reply.TaskID)
		}

		if exit || !success {
			fmt.Println("Master exited or all tasks have been completed, worker exiting.")
			return
		}

		time.Sleep(TaskInterval * time.Millisecond)
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
