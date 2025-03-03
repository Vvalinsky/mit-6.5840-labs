package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const TempDir = "tmp"
const TaskTimeout = 10

type TaskState int
type TaskType int

const (
	Idle       TaskState = iota // Task is not yet started
	InProgress                  // Task is currently being worked on
	Completed                   // Task is finished
)

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
	ExitTask
)

type Task struct {
	State    TaskState
	WorkerID int
	Type     TaskType
	File     string
	Index    int
}

type Coordinator struct {
	mu          sync.Mutex
	reduceTasks []Task
	mapTasks    []Task
	nReduce     int
	nMap        int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) SelectTask(tasks []Task, workerID int) *Task {
	for i := 0; i < len(tasks); i++ {
		if tasks[i].State == Idle {
			tasks[i].State = InProgress
			tasks[i].WorkerID = workerID
			return &tasks[i]
		}
	}
	return &Task{State: Completed, WorkerID: -1, Type: NoTask, File: "", Index: -1}
}

func (c *Coordinator) GetReduceCount(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.ReduceCount = len(c.reduceTasks)

	return nil
}

func (c *Coordinator) WaitForTask(task *Task) {
	// wait for task to be completed
	if task.Type != MapTask && task.Type != ReduceTask {
		return
	}

	<-time.After(TaskTimeout * time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()

	if task.State == InProgress {
		task.State = Idle
		task.WorkerID = -1
	}

}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()

	var task *Task

	if c.nMap > 0 {
		task = c.SelectTask(c.mapTasks, args.WorkerID)
	} else if c.nReduce > 0 {
		task = c.SelectTask(c.reduceTasks, args.WorkerID)
	} else {
		task = &Task{State: Completed, WorkerID: -1, Type: ExitTask, File: "", Index: -1}
	}

	reply.Type = task.Type
	reply.TaskID = task.Index
	reply.File = task.File

	c.mu.Unlock()

	// Fault Tolerance
	go c.WaitForTask(task)

	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task

	switch args.Type {
	case MapTask:
		task = &c.mapTasks[args.TaskID]
	case ReduceTask:
		task = &c.reduceTasks[args.TaskID]
	default:
		fmt.Println("Invalid task type %v\n", args.Type)
		return nil
	}

	if args.WorkerID == task.WorkerID && task.State == InProgress {
		task.State = Completed
		if args.Type == MapTask && c.nMap > 0 {
			c.nMap--
		} else if args.Type == ReduceTask && c.nReduce > 0 {
			c.nReduce--
		}
	}

	reply.CanExit = c.nMap == 0 && c.nReduce == 0

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)  // Coordinator methods can be called via rpc
	rpc.HandleHTTP() // rpc calls are now handled via http
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()        // get a unique socket name
	os.Remove(sockname)                  // removes any existing UNIX socket file from prior runs
	l, e := net.Listen("unix", sockname) // listens on UNIX socket
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) // starts http server, runs in the background, and listens for RPCs and processes them concurrently
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.nMap == 0 && c.nReduce == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	nMap := len(files)
	c.nMap = nMap
	c.nReduce = nReduce
	c.reduceTasks = make([]Task, 0, nReduce)
	c.mapTasks = make([]Task, 0, nMap)

	for i := 0; i < nMap; i++ {
		c.mapTasks = append(c.mapTasks, Task{State: Idle, WorkerID: -1, Type: MapTask, File: files[i], Index: i})
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{State: Idle, WorkerID: -1, Type: ReduceTask, File: "", Index: i})
	}

	c.server()

	// create temp directory
	outFiles, _ := filepath.Glob("mr-out*")
	for _, f := range outFiles {
		if err := os.RemoveAll(f); err != nil {
			log.Fatal("Cannot remove file: %v\n", f)
		}
	}

	err := os.RemoveAll(TempDir)

	if err != nil {
		log.Fatalf("Cannot remove temp directory %v\n", TempDir)
	}

	err = os.Mkdir(TempDir, 0755)

	if err != nil {
		log.Fatalf("Cannot create temp directory %v\n", TempDir)
	}

	return &c
}
