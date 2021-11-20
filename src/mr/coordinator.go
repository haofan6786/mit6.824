package mr

import (
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files          []string
	taskList       []Task
	lock           sync.Mutex
	nextWorkerID   int
	finishedMap    int
	finishedReduce int
	numReduce      int
	numMap         int
}

type Task struct {
	FileList   []string
	TaskType   string
	TaskStatus string
	WorkerId   int
	DeadTime   time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskforTask(args *MyArgs, reply *MyReply) error {
	c.lock.Lock()
	if c.finishedMap < len(c.files) {
		for taskId, task := range c.taskList {
			if task.TaskType == "Map" && task.TaskStatus == "idle" {
				reply.TaskId = taskId
				reply.TaskInfo = task
				reply.NumReduce = c.numReduce
				reply.NumMap = c.numMap
				c.taskList[taskId].TaskStatus = "in-process"
				c.taskList[taskId].WorkerId = args.WorkerId
				c.taskList[taskId].DeadTime = time.Now().Add(10 * time.Second)
				log.Printf("Map Task %d was distributed to Worker %v\n", taskId, args.WorkerId)
				break
			}
		}
	} else if c.finishedReduce < c.numReduce {
		for taskId, task := range c.taskList {
			if task.TaskType == "Reduce" && task.TaskStatus == "idle" {
				reply.TaskId = taskId
				reply.TaskInfo = task
				reply.NumReduce = c.numReduce
				reply.NumMap = c.numMap
				c.taskList[taskId].TaskStatus = "in-process"
				c.taskList[taskId].WorkerId = args.WorkerId
				c.taskList[taskId].DeadTime = time.Now().Add(10 * time.Second)
				log.Printf("Reduce Task %d was distributed to Worker %v\n", taskId, args.WorkerId)
				break
			}
		}
	} else {
		reply.TaskId = -1
	}
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) RespondforTask(args *MyArgs, reply *MyReply) error {
	c.lock.Lock()
	if args.WorkerId == c.taskList[args.TaskId].WorkerId {
		//match the worker id
		reply.Flag = true
		c.taskList[args.TaskId].TaskStatus = "complete"
		if args.TaskType == "Map" {
			c.finishedMap += 1
			for _, filename := range args.FileList {
				words := strings.Split(filename, "-")
				partition, _ := strconv.Atoi(words[len(words)-1])
				reduceId := len(c.files) + partition
				c.taskList[reduceId].FileList = append(c.taskList[reduceId].FileList, filename)
			}
			log.Printf("Worker %v finished Map Task %d", args.WorkerId, args.TaskId)
			_ = 1
		} else {
			c.finishedReduce += 1
			log.Printf("Worker %v finished Ruduce Task %d", args.WorkerId, args.TaskId)
			_ = 1
		}
	}
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) AskforWorkerId(args *MyArgs, reply *WorkerID) error {
	c.lock.Lock()
	reply.ID = c.nextWorkerID
	c.nextWorkerID++
	c.lock.Unlock()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() { //
	// example to show how to declare the arguments
	// and reply for an RPC.
	//
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
	ret := true

	// Your code here.
	c.lock.Lock()
	if c.finishedReduce == c.numReduce {
		ret = true
	} else {
		ret = false
	}
	c.lock.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//fmt.Println(files)
	c := Coordinator{
		taskList:       make([]Task, len(files)+nReduce, len(files)+nReduce),
		files:          files,
		finishedMap:    0,
		finishedReduce: 0,
		nextWorkerID:   1,
		numReduce:      nReduce,
		numMap:         len(files),
	}

	// Your code here.
	c.lock.Lock()
	taskId := 0
	for _, fileName := range c.files {
		c.taskList[taskId] = Task{
			[]string{fileName},
			"Map",
			"idle",
			-1,
			time.Now(),
		}
		taskId++
	}
	for taskId := len(c.files); taskId < len(files)+nReduce; taskId++ {
		c.taskList[taskId] = Task{
			[]string{},
			"Reduce",
			"idle",
			-1,
			time.Now(),
		}
	}
	c.lock.Unlock()
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)

			c.lock.Lock()
			for taskId, task := range c.taskList {
				if c.taskList[taskId].TaskStatus == "in-process" && time.Now().After(c.taskList[taskId].DeadTime) {
					// 回收并重新分配
					log.Printf(
						"Found timed-out %s task %d previously running on worker %d. Prepare to re-assign",
						task.TaskType, taskId, task.WorkerId)
					c.taskList[taskId].WorkerId = -1
					c.taskList[taskId].TaskStatus = "idle"
				}
			}
			c.lock.Unlock()
		}
	}()
	log.Printf("Coordinator start\n")
	c.server()
	return &c
}
