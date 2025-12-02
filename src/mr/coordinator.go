package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	nReduce                int
	TaskList               []string
	UnstartedMapTaskIdx    int
	UnstartedReduceTaskIdx int
	WorkID                 int
	mu                     sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *ArgsGetTask, reply *ReplyGetTask) error {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.UnstartedMapTaskIdx < len(c.TaskList) {
		reply.Filename = c.TaskList[c.UnstartedMapTaskIdx]
		reply.MapTaskID = c.UnstartedMapTaskIdx
		reply.TaskType = 0 // map task
		reply.NReduce = c.nReduce
		c.UnstartedMapTaskIdx++
	} else if c.UnstartedReduceTaskIdx < c.nReduce {
		reply.TaskType = 1 // reduce task
		reply.ReduceTaskID = c.UnstartedReduceTaskIdx
		c.UnstartedReduceTaskIdx++
	} else {
		reply.TaskType = 3 // exit
	}

	return nil
}

func (c *Coordinator) reportTaskDone(args *ArgsReportTaskDone, reply *ReplyGetTask) error {
	// Your code here.

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
	ret := false

	// Your code here.
	if c.UnstartedReduceTaskIdx >= c.nReduce {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.TaskList = files
	c.UnstartedMapTaskIdx = 0
	c.UnstartedReduceTaskIdx = 0

	c.server()
	return &c
}
