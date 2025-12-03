package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 任务状态枚举
type TaskStatus int

const (
	TaskPending   TaskStatus = iota // 未分配
	TaskRunning                     // 执行中
	TaskCompleted                   // 已完成
)

type TaskContent struct {
	Filename string
	NReduce  int
}

// 任务结构体
type Task struct {
	ID         int          // 任务唯一标识
	Content    *TaskContent // 任务内容（根据实际需求定义，比如文件名、计算参数等）
	AssignTime time.Time    // 分配时间（用于判断超时）
	Status     TaskStatus   // 任务状态
	TaskType   TaskType     // 任务类型
}

// Coordinator 结构体扩展（添加任务管理相关字段）
type Coordinator struct {
	mu                      sync.Mutex    // 保护共享资源（任务池、Worker 列表）
	nReduce                 int           // reduce任务数量
	taskQueue               []*Task       // 待分配任务队列（未分配 + 超时重入的任务）
	nextTaskID              int           // 下一个任务ID（自增生成）
	timeout                 time.Duration // 任务超时时间（10秒）
	finishedMapTaskCount    int           // 已完成map任务数量
	finishedReduceTaskCount int           // 已完成reduce任务数量
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *ArgsGetTask, reply *Task) error {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finishedReduceTaskCount == c.nReduce {
		reply.TaskType = ExitTask
		return nil
	}
	for c.nextTaskID < len(c.taskQueue) {
		if c.taskQueue[c.nextTaskID].Status == TaskPending ||
			(c.taskQueue[c.nextTaskID].Status == TaskRunning && time.Since(c.taskQueue[c.nextTaskID].AssignTime) > c.timeout) {
			// Copy task content to reply
			c.taskQueue[c.nextTaskID].AssignTime = time.Now()
			c.taskQueue[c.nextTaskID].Status = TaskRunning
			*reply = *c.taskQueue[c.nextTaskID]
			log.Printf("assigned task type %d, task id %d, worker id %d\n", c.taskQueue[c.nextTaskID].TaskType, c.nextTaskID, args.WorkerID)
			return nil
		}
		c.nextTaskID++
	}
	c.nextTaskID = 0
	reply.TaskType = WaitTask
	return nil
}

func (c *Coordinator) ReportTaskDone(args *ArgsReportTaskDone, reply *ReplyReportTaskDone) error {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case MapTask:
		if c.taskQueue[args.TaskID].Status == TaskRunning {
			c.finishedMapTaskCount++
			log.Printf("finished map task %d\n", args.TaskID)
			if c.finishedMapTaskCount == len(c.taskQueue) {
				c.init_reduce_task()
				return nil
			}
		}
	case ReduceTask:
		if c.taskQueue[args.TaskID].Status == TaskRunning {
			c.finishedReduceTaskCount++
			log.Printf("finished reduce task %d\n", args.TaskID)
		}
	}
	c.taskQueue[args.TaskID].Status = TaskCompleted
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
	if c.finishedReduceTaskCount == c.nReduce {
		ret = true
	}

	return ret
}

func (c *Coordinator) init_map_task(files []string, nReduce int) {
	c.taskQueue = make([]*Task, 0)
	for i, file := range files {
		task := Task{
			ID:       i,
			Content:  &TaskContent{Filename: file, NReduce: nReduce},
			Status:   TaskPending,
			TaskType: MapTask,
		}
		c.taskQueue = append(c.taskQueue, &task)
	}
	// print task queue
	// for _, task := range c.taskQueue {
	// 	log.Printf("Task %d: %v\n", task.ID, task.Content)
	// }
	c.nextTaskID = 0
}

func (c *Coordinator) init_reduce_task() {
	c.taskQueue = make([]*Task, 0)
	for i := 0; i < c.nReduce; i++ {
		task := Task{
			ID:       i,
			Content:  &TaskContent{},
			Status:   TaskPending,
			TaskType: ReduceTask,
		}
		c.taskQueue = append(c.taskQueue, &task)
	}
	c.nextTaskID = 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.timeout = 10 * time.Second
	c.init_map_task(files, nReduce)

	c.server()
	return &c
}
