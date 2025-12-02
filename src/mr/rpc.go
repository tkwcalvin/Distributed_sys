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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type ArgsGetTask struct {
	WorkerID int
}

type ReplyGetTask struct {
	Filename     string
	MapTaskID    int
	ReduceTaskID int
	TaskType     int // 0: map task; 1: reduce task; 2: wait; 3: exit
	NReduce      int
}

type ArgsReportTaskDone struct {
	WorkerID int
	TaskType int // 0: map task; 1: reduce task
	TaskID   int // MapTaskID or ReduceTaskID
}

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
