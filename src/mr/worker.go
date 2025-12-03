package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		task, ok := getTask()
		if !ok {
			log.Fatalf("get task failed")
		}
		switch task.TaskType {
		case MapTask:
			doMapTask(mapf, task.Content.Filename, task.ID, task.Content.NReduce)
			reportTaskDone(MapTask, task.ID)
		case ReduceTask:
			doReduceTask(reducef, task.ID)
			reportTaskDone(ReduceTask, task.ID)

		case WaitTask:
			time.Sleep(1 * time.Second)
		case ExitTask:
			return
		}
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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

func getTask() (*Task, bool) {
	args := ArgsGetTask{}
	task := Task{}
	ok := call("Coordinator.GetTask", &args, &task)
	if ok {
		switch task.TaskType {
		case MapTask:
			log.Printf("Received Map Task, MapTaskID: %d\n", task.ID)
		case ReduceTask:
			log.Printf("Received Reduce Task, ReduceTaskID: %d\n", task.ID)
		case WaitTask:
			log.Printf("Received Wait Task\n")
		case ExitTask:
			log.Printf("Received Exit Task\n")
		}
		return &task, true

	} else {
		return nil, false
	}
}

func doMapTask(mapf func(string, string) []KeyValue, filename string, mapTaskID int, nReduce int) {

	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("read file %s failed: %v", filename, err)
	}
	// map
	intermediate := mapf(filename, string(content))

	// create temp files for storing intermediate results
	var openFiles []*os.File

	for range nReduce {
		tempFile, err := os.CreateTemp("", "mr-")
		if err != nil {
			log.Fatalf("create temp file failed: %v", err)
		}
		openFiles = append(openFiles, tempFile)

	}

	defer func() {
		for i, file := range openFiles {
			err := file.Close()
			if err != nil {
				log.Fatalf("close file %s failed: %v", file.Name(), err)
			}
			intermediateFilename := "mr-" + strconv.Itoa(mapTaskID) + "-" + strconv.Itoa(i)
			err = os.Rename(file.Name(), intermediateFilename)
			if err != nil {
				log.Fatalf("rename file %s failed: %v", file.Name(), err)
			}
		}
	}()

	sort.Sort(ByKey(intermediate))

	// encode intermediate results to temp files
	i := 0
	for i < len(intermediate) {
		j := i + 1
		reduceTaskID := ihash(intermediate[i].Key) % nReduce
		tempFile := openFiles[reduceTaskID]
		enc := json.NewEncoder(tempFile)
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		for k := i; k < j; k++ {
			err := enc.Encode(&intermediate[k])
			if err != nil {
				log.Fatalf("encode %v failed: %v", intermediate[k], err)
			}
		}
		i = j
	}
}

func doReduceTask(reducef func(string, []string) string, reduceTaskID int) {

	suffixStr := strconv.Itoa(reduceTaskID)
	entries, err := os.ReadDir("./")
	if err != nil {
		log.Fatalf("read working directory failed: %v", err)
		return
	}

	// traverse working directory, filter intermediate files with reduce task id
	var keyValues []KeyValue
	for _, entry := range entries {

		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		parts := strings.Split(filename, "-")
		// filename format should be "xxx-xxx-i" (at least 3 parts, the last part is a number)
		if len(parts) >= 3 && parts[len(parts)-1] == suffixStr {

			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("open file %s failed: %v", filename, err)
			}

			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				keyValues = append(keyValues, kv)
			}
			err = file.Close()
			if err != nil {
				log.Fatalf("close file %s failed: %v", filename, err)
			}
		}
	}
	sort.Sort(ByKey(keyValues))

	// create temp file for storing final results
	ofile, err := os.CreateTemp("", "mr-out-")
	if err != nil {
		log.Fatalf("create temp file failed: %v", err)
	}
	defer func() {
		err := ofile.Close()
		if err != nil {
			log.Fatalf("close file %s failed: %v", ofile.Name(), err)
		}
		finalName := "mr-out-" + suffixStr
		err = os.Rename(ofile.Name(), finalName)
		if err != nil {
			log.Fatalf("rename file %s failed: %v", ofile.Name(), err)
		}
	}()

	// reduce
	i := 0
	for i < len(keyValues) {
		j := i + 1
		for j < len(keyValues) && keyValues[j].Key == keyValues[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, keyValues[k].Value)
		}
		output := reducef(keyValues[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", keyValues[i].Key, output)
		i = j
	}

}

func reportTaskDone(taskType TaskType, taskID int) bool {
	return call("Coordinator.ReportTaskDone", &ArgsReportTaskDone{
		TaskType: taskType,
		TaskID:   taskID,
	}, &ReplyGetTask{})
}
