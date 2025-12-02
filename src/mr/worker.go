package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
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
		args := ArgsGetTask{}
		args.WorkerID = -1
		reply := ReplyGetTask{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if ok {
			switch reply.TaskType {
			case 0:
				fmt.Printf("Received Map Task: %s, MapTaskID: %d\n", reply.Filename, reply.MapTaskID)
			case 1:
				fmt.Printf("Received Reduce Task: ReduceTaskID: %d\n", reply.ReduceTaskID)
			case 2:
				fmt.Printf("Received Wait Task\n")
			case 3:
				fmt.Printf("Received Exit Task\n")
			}

		} else {
			fmt.Printf("call failed!\n")
			return
		}

		switch reply.TaskType {
		case 0:
			doMapTask(mapf, reply.Filename, reply.MapTaskID, reply.NReduce)

		case 1:
			doReduceTask(reducef, reply.ReduceTaskID)

		case 2:
			// Wait task: do nothing, just return
			time.Sleep(1 * time.Second)
		case 3:
			// Exit task: log and return
			log.Printf("Worker %d: No task assigned, exiting.", args.WorkerID)
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

func doMapTask(mapf func(string, string) []KeyValue, filename string, mapTaskID int, nReduce int) {
	// Your code here.
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content))

	// 1. 定义文件列表切片：存储所有创建的 tempFile（*os.File 类型）
	var openFiles []*os.File

	// 3. 循环创建临时文件，加入列表
	for i := 0; i < nReduce; i++ {
		// 自定义的文件名（仅用于标识，实际文件是 os.CreateTemp 生成的临时文件）

		// 创建临时文件：存放目录 "tmp"，前缀 "mr-"
		tempFile, err := os.CreateTemp("", "mr-")
		if err != nil {
			// 错误处理：打印真实错误+自定义文件名，方便排查
			log.Fatalf("创建临时文件失败 (%s): %v", tempFile.Name(), err)
		}

		// 关键：将创建的 tempFile 追加到文件列表
		openFiles = append(openFiles, tempFile)

	}

	defer func() {
		for i, file := range openFiles {
			// 先关闭文件（释放资源，确保数据写入完成）
			if err := file.Close(); err != nil {
				log.Printf("关闭文件 %s 失败：%v", file.Name(), err)
			}
			intermediateFilename := "mr-" + strconv.Itoa(mapTaskID) + "-" + strconv.Itoa(i)
			os.Rename(file.Name(), intermediateFilename)
		}
	}()

	sort.Sort(ByKey(intermediate))
	i := 0

	for i < len(intermediate) {
		j := i
		reduceTaskID := ihash(intermediate[i].Key) % nReduce
		tempFile := openFiles[reduceTaskID]
		enc := json.NewEncoder(tempFile)
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			err := enc.Encode(&intermediate[j])
			if err != nil {
				log.Fatalf("cannot encode %v", intermediate[j])
			}
			// log.Printf("临时文件真实路径：%s", tempFile.Name())

			j++
		}
		i = j
	}
}

func doReduceTask(reducef func(string, []string) string, reduceTaskID int) {
	// Your code here.

	suffixStr := strconv.Itoa(reduceTaskID) // 转换为字符串，方便匹配

	// 1. 读取 tmp 目录（单层，不递归）
	entries, err := os.ReadDir("./")
	if err != nil {
		log.Fatalf("读取目录 %s 失败：%v", "", err)
		return
	}

	// 2. 遍历目录，筛选后缀为 targetSuffix 的文件
	var keyValues []KeyValue
	for _, entry := range entries {
		// 跳过目录，只处理文件
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		// 按 "-" 分割文件名，判断最后一段是否为目标后缀
		parts := strings.Split(filename, "-")
		// 文件名格式需满足 "xxx-xxx-i"（至少 3 段，最后一段是数字）
		if len(parts) >= 3 && parts[len(parts)-1] == suffixStr {
			// 拼接完整路径（避免相对路径歧义）

			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("无法打开文件 %s: %v", filename, err)
			}

			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				keyValues = append(keyValues, kv)
			}
		}
	}
	sort.Sort(ByKey(keyValues))

	i := 0
	ofile, _ := os.CreateTemp("", "mr-out-")
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
	ofile.Close()
	finalName := "mr-out-" + suffixStr
	os.Rename(ofile.Name(), finalName)
}

func done() bool {

	return false
}
