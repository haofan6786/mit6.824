package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	idargs, idreply := MyArgs{}, WorkerID{}
	call("Coordinator.AskforWorkerId", &idargs, &idreply)
	workerId := idreply.ID
	log.Printf("Worker %v started\n", workerId)
	//args, reply := MyArgs{WorkerId: workerId}, MyReply{}
	// Your worker implementation here.
	for true {
		args, reply := MyArgs{WorkerId: workerId}, MyReply{}
		call("Coordinator.AskforTask", &args, &reply)
		//fmt.Println(reply)
		if reply.TaskId == -1 {
			time.Sleep(2 * time.Second)
			fmt.Println("no task...")
		} else if reply.TaskId == -2 {
			break
		} else {
			if reply.TaskInfo.TaskType == "Map" {
				//do map task
				filename := reply.TaskInfo.FileList[0]
				res := doMap(mapf, filename, reply.NumReduce, reply.TaskId, workerId)
				args.TaskId = reply.TaskId
				args.FileList = res
				args.TaskType = "Map"
				reply.Flag = false
				call("Coordinator.RespondforTask", &args, &reply)
				if reply.Flag == false {
					for _, filename := range args.FileList {
						os.Remove(filename)
					}
				}
			} else if reply.TaskInfo.TaskType == "Reduce" {
				res := doReduce(reply, reducef, workerId)
				time.Sleep(2)
				args.TaskId = reply.TaskId
				args.TaskType = "Reduce"
				args.FileList = res
				reply.Flag = false
				call("Coordinator.RespondforTask", &args, &reply)
				if reply.Flag == true {
					//respond success, remove the map intermediate files
					for _, filename := range reply.TaskInfo.FileList {
						os.Remove(filename)
					}
				} else {
					//respond file, remove reduce intermediate file
					log.Printf("remove %v", res[0])
					//os.Remove(oname)
				}
			}
		}
		reply.TaskId = -1
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func doMap(mapf func(string, string) []KeyValue, filename string, numreduce int, taskid int, workerid int) []string {
	file, err := os.Open(filename)
	res := make([]string, 0, 0)

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content))
	sort.Sort(ByKey(intermediate))

	oname := "map-%d-%d-%d"

	file_slice := make([]*json.Encoder, numreduce)
	for i := 0; i < len(file_slice); i++ {
		res = append(res, fmt.Sprintf(oname, taskid, workerid, i))
		mapfile, _ := os.Create(fmt.Sprintf(oname, taskid, workerid, i))
		file_slice[i] = json.NewEncoder(mapfile)
	}
	for _, kv := range intermediate {
		err := file_slice[ihash(kv.Key)%numreduce].Encode(&kv)
		if err != nil {
			log.Fatalf("json write error:", err)
		}
	}
	return res
}

func doReduce(args MyReply, reducef func(string, []string) string, id int) []string {
	intermediate := []KeyValue{}
	for _, filename := range args.TaskInfo.FileList {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		kva := []KeyValue{}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		intermediate = append(intermediate, kva...)
		file.Close()
	}
	tmpname := fmt.Sprintf("mr-out-%d-%d", (args.TaskId-args.NumMap)%args.NumReduce, id)
	finalname := fmt.Sprintf("mr-out-%d", (args.TaskId-args.NumMap)%args.NumReduce)

	ofile, _ := os.Create(tmpname)
	sort.Sort(ByKey(intermediate))
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	return []string{tmpname, finalname}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
