package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
	"sync"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	bytesData, err := ioutil.ReadFile(inFile) // read file data
	checkError(err)
	data := string(bytesData)
	records := mapF(inFile, data)           // turn file data into slice of KeyValue
	partitionSize := len(records) / nReduce // set numRecord/R ratio
	fmt.Printf("Total record: %d, R: %d, parSize: %d\n", len(records), nReduce, partitionSize)

	wg := new(sync.WaitGroup)

	for reduceTask := 1; reduceTask <= nReduce; reduceTask++ {
		var startIdx, endIdx int

		if reduceTask == 1 {
			startIdx = 0
		} else {
			startIdx = (reduceTask-1)*partitionSize + 1
		}

		if reduceTask == nReduce {
			endIdx = reduceTask*partitionSize - 1
		} else {
			endIdx = reduceTask * partitionSize
		}

		wg.Add(1)
		go func(start, end, reduceTask int) {
			//fmt.Printf("Partitioning at index %d to index %d\n", start, end)

			// create partitioned file with format "...[mapTask]-[reduceTask].txt"
			fileName := reduceName(jobName, mapTaskNumber, reduceTask-1)
			f, err := os.Create(fileName)
			checkError(err)

			// turn data from source file into JSON format and copy to paritioned file
			for i := start; i <= end; i++ {
				b, err := json.Marshal(records[i])
				checkError(err)
				data := string(b) + "\n"
				f.Write([]byte(data))
			}
			wg.Done()
			fmt.Printf("Done mapping to %s\n", fileName)
		}(startIdx, endIdx, reduceTask)
	}

	wg.Wait() // wait for all go-routines to complete partitioning

	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	// Use checkError to handle errors.

}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
