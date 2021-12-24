package mapreduce

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	target := "he"
	fmt.Println("Start merging")
	records := make(map[string][]string)

	for i := 0; i < nMap; i++ { // scan all (nMap * nReduce) files
		mappedFileName := reduceName(jobName, i, reduceTaskNumber) // identify mapped file
		fmt.Println("Merging file", mappedFileName)

		bytesData, err := ioutil.ReadFile(mappedFileName)
		checkError(err)

		jsonData := strings.Fields(string(bytesData))

		// read JSON data from mappedFile into KeyValue slice
		for i := 0; i < len(jsonData); i++ {
			var x KeyValue
			err := json.Unmarshal([]byte(jsonData[i]), &x)

			//fmt.Printf("JSON val: %v\n", x)
			if err == io.EOF {
				break
			}

			if _, ok := records[x.Key]; !ok {
				records[x.Key] = []string{x.Value}
			} else {
				records[x.Key] = append(records[x.Key], x.Value)
			}
		}

		// if _, ok := records[target]; ok {
		// 	fmt.Println("Key", target, ", val:", records[target])
		// }

		fmt.Println("Done merging file", mappedFileName)
	}

	mergeFile := mergeName(jobName, reduceTaskNumber) // set output file name
	f, err := os.Create(mergeFile)
	checkError(err)
	enc := json.NewEncoder(f)

	for key, vals := range records {
		if key == target {
			fmt.Println("Encoding key", key, "with vals", reduceF(key, vals))
		}
		enc.Encode(KeyValue{key, reduceF(key, vals)})
	}
	f.Close()

	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Use checkError to handle errors.
}
