package mapreduce

import (
	"io/ioutil"
	"fmt"
	"os"
	"strings"
	"sort"
	"encoding/json"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	f, err := os.Create(outFile)
	if err != nil {
		fmt.Println(err)
		return
	}
	enc := json.NewEncoder(f)
	//extract key/values from json
	for i:= 0; i < nMap; i++ {
	content, err := ioutil.ReadFile(reduceName(jobName, i,reduceTask))
	if err != nil {
		fmt.Println(err)
		return
	}
	dec := json.NewDecoder(strings.NewReader(string(content)))
	var kvs = []KeyValue{}
	var kv KeyValue
	for {
		err := dec.Decode(&kv)
		kvs = append(kvs, kv)
		if (err != nil) {
			break;
		}
	}

	// sort the intermediate key/value pairs by key
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	// reduce operations
	var reducedKVs = []KeyValue{}
	if (len(kvs) > 0) {
		var currKey = kvs[0].Key
		var currVals = []string{kvs[0].Value}
		for i:= 1; i < len(kvs); i++ {
			if kvs[i].Key == currKey {
				currVals = append(currVals, kvs[i].Value)
				continue
			}
			reducedKV := KeyValue{currKey, reduceF(currKey, currVals)}
			reducedKVs = append(reducedKVs, reducedKV)
			currKey = kvs[i].Key
			currVals = []string{kvs[0].Value}
		}
		reducedKVs = append(reducedKVs, KeyValue{currKey, reduceF(currKey, currVals)})
	}

	// write to json
	for _, kv := range reducedKVs {
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}
f.Close()
}
