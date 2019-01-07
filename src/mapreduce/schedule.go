package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	//first put all tasks into channel ch, and marked them as "TODO"
	var waitGroup sync.WaitGroup
	tasks := make(chan int, ntasks)
	for i:= 0; i < ntasks; i++ {
		waitGroup.Add(1)
		tasks <- i
	}

	go func() {
		fmt.Printf("this thread is running\n")
		waitGroup.Wait()
		fmt.Printf("ready to clse tasks")
		close(tasks)
	}()

	for i := range tasks {
		fmt.Printf("i = %d\n", i)
		go func(i int) {
			worker := <- registerChan
			var file string
			if phase == mapPhase {
				file = mapFiles[i]
			}
			var taskArgs = DoTaskArgs{jobName, file, phase, i, n_other}
			taskArgs.TaskNumber = i
			if call(worker, "Worker.DoTask", &taskArgs, nil) {
				registerChan <- worker
				waitGroup.Done()
			} else {
				tasks <- i
			}
		}(i)
	}



	fmt.Printf("Schedule: %v done\n", phase)
}
