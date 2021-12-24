package mapreduce

import (
	"fmt"
	"strings"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("====\nSchedule: %v %v tasks (%d I/Os)\n====\n", ntasks, phase, nios)

	wg := new(sync.WaitGroup)
	fmt.Printf("Files: %d, R:%d\n", len(mr.files), mr.nReduce)

	fmt.Println("Start assigning workers")
	switch phase {
	case mapPhase:

		for i, f := range mr.files {
			wg.Add(1)

			go func(f string, i int) {
				args := DoTaskArgs{
					JobName:       mr.jobName,
					File:          f, // input file for map-task
					Phase:         mapPhase,
					TaskNumber:    i,
					NumOtherPhase: mr.nReduce,
				}

				// make sure the file f gets executed no matter what
				mr.scheduleTask(args, wg)

			}(f, i)
		}
	case reducePhase:
		for i := 0; i < mr.nReduce; i++ {
			wg.Add(1)

			go func(i int) {

				args := DoTaskArgs{
					JobName:       mr.jobName,
					Phase:         reducePhase,
					TaskNumber:    i,
					NumOtherPhase: len(mr.files),
				}

				mr.scheduleTask(args, wg)
			}(i)

		}
	}

	fmt.Printf("Waiting for %s workers to finish\n", phase)
	wg.Wait()
	fmt.Printf("%s workers finished\n", phase)

	// 2. Handout work to workers
	// tell the workers the name of the original input file (mr.files[task])
	// and the task task; each worker knows from which files to read its input
	// and to which files to write its output

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	debug("====\nSchedule: %v phase done\n====\n", phase)
}

// Main policy: automatically re-schedule if polled a failed worker

func (mr *Master) scheduleTask(args DoTaskArgs, wg *sync.WaitGroup) error {
	reply := ShutdownReply{}
	wkAddr := <-mr.registerChannel

	ok := call(wkAddr, "Worker.DoTask", args, &reply)

	if ok {
		wg.Done()
		mr.registerChannel <- wkAddr // put worker back to worker-pool
		fmt.Printf("Worker %s backed to pool after %dth %s task\n", wkAddr, args.TaskNumber, args.Phase)
		return nil
	} else { // re-schedule if needed
		fmt.Printf("[RE-SCHEDULE] Worker %s failed %dth map-task of file %s, re-scheduling\n", strings.Split(wkAddr, "-")[2], args.TaskNumber, args.File)
		mr.scheduleTask(args, wg)
		return fmt.Errorf("Failed to schedule %dth map-task of file %s", args.TaskNumber, args.File)
	}

}
