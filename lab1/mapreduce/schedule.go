package mapreduce

import (
	"fmt"
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

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	taskChannel := make(chan int, ntasks)
	workerChannel := make(chan string)
	listenChannel := make(chan struct {})
	shutdownChannel := make(chan struct {})
	workers := make(map[string]bool)	
	finishedTasks := make(map[int]bool)
	mutex := sync.Mutex{}

	// Process new worker registration.
	go func() {
		// Get currently available workers.
		mr.Lock()
		for _, w := range mr.workers {
			if !workers[w] {
				workers[w] = true
				go func(w string) {
					defer func() {
						recover()
					}()
					workerChannel <- w
				}(w)
			}
		}
		mr.Unlock()

		// Listen to newly register workers.
		for {
			select {
			case w := <-mr.registerChannel:
				if !workers[w] {
					workers[w] = true
					go func(w string) {
						defer func() {
							recover()
						}()
						workerChannel <- w
					}(w)
				}
			case <-listenChannel:
				return
			}
		}
	}()	

	// Initialize tasks.
	for i := 0; i < ntasks; i++ {
		taskChannel <- i	
	}

	// Schedule each task to workers.
loop:
	for {
		select {
		case taskNum := <- taskChannel:
			// Wait for an available worker.
			curWorker := <- workerChannel 
			fmt.Printf("(cheng) assign task %d to worker %s\n", taskNum, curWorker)	
			// Schedule the task.
			go func() {
				args := &DoTaskArgs{mr.jobName, mr.files[taskNum], phase, taskNum, nios}
				ok := call(curWorker, "Worker.DoTask", args, new(struct{}))
				if ok == false {
					// Handle worker failure.
					go func() {
						defer func() {
							recover()
						}()
						taskChannel <- taskNum	
					}()
					fmt.Printf("schedule: RPC Worker.DoTask error, worker: %s, task: %d\n", curWorker, taskNum)
				} else {
					// Signal the finishedTask and worker channel.
					// Update finished tasks.
					mutex.Lock()
					if !finishedTasks[taskNum] {
						finishedTasks[taskNum] = true
						if len(finishedTasks) == ntasks {
							close(shutdownChannel)
						}
					}
					mutex.Unlock()

					go func() {
						defer func() {
							recover()
						}()
						workerChannel <- curWorker
					}()
				}
			}()
		
		case <-shutdownChannel:
			// Close channels.
			close(taskChannel)
			close(workerChannel)
			close(listenChannel)
			break loop
		}
	} 

	fmt.Printf("Schedule: %v phase done\n", phase)
}
