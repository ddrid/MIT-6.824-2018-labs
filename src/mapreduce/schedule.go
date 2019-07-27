package mapreduce

import (
	"fmt"
	"log"
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
func schedule(
	jobName string,
	mapFiles []string,
	nReduce int,
	phase jobPhase,
	registerChan chan string) {

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

	var taskArgs DoTaskArgs

	taskArgs.JobName = jobName
	taskArgs.NumOtherPhase = n_other
	taskArgs.Phase = phase

	var waitGroup sync.WaitGroup

	//提供taskNumber
	taskNumberChan := make(chan int)

	for i := 0; i < ntasks; i++ {
		taskNumberChan <- i
		waitGroup.Add(1)
	}

	//等待任务执行完后关闭提供taskId的通道
	go func() {
		waitGroup.Wait()
		close(taskNumberChan)
	}()

	for taskNumber := range taskNumberChan {

		taskArgs.TaskNumber = taskNumber
		if phase == mapPhase {
			taskArgs.File = mapFiles[taskNumber]
		}

		worker := <-registerChan

		go func(worker string, taskArgs DoTaskArgs) {

			if call(worker, "Worker.DoTask", taskArgs, nil) {
				waitGroup.Done()

				//确认worker可以工作，放回registerChan
				registerChan <- worker
			} else {
				log.Printf("Worker:%s, TaskNumber:%d assigned failed !",
					worker, taskArgs.TaskNumber)

				//将失败的taskNumber放回taskNumberChan，供下次再次部署
				taskNumberChan <- taskArgs.TaskNumber
			}
		}(worker, taskArgs)

	}

	fmt.Printf("Schedule: %v done\n", phase)
}
