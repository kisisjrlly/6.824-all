package mapreduce

import (
	"fmt"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
/*
schedule（）启动并等待给定阶段（Map或Reduce）中的所有任务。
mapFiles参数保存作为映射阶段输入的文件的名称，每个映射任务一个。
nReduce是reduce任务的数量。
registerChan参数生成一个已注册的worker流；每个项都是worker的RPC地址，适合传递给call（）。
registerChan将提供所有现有的注册worker（如果有的话）和新的注册worker。
*/
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	//fmt.Println("-------------1-----------------")
	//fmt.Println("jobname:", jobName, "mapFiles:", mapFiles, "nReduce:", nReduce, "phase", phase)
	//fmt.Println("-------------2-----------------")
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

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	rC := <-registerChan
	fmt.Println("schdule phase:", rC, phase)
	for i := 0; i < ntasks; i++ {
		//for _, mF := range mapFiles {
		//debug("wait for registerChan:", i, phase)

		ok := call(rC, "Worker.DoTask", &DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}, new(struct{}))
		for {
			if ok == false {
				rC = <-registerChan
				ok = call(rC, "Worker.DoTask", &DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}, new(struct{}))
			} else {
				break
			}

		}
		//fmt.Println("schdule process:", mapFiles[i], phase)
		//}
	}
	//registerChan <- rC
	fmt.Printf("Schedule: %v phase done\n", phase)
}
