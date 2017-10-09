package mapreduce

import "container/list"
import "fmt"
import "strconv"


type WorkerInfo struct {
	address string
	// You can add definitions here.
    res *DoJobReply
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) doMasterJob(op JobType, i int) {
    args := &DoJobArgs{}
    args.File = mr.file
    args.Operation = op
    args.JobNumber = i
    switch op {
        case Map:
            args.NumOtherPhase = nReduce
        case Reduce:
            args.NumOtherPhase = nMap
    }
    mr.jobChannel <- args
}

func (mr *MapReduce) consumer(args *DoJobArgs) {
    worker := <-mr.registerChannel
    DPrintf("Consume\n")
    var curWorker *WorkerInfo
    mr.mapLock.Lock()
    mr.Workers[strconv.Itoa(args.JobNumber)] = &WorkerInfo{worker, &DoJobReply{false}}
    curWorker = mr.Workers[strconv.Itoa(args.JobNumber)]
    mr.mapLock.Unlock()
    DPrintf("worker:%s\n", worker)
    ok := call(curWorker.address, "Worker.DoJob", args, curWorker.res)
    if ok == false {
        // error
        mr.jobChannel <- args
    } else {
        mr.replyChannel <- curWorker.res
    }
    mr.registerChannel <- worker
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
    go func() {
        for mr.alive {
            select {
            case args := <-mr.jobChannel:
                go mr.consumer(args)
            default:
            }
        }
    }()
    for i := 0; i < nMap; i++ {
        mr.doMasterJob(Map, i)
    }
    // wait for all map jobs to finish
    jobCount := nMap
    for jobCount > 0 {
        c := <-mr.replyChannel
        if c.OK {
            jobCount--
        }
    }
    for i := 0; i < nReduce; i++ {
        mr.doMasterJob(Reduce, i)
    }
    jobCount = nReduce
    for jobCount > 0 {
        c := <-mr.replyChannel
        if c.OK {
            jobCount--
        }
    }
    return mr.KillWorkers()
}
