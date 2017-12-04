package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Type  string
	Uid   string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	database    map[string]string
	history     map[string]bool
	seqToCommit int
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("%d Get %s\n", args.Uid, args.Key)

	// if not handled
	if _, exists := kv.history[args.Uid]; !exists {
		op := Op{}
		op.Key = args.Key
		op.Type = "Get"
		op.Uid = args.Uid
		// handle this request
		kv.handle(op)
	}

	// if the entry exists in database
	if _, exists := kv.database[args.Key]; exists {
		reply.Value = kv.database[args.Key]
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("%d %s %s, %s\n", args.Uid, args.Op, args.Key, args.Value)

	// if not handled
	if _, exists := kv.history[args.Uid]; !exists {
		op := Op{}
		op.Key = args.Key
		op.Value = args.Value
		op.Type = args.Op
		op.Uid = args.Uid
		// handle this request
		kv.handle(op)
	}
	reply.Err = OK
	return nil
}

func (kv *KVPaxos) handle(op Op) {
	for {
		// synchronize local paxos peer with other peers
		// commit log in sync() too
		kv.sync()
		v := kv.addToLog(kv.seqToCommit, op)
		if v == op {
			break
		}
	}
}

func (kv *KVPaxos) sync() {
	maxSeq := kv.px.Max()
	for kv.seqToCommit <= maxSeq {
		status, v := kv.px.Status(kv.seqToCommit)
		if status == paxos.Pending {
			// values doesn't matter
			// there should have been an accepted value
			kv.addToLog(kv.seqToCommit, Op{})
			status, v = kv.px.Status(kv.seqToCommit)
		}
		to := 10 * time.Millisecond
		for {
			if status == paxos.Decided {
				// commit log
				op := v.(Op)
				if op.Type != "Get" {
					// if not handled
					if _, exists := kv.history[op.Uid]; !exists {
						if op.Type == "Put" {
							kv.database[op.Key] = op.Value
						} else if op.Type == "Append" {
							kv.database[op.Key] += op.Value
						}
						kv.history[op.Uid] = true
					}
				}
				break
			}
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
			status, v = kv.px.Status(kv.seqToCommit)
		}
		// after committing a log entry
		kv.seqToCommit++
	}
	kv.px.Done(kv.seqToCommit - 1)
}

func (kv *KVPaxos) addToLog(seq int, v interface{}) interface{} {
	kv.px.Start(seq, v)
	to := 10 * time.Millisecond
	for {
		status, v := kv.px.Status(seq)
		if status == paxos.Decided {
			return v
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.database = make(map[string]string)
	kv.history = make(map[string]bool)
	kv.seqToCommit = 0

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
