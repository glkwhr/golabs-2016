package shardkv

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"shardmaster"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const Debug = 0

type OpType int

const (
	Get      OpType = iota // 0
	Put                    // 1
	Append                 // 2
	Reconfig               // 3
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Type  OpType
	Key   string
	Value string
	// Seq in client request is used to detect duplicated request
	// Seq in Reconfig entry is used to distinguish Reconfig entry
	Seq      string
	ClientID int64
	// for reconfig
	Data ReconfigData
}

type History struct {
	Seq   string // request seq
	Err   Err    // reply
	Value string // reply (only used in Get())
}

// data needed to update when reconfiguring
type ReconfigData struct {
	Num      int
	KVData   map[string]string
	HistData map[int64]History
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	db          map[string]string  // key/value database
	history     map[int64]History  // request history, for each client, store the most recent request
	config      shardmaster.Config // current shard config
	seqToCommit int                // next seq of the last commmited seq, used in paxos
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// check if the request has been handled
	if oldRequest := kv.history[args.ClientID]; oldRequest.Seq >= args.Seq {
		if oldRequest.Seq == args.Seq {
			reply.Err, reply.Value = oldRequest.Err, oldRequest.Value
		}
		// else there is no need to reply
		return nil
	}
	// append log entry
	op := Op{}
	op.Type = Get
	op.ClientID = args.ClientID
	op.Key = args.Key
	op.Seq = args.Seq
	DPrintf("1 max: %d", kv.px.Max())
	for kv.isdead() == false {
		seq := kv.px.Max() + 1
		v := kv.addToLog(seq, op)
		if v.(Op).Seq == op.Seq {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	DPrintf("2 max: %d", kv.px.Max())
	// need to commit the whole log before responding
	DPrintf("Before sync() HSeq: %s cseq: %d", kv.history[args.ClientID].Seq, kv.seqToCommit)
	kv.sync()
	DPrintf("After sync() HSeq: %s cseq: %d", kv.history[args.ClientID].Seq, kv.seqToCommit)
	// server responds to client according to history data
	if kv.history[args.ClientID].Seq == args.Seq {
		reply.Err, reply.Value = kv.history[args.ClientID].Err, kv.history[args.ClientID].Value
	} else {
		reply.Err = ErrWrongGroup
	}
	DPrintf("S: %v Get k: %s v: %s id %v Err: %s Seq: %s", kv.me, args.Key, reply.Value, args.ClientID, reply.Err, args.Seq)
	DPrintf("Gid: %v HSeq: %s", kv.gid, kv.history[args.ClientID].Seq)
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// check if the request has been handled
	if oldRequest := kv.history[args.ClientID]; oldRequest.Seq >= args.Seq {
		if oldRequest.Seq == args.Seq {
			reply.Err = oldRequest.Err
		}
		// else there is no need to reply
		return nil
	}
	// append log entry
	op := Op{}
	if args.Op == "Put" {
		op.Type = Put
	} else {
		op.Type = Append
	}
	op.Seq = args.Seq
	op.ClientID = args.ClientID
	op.Key = args.Key
	op.Value = args.Value
	for kv.isdead() == false {
		seq := kv.px.Max() + 1
		v := kv.addToLog(seq, op)
		if v.(Op).Seq == op.Seq {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	// need to commit the whole log before responding
	kv.sync()
	// server responds to client according to history data
	if kv.history[args.ClientID].Seq == args.Seq {
		reply.Err = kv.history[args.ClientID].Err
	} else {
		reply.Err = ErrWrongGroup
	}
	DPrintf("S: %v %s k: %s v: %s id %v Err: %s Seq: %s", kv.me, args.Op, args.Key, args.Value, args.ClientID, reply.Err, args.Seq)
	DPrintf("Gid: %v HSeq: %s", kv.gid, kv.history[args.ClientID].Seq)
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// sychronize with other replicants via Paxos
	kv.sync()

	// query shardmaster for the latest config
	latestConfig := kv.sm.Query(-1)
	//DPrintf("S: %d tick() current config: %v", kv.me, kv.config.Num)
	//DPrintf("S: %d tick() latest config: %v", kv.me, latestConfig.Num)
	// should update config one by one or there may be data lost
	for i := kv.config.Num + 1; i <= latestConfig.Num; i++ {
		nextConfig := kv.sm.Query(i)
		if kv.updateConfig(nextConfig) != OK {
			// data transfer failed
			//DPrintf("Data tranfer failed. Goal Config Num: %d", nextConfig.Num)
			break
		}
		kv.sync()
	}
}

// doesn't update config directly, but through creating log entry
// returns Err
func (kv *ShardKV) updateConfig(latestConfig shardmaster.Config) string {
	var data ReconfigData
	data.Num = latestConfig.Num
	data.KVData = make(map[string]string)
	data.HistData = make(map[int64]History)
	if len(kv.config.Groups) > 0 {
		for index, gid := range latestConfig.Shards {
			if kv.gid == gid && kv.config.Shards[index] != kv.gid {
				// the shard will be stored in this group
				// the previous group is not current group
				// together indicates that current group need to accept data from the previous group
				oldGid := kv.config.Shards[index]
				// request data from the previous group
				transferSucceeded := false
				args := TransferArgs{index, latestConfig.Num}
				reply := TransferReply{}
				reply.Err = ErrFailedTransfer
				for _, server := range kv.config.Groups[oldGid] {
					ok := call(server, "ShardKV.Transfer", &args, &reply)
					if ok && reply.Err == OK {
						// add data to reconfig data
						for k, v := range reply.Data.KVData {
							data.KVData[k] = v
						}
						for c, h := range reply.Data.HistData {
							data.HistData[c] = h
						}
						transferSucceeded = true
						break
					} else {
						transferSucceeded = false
					}
				}
				// reconfiguration should fail if current group cannot get all data it needs from any server of the previous group
				if transferSucceeded == false {
					return reply.Err
				}
			}
			// doesn't delete data if previously owning shard is assigned to other group, because will need to transfer data to that group in the future
		}
	}
	// append to the log
	op := Op{}
	op.Seq = strconv.FormatInt(time.Now().UnixNano(), 10) + "#" + strconv.Itoa(kv.me)
	op.Type = Reconfig
	op.Data = data
	op.Data.Num = latestConfig.Num
	for kv.isdead() == false {
		seq := kv.px.Max() + 1
		v := kv.addToLog(seq, op)
		if v.(Op).Seq == op.Seq {
			//DPrintf("S: %d added seq %d to log", kv.me, seq)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	return OK
}

// RPC
// used to tranfer data to other server
func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) error {
	// check if I have those data
	if args.Num > kv.config.Num {
		reply.Err = ErrFailedTransfer
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// copy database data
	kvdata := make(map[string]string)
	for k, v := range kv.db {
		if key2shard(k) == args.Index {
			kvdata[k] = v
		}
	}
	// copy client request history data
	histdata := make(map[int64]History)
	for k, v := range kv.history {
		histdata[k] = v
	}
	reply.Data = ReconfigData{args.Num, kvdata, histdata}
	reply.Err = OK

	return nil
}

// returns the last handle result
func (kv *ShardKV) sync() (Err, string) {
	maxSeq := kv.px.Max()
	// last handle results
	var err Err
	var value string
	for kv.seqToCommit <= maxSeq {
		status, v := kv.px.Status(kv.seqToCommit)
		if status == paxos.Pending {
			kv.addToLog(kv.seqToCommit, Op{})
			status, v = kv.px.Status(kv.seqToCommit)
		}
		to := 10 * time.Millisecond
		for kv.isdead() == false {
			if status == paxos.Decided {
				// if this log entry is decided, commit the log entry op
				op := v.(Op)
				DPrintf("S: %d sync() seq: %v op: %v", kv.me, kv.seqToCommit, op.Seq)
				err, value = kv.handle(op)
				break
			}
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
			// this log entry not decided yet
			status, v = kv.px.Status(kv.seqToCommit)
		}
		// after committing a log entry
		kv.seqToCommit++
	}
	// after committing the whole log
	kv.px.Done(kv.seqToCommit - 1)
	return err, value
}

// at-most-once  doesn't guarantee to successfully append v to log
func (kv *ShardKV) addToLog(seq int, v interface{}) interface{} {
	//DPrintf("S: %d config: %v adds %v to log %v", kv.me, kv.config.Num, v.(Op).Data, seq)
	kv.px.Start(seq, v)
	to := 10 * time.Millisecond
	for kv.isdead() == false {
		status, v := kv.px.Status(seq)
		if status == paxos.Decided {
			return v
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
	return nil
}

// op should not be history request
// op should have been added to paxos log
// handles Get, Put, Append and Reconfig
// returns Err, Value
func (kv *ShardKV) handle(op Op) (Err, string) {
	// since all servers in the group can handle request
	// it's possible to have duplicated log entries
	// so, still need to check history in sync()->handle()
	if h, exists := kv.history[op.ClientID]; exists {
		if h.Seq >= op.Seq {
			return kv.history[op.ClientID].Err, kv.history[op.ClientID].Value
		}
	}
	var err Err
	var value string
	switch op.Type {
	case Get:
		err, value = kv.getHandler(op.Key)
		DPrintf("handled Get seq: %s err: %s", op.Seq, err)
		break
	case Put:
		err = kv.putappendHandler(op.Type, op.Key, op.Value)
		break
	case Append:
		err = kv.putappendHandler(op.Type, op.Key, op.Value)
		break
	case Reconfig:
		//DPrintf("Handle reconfig")
		return kv.reconfigHandler(op.Data), ""
	}
	// client requests need to be stored
	// server responds to client according to history data
	if err != ErrWrongGroup && kv.history[op.ClientID].Seq < op.Seq {
		rh := History{}
		rh.Seq = op.Seq
		rh.Err = err
		rh.Value = value
		kv.history[op.ClientID] = rh
		DPrintf("%v H: %s", kv.me, rh.Seq)
	}
	return err, value
}

func (kv *ShardKV) getHandler(key string) (Err, string) {
	var err Err
	var value string
	if kv.gid != kv.config.Shards[key2shard(key)] {
		err = ErrWrongGroup
	} else {
		if v, exists := kv.db[key]; exists {
			// key exists
			err = OK
			value = v
		} else {
			err = ErrNoKey
		}
	}
	return err, value
}

// returns Err
func (kv *ShardKV) putappendHandler(t OpType, key string, value string) Err {
	var err Err
	if kv.gid != kv.config.Shards[key2shard(key)] {
		err = ErrWrongGroup
	} else {
		if t == Put {
			kv.db[key] = value
		} else if t == Append {
			kv.db[key] += value
		}
		err = OK
	}
	return err
}

// returns Err
func (kv *ShardKV) reconfigHandler(data ReconfigData) Err {
	if data.Num <= kv.config.Num {
		return OK
	}
	kv.config = kv.sm.Query(data.Num)
	//DPrintf("reconfigHandler() update config to: %v", kv.config)
	// update database
	for k, v := range data.KVData {
		kv.db[k] = v
	}
	// update request history
	for c, h := range data.HistData {
		if kv.history[c].Seq < h.Seq {
			kv.history[c] = h
		}
	}
	return OK
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.db = make(map[string]string)
	kv.history = make(map[int64]History)
	kv.seqToCommit = 0
	kv.config = shardmaster.Config{}
	kv.config.Num = -1

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
