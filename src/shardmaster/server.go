package shardmaster

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs     []Config // indexed by config num starting from 1
	seqToCommit int
}

type ConfigType int

const (
	Join  ConfigType = iota // 0
	Leave                   // 1
	Move                    // 2
	Query                   // 3
)

type Op struct {
	// Your data here.
	Type    ConfigType
	OPID    string   // generated in Join(), Leave(), Move() or Query()
	GID     int64    // Join, Leave, Move
	Servers []string // Join
	Shard   int      // Move
	Num     int      // Query
}

type int64arr []int64

func (a int64arr) Len() int           { return len(a) }
func (a int64arr) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64arr) Less(i, j int) bool { return a[i] < a[j] }

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	//DPrintf("Join %d -- GID: %d Servers: %d\n", sm.me, args.GID, len(args.Servers))
	// generate a globally unique op id
	opid := strconv.FormatInt(time.Now().UnixNano(), 10) + "#" + strconv.Itoa(sm.me)
	op := Op{}
	op.Type = Join
	op.OPID = opid
	op.GID = args.GID
	op.Servers = make([]string, len(args.Servers))
	copy(op.Servers, args.Servers)
	sm.handle(op)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	//DPrintf("Leave %d -- GID: %d\n", sm.me, args.GID)
	opid := strconv.FormatInt(time.Now().UnixNano(), 10) + "#" + strconv.Itoa(sm.me)
	op := Op{}
	op.Type = Leave
	op.OPID = opid
	op.GID = args.GID
	sm.handle(op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	//DPrintf("Move %d - Shard: %d to GID: %d\n", sm.me, args.Shard, args.GID)
	opid := strconv.FormatInt(time.Now().UnixNano(), 10) + "#" + strconv.Itoa(sm.me)
	op := Op{}
	op.Type = Move
	op.OPID = opid
	op.Shard = args.Shard
	op.GID = args.GID
	sm.handle(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	opid := strconv.FormatInt(time.Now().UnixNano(), 10) + "#" + strconv.Itoa(sm.me)
	op := Op{}
	op.Type = Query
	op.OPID = opid
	op.Num = args.Num
	sm.handle(op)
	if args.Num > -1 && args.Num < len(sm.configs) {
		reply.Config = sm.configs[args.Num]
	} else {
		// return the latest configuration
		if len(sm.configs) > 0 {
			reply.Config = sm.configs[len(sm.configs)-1]
		}
	}
	return nil
}

func (sm *ShardMaster) handle(op Op) {
	// handles one request a time
	for sm.isdead() == false {
		sm.sync()
		v := sm.addToLog(sm.seqToCommit, op)
		if v.(Op).OPID == op.OPID {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	DPrintf("%d -- Num: %v\n", sm.me, sm.configs[len(sm.configs)-1].Num)
	DPrintf("%d -- Shards: %v\n", sm.me, sm.configs[len(sm.configs)-1].Shards)
	DPrintf("%d -- Groups: %v\n", sm.me, sm.configs[len(sm.configs)-1].Groups)
}

func (sm *ShardMaster) sync() {
	maxSeq := sm.px.Max()
	//DPrintf("%d starts Sync from %d to %d\n", sm.me, sm.seqToCommit, maxSeq)
	for sm.seqToCommit <= maxSeq {
		status, v := sm.px.Status(sm.seqToCommit)
		if status == paxos.Pending {
			// propose value doesn't matter
			// there should have been an accepted value
			sm.addToLog(sm.seqToCommit, Op{})
			status, v = sm.px.Status(sm.seqToCommit)
		}
		to := 10 * time.Millisecond
		for {
			if status == paxos.Decided {
				// commit log
				op := v.(Op)
				switch op.Type {
				case Join:
					// handle Join request
					sm.joinHandler(op.GID, op.Servers)
					break
				case Leave:
					// handle Leave request
					sm.leaveHandler(op.GID)
					break
				case Move:
					// handle Move request
					sm.moveHandler(op.Shard, op.GID)
					break
				case Query:
					// no need to handle
					break
				}
				break
			}
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
			status, v = sm.px.Status(sm.seqToCommit)
		}
		// after committing a log entry
		sm.seqToCommit++
		time.Sleep(50 * time.Millisecond)
	}
	sm.px.Done(sm.seqToCommit - 1)
	//DPrintf("%d finishes Sync to %d\n", sm.me, sm.seqToCommit)
}

func (sm *ShardMaster) addToLog(seq int, v interface{}) interface{} {
	sm.px.Start(seq, v)
	to := 10 * time.Millisecond
	for {
		status, v := sm.px.Status(seq)
		if status == paxos.Decided {
			return v
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (sm *ShardMaster) joinHandler(GID int64, servers []string) {
	//DPrintf("JoinHandler %d starts -- GID: %d Servers: %d\n", sm.me, GID, len(servers))
	currentConfig := sm.configs[len(sm.configs)-1]
	if _, exists := currentConfig.Groups[GID]; exists {
		// the joining group already exists
		return
	}
	nextConfig := sm.copyCurrentConfig(currentConfig)
	// add new group
	nextConfig.Groups[GID] = make([]string, len(servers))
	copy(nextConfig.Groups[GID], servers)
	// adjust Shards assignment
	_, selectedGID := sm.selectGID(nextConfig, GID)
	groupShards := make(map[int64][]int)
	for index, gid := range nextConfig.Shards {
		groupShards[gid] = append(groupShards[gid], index)
	}
	tmp := NShards / len(nextConfig.Groups)
	for i := 0; i < tmp; i++ {
		nextConfig.Shards[groupShards[selectedGID][i]] = GID
	}
	// update configuration
	sm.configs = append(sm.configs, nextConfig)
	//DPrintf("JoinHandler %d finishes -- GID: %d Servers: %d\n", sm.me, GID, len(servers))
}

func (sm *ShardMaster) leaveHandler(GID int64) {
	// assign GID's shards to the group with the least shards
	currentConfig := sm.configs[len(sm.configs)-1]
	nextConfig := sm.copyCurrentConfig(currentConfig)
	// remove GID from Groups
	delete(nextConfig.Groups, GID)
	selectedGID, _ := sm.selectGID(nextConfig, GID)
	// re-assign shards
	for index, gid := range nextConfig.Shards {
		if gid == GID {
			nextConfig.Shards[index] = selectedGID
		}
	}
	// update configuration
	sm.configs = append(sm.configs, nextConfig)
}

func (sm *ShardMaster) moveHandler(shard int, GID int64) {
	nextConfig := sm.copyCurrentConfig(sm.configs[len(sm.configs)-1])
	nextConfig.Shards[shard] = GID
	// update configuration
	sm.configs = append(sm.configs, nextConfig)
}

func (sm *ShardMaster) copyCurrentConfig(currentConfig Config) Config {
	//DPrintf("copyCurrentConfig %d starts -- Num: %d Groups: %d\n", sm.me, currentConfig.Num, len(currentConfig.Groups))
	nextConfig := Config{}
	if len(sm.configs) == 1 {
		nextConfig.Num = 1
		nextConfig.Groups = make(map[int64][]string)
	} else {
		nextConfig.Num = currentConfig.Num + 1
		// copy Groups map
		nextConfig.Groups = make(map[int64][]string)
		for k, v := range currentConfig.Groups {
			nextConfig.Groups[k] = make([]string, len(v))
			copy(nextConfig.Groups[k], v)
		}
		// copy shards assignment
		for i, v := range currentConfig.Shards {
			nextConfig.Shards[i] = v
		}
	}
	//DPrintf("copyCurrentConfig %d finishes -- Num: %d Groups: %d\n", sm.me, currentConfig.Num, len(currentConfig.Groups))
	return nextConfig
}

func (sm *ShardMaster) getSortedGIDs(groups map[int64][]string) []int64 {
	sorted := make(int64arr, 0)
	for gid := range groups {
		sorted = append(sorted, gid)
	}
	sort.Sort(sorted)
	return sorted
}

// select the group with the least/most shards count
func (sm *ShardMaster) selectGID(nextConfig Config, GID int64) (int64, int64) {
	// count shards
	shardCounts := make(map[int64]int)
	for _, gid := range nextConfig.Shards {
		shardCounts[gid]++
	}
	minCount := NShards + 1
	maxCount := 0
	var minGID int64
	var maxGID int64
	groups := sm.getSortedGIDs(nextConfig.Groups)
	for _, gid := range groups {
		shardCount := shardCounts[gid]
		if shardCount < minCount {
			minCount = shardCount
			minGID = gid
		}
		if shardCount > maxCount {
			maxCount = shardCount
			maxGID = gid
		}
	}
	return minGID, maxGID
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	sm.seqToCommit = 0

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
