package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type instanceState struct {
	n_p    int // highest propose number seen
	n_a    int // highest accepted number seen
	v_a    interface{}
	status Fate // instance status
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	minSeq    int // the lowest sequence # of current instances
	maxSeq    int // the highest sequence # of current instances
	doneSeqs  map[int]int
	instances map[int]*instanceState
}

type IsDecidedArgs struct {
	Seq       int
	PeerIndex int
	DoneSeq   int
}

type IsDecidedReply struct {
	Status Fate
}

type PrepareArgs struct {
	Seq           int
	ProposeNumber int
	PeerIndex     int
	DoneSeq       int
}

type PrepareReply struct {
	OK                 bool
	HighestProposeNum  int // the highest propose number seen by the peer (n_p)
	AcceptedProposeNum int // the accepted propose number (n_a)
	Status             Fate
	Value              interface{}
}

type AcceptArgs struct {
	Seq           int
	ProposeNumber int
	Value         interface{}
	PeerIndex     int
	DoneSeq       int
}

type AcceptReply struct {
	OK bool
}

type DecideArgs struct {
	Seq                   int
	AcceptedProposeNumber int
	Value                 interface{}
	PeerIndex             int
	DoneSeq               int
}

type DecideReply struct {
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	go px.propose(seq, v) // propose v for instance #seq
}

func (px *Paxos) propose(seq int, v interface{}) {
	// the instance has been decided and forgotten
	if seq < px.minSeq {
		return
	}

	// choose n as propose number
	// should be unique and higher than any n (associated with this seq) seen so far
	// starts from 1, changes according to prepare replies
	n := 1
	proposeValue := v
	highestProposeNum := n
	highestAcceptedNum := -1
	decidedReply := PrepareReply{}
	// keep proposing until #seq instance has been decided
	for !px.isDecided(seq) {
		// used to determine majority
		okCount := 0

		if decidedReply.Value != nil {
			for i := 0; i < len(px.peers); i++ {
				px.decide(i, seq, decidedReply.AcceptedProposeNum, decidedReply.Value)
			}
			continue
		}

		// send prepare(n) to all servers including self
		for i := 0; i < len(px.peers); i++ {
			reply, er := px.prepare(i, seq, n)
			if er != nil {
				// cannot connect to this peer
				// fmt.Printf(er.Error())
				continue
			}
			// the reply (prepare_reject) indicates instance #seq is decided
			if reply.Status == Decided {
				// broadcast it to peers
				decidedReply = reply
				break
			}
			// update global highest propose number
			if reply.HighestProposeNum > highestProposeNum {
				highestProposeNum = reply.HighestProposeNum
			}
			if reply.OK {
				// received prepare_ok
				// means n is larger than the peer's n_p
				// v' = v_a with highest n_a; choose own v otherwise
				if reply.Value != nil && reply.AcceptedProposeNum > highestAcceptedNum {
					proposeValue = reply.Value
					highestAcceptedNum = reply.AcceptedProposeNum
				}
				okCount++
			}
		}

		if decidedReply.Value != nil {
			continue
		}

		// check if received prepare_ok from majority
		if okCount < (len(px.peers)/2 + 1) {
			// if not
			proposeValue = v
			n = highestProposeNum + 1
			continue
		} else {
			// prepare_ok from majority
			// send accept(n, v') to all
			okCount = 0
			for i := 0; i < len(px.peers); i++ {
				reply, _ := px.accept(i, seq, n, proposeValue)
				if reply.OK {
					okCount++
				}
			}
			if okCount < (len(px.peers)/2 + 1) {
				continue
			} else {
				// accept_ok from majority
				// decide #seq instance
				for i := 0; i < len(px.peers); i++ {
					px.decide(i, seq, n, proposeValue)
				}
			}
		}
	}
}

func (px *Paxos) isDecided(seq int) bool {
	var reply IsDecidedReply
	args := &IsDecidedArgs{}
	args.Seq = seq
	args.PeerIndex = px.me
	px.mu.Lock()
	args.DoneSeq = px.doneSeqs[px.me]
	px.mu.Unlock()

	for i := 0; i < len(px.peers); i++ {
		if i == px.me {
			px.IsDecidedHandler(args, &reply)
		} else {
			if !call(px.peers[i], "Paxos.IsDecidedHandler", args, &reply) {
				continue
			}
		}
		if reply.Status != Decided {
			return false
		}
	}
	return true
}

func (px *Paxos) IsDecidedHandler(args *IsDecidedArgs, reply *IsDecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	// update done seq of the sender
	if args.DoneSeq > px.doneSeqs[args.PeerIndex] {
		px.doneSeqs[args.PeerIndex] = args.DoneSeq
	}

	if _, ok := px.instances[args.Seq]; !ok {
		reply.Status = Pending
	} else {
		reply.Status = px.instances[args.Seq].status
	}

	return nil
}

func (px *Paxos) prepare(peerIndex int, seq int, proposeNum int) (PrepareReply, error) {
	var reply PrepareReply
	// prepare arguments
	args := &PrepareArgs{}
	args.ProposeNumber = proposeNum
	args.Seq = seq
	args.PeerIndex = px.me
	px.mu.Lock()
	args.DoneSeq = px.doneSeqs[px.me]
	px.mu.Unlock()

	if peerIndex == px.me {
		err := px.PrepareHandler(args, &reply)
		return reply, err
	}
	ok := call(px.peers[peerIndex], "Paxos.PrepareHandler", args, &reply)
	if !ok {
		return reply, fmt.Errorf("prepare RPC failed: index: %d, number: %d, seq: %d", peerIndex, proposeNum, seq)
	}
	return reply, nil
}

func (px *Paxos) PrepareHandler(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	// update done seq of the sender
	if args.DoneSeq > px.doneSeqs[args.PeerIndex] {
		px.doneSeqs[args.PeerIndex] = args.DoneSeq
	}

	// if instance #seq doesn't exist
	if _, ok := px.instances[args.Seq]; !ok {
		instance := &instanceState{}
		instance.status = Pending
		instance.n_p = args.ProposeNumber
		px.instances[args.Seq] = instance
		reply.OK = true
		reply.Status = Pending
		if args.Seq > px.maxSeq {
			px.maxSeq = args.Seq
		}
		return nil
	}

	// instance #seq exists
	if reply.OK = (px.instances[args.Seq].status == Pending && args.ProposeNumber > px.instances[args.Seq].n_p); reply.OK {
		px.instances[args.Seq].n_p = args.ProposeNumber
	}
	reply.AcceptedProposeNum = px.instances[args.Seq].n_a
	reply.Value = px.instances[args.Seq].v_a
	reply.HighestProposeNum = px.instances[args.Seq].n_p
	reply.Status = px.instances[args.Seq].status

	return nil
}

func (px *Paxos) accept(peerIndex int, seq int, proposeNum int, v interface{}) (AcceptReply, error) {
	var reply AcceptReply
	args := &AcceptArgs{}
	args.ProposeNumber = proposeNum
	args.Seq = seq
	args.Value = v
	px.mu.Lock()
	args.DoneSeq = px.doneSeqs[px.me]
	px.mu.Unlock()
	args.PeerIndex = px.me

	if peerIndex == px.me {
		err := px.AcceptHandler(args, &reply)
		return reply, err
	}
	ok := call(px.peers[peerIndex], "Paxos.AcceptHandler", args, &reply)
	if !ok {
		return reply, fmt.Errorf("accept RPC failed: index: %d, number: %d, seq: %d", peerIndex, proposeNum, seq)
	}

	return reply, nil
}

func (px *Paxos) AcceptHandler(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	// update done seq of the sender
	if args.DoneSeq > px.doneSeqs[args.PeerIndex] {
		px.doneSeqs[args.PeerIndex] = args.DoneSeq
	}

	// if instance #seq doesn't exist
	if _, ok := px.instances[args.Seq]; !ok {
		instance := &instanceState{}
		instance.status = Pending
		instance.n_p = args.ProposeNumber
		px.instances[args.Seq] = instance
		if args.Seq > px.maxSeq {
			px.maxSeq = args.Seq
		}
	}

	if reply.OK = (px.instances[args.Seq].status == Pending && args.ProposeNumber >= px.instances[args.Seq].n_p); reply.OK {
		px.instances[args.Seq].n_p = args.ProposeNumber
		px.instances[args.Seq].n_a = args.ProposeNumber
		px.instances[args.Seq].v_a = args.Value
	}

	return nil
}

func (px *Paxos) decide(peerIndex int, seq int, acceptedProposeNumber int, v interface{}) {
	var reply DecideReply
	args := &DecideArgs{}
	args.Seq = seq
	args.AcceptedProposeNumber = acceptedProposeNumber
	args.Value = v
	args.PeerIndex = px.me
	px.mu.Lock()
	args.DoneSeq = px.doneSeqs[px.me]
	px.mu.Unlock()

	if peerIndex == px.me {
		px.DecideHandler(args, &reply)
		return
	}

	call(px.peers[peerIndex], "Paxos.DecideHandler", args, &reply)
}

func (px *Paxos) DecideHandler(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	// update done seq of the sender
	if args.DoneSeq > px.doneSeqs[args.PeerIndex] {
		px.doneSeqs[args.PeerIndex] = args.DoneSeq
	}

	// if instance #seq doesn't exist
	if _, ok := px.instances[args.Seq]; !ok {
		instance := &instanceState{}
		instance.status = Pending
		px.instances[args.Seq] = instance
		if args.Seq > px.maxSeq {
			px.maxSeq = args.Seq
		}
	}

	if px.instances[args.Seq].status != Decided {
		px.instances[args.Seq].status = Decided
		px.instances[args.Seq].n_a = args.AcceptedProposeNumber
		px.instances[args.Seq].v_a = args.Value
		if px.instances[args.Seq].n_p < args.AcceptedProposeNumber {
			px.instances[args.Seq].n_p = args.AcceptedProposeNumber
		}
	}

	return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	px.doneSeqs[px.me] = seq
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	minDoneSeq := px.doneSeqs[0]
	for i := 0; i < len(px.peers); i++ {
		if px.doneSeqs[i] < minDoneSeq {
			minDoneSeq = px.doneSeqs[i]
		}
	}
	if minDoneSeq > px.minSeq {
		var oldMinSeq int
		if px.minSeq < 0 {
			oldMinSeq = 0
		} else {
			oldMinSeq = px.minSeq
		}
		px.minSeq = minDoneSeq
		for i := oldMinSeq; i <= px.minSeq; i++ {
			delete(px.instances, i)
		}
	}
	return px.minSeq + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	if seq <= px.minSeq {
		return Forgotten, nil
	}
	if _, ok := px.instances[seq]; ok {
		return px.instances[seq].status, px.instances[seq].v_a
	}
	return Pending, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instances = make(map[int]*instanceState)
	px.doneSeqs = make(map[int]int)
	px.maxSeq = -1
	px.minSeq = -1
	for i := 0; i < len(px.peers); i++ {
		px.doneSeqs[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
