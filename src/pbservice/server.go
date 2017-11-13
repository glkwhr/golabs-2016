package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
//import "errors"


type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	database   map[string]string
	dataLock   sync.Mutex
	handleHistory map[int64]bool
	view       viewservice.View
	isPrimary  bool
	needTransfer bool
	isBackup   bool
	isActive   bool
	responseTime time.Time
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	if pb.isActive != true || pb.isPrimary != true {
	    // should not handle this request
	    reply.Err = ErrWrongServer
	    reply.Value = ""
	} else {
	    // handle request
        //pb.dataLock.Lock()
        if value, ok := pb.database[args.Key]; ok == true {
            reply.Err = OK
            reply.Value = value
        } else {
            reply.Err = ErrNoKey
            reply.Value = ""
        }
        //pb.dataLock.Unlock()
    }
    pb.mu.Unlock()

	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
    //log.Printf("enter PutAppend()")
	// Your code here.
	pb.mu.Lock()
	//log.Printf("gained lock PutAppend()")
	if pb.isActive != true || pb.isPrimary != true {
	    // should not handle this request
	    reply.Err = ErrWrongServer
	} else {
	    // handle request
        pb.handlePutAppend(args, reply)
    }
    //log.Printf("exit PutAppend()")
    pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Forward(args *ForwardArgs, reply *ForwardReply) error {
    //log.Printf("Inside %v Forward()\n", pb.me)
    pb.mu.Lock()
    if pb.isBackup == true && pb.view.Primary == args.Me {
        handleArgs := &PutAppendArgs{args.Key, args.Value, args.Op, args.Seq}
        handleReply := new(PutAppendReply)
        pb.handlePutAppend(handleArgs, handleReply)
        reply.Err = handleReply.Err
    } else {
        reply.Err = ErrWrongServer
    }
    pb.mu.Unlock()
    return nil
}

// at most once
func (pb *PBServer) forwardBackup(args *PutAppendArgs) bool {
    forwardArgs := new(ForwardArgs)
    forwardArgs.Key = args.Key
    forwardArgs.Value = args.Value
    forwardArgs.Op = args.Op
    forwardArgs.Seq = args.Seq
    forwardArgs.Me = pb.me
    forwardReply := new(ForwardReply)
    forwardReply.Err = OK
    //log.Printf("%v forwards backup to %v\n", pb.me, pb.view.Backup)
    for pb.isPrimary == true && pb.view.Backup != "" {
        ok := call(pb.view.Backup, "PBServer.Forward", forwardArgs, forwardReply)
        if ok == true && forwardReply.Err == OK {
            break
        }
        time.Sleep(viewservice.PingInterval)
        pb.updateView()
    }
    return forwardReply.Err == OK
}

func (pb *PBServer) handlePutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    reply.Err = OK
    //log.Printf("Enter %v handlePutAppend()\n", pb.me)
    //pb.dataLock.Lock()
    //log.Printf("%v gained dataLock(handlePutAppend)\n", pb.me)
    if handled, ok := pb.handleHistory[args.Seq]; (ok == false) || (handled == false) {
        // not handled
        if pb.isBackup || (pb.isPrimary && pb.forwardBackup(args)) {
            if oldData, ok := pb.database[args.Key]; ok && (args.Op == "Append") {
                // do append
                //log.Printf("Append %v database\n", pb.me)
                pb.database[args.Key] = oldData + args.Value
            } else {
                // do put
                //log.Printf("Put %v database\n", pb.me)
                pb.database[args.Key] = args.Value
            }
            pb.handleHistory[args.Seq] = true
        } else {
            reply.Err = ErrWrongServer
        }
    }
    
    //pb.dataLock.Unlock()
    //log.Printf("Exit %v handlePutAppend()\n", pb.me)
    //log.Printf("%v released dataLock(handlePutAppend)\n", pb.me)
}

func (pb *PBServer) transferBackup() error {
    //pb.dataLock.Lock()
    //log.Printf("%v gained dataLock(transferBackup)\n", pb.me)
    args := new(TransferArgs)
    args.Database = pb.database
    args.HandleHistory = pb.handleHistory
    reply := new(TransferReply)
    for {
        ok := call(pb.view.Backup, "PBServer.Transfer", args, reply)
        if ok != true {
            time.Sleep(viewservice.PingInterval)
            pb.updateView()
            continue
        }
        if reply.Err == ErrWrongServer {
            // no longer a backup server
        } else {
            // succeeded
            break
        }
    }
    //pb.dataLock.Unlock()
    return nil
}

// RPC
func (pb *PBServer) Transfer(args *TransferArgs, reply *TransferReply) error {
    pb.mu.Lock()
    if pb.isBackup == true {
        //pb.dataLock.Lock()
        pb.database = args.Database
        pb.handleHistory = args.HandleHistory
        //pb.dataLock.Unlock()
        reply.Err = OK
    } else {
        reply.Err = ErrWrongServer
    }
    pb.mu.Unlock()
    return nil
}

func (pb *PBServer) updateView() {
    // ping for current view
    primaryChanged := false
    backupChanged := false
    currentView, err := pb.vs.Ping(pb.view.Viewnum)
    if err != nil {
        // ping error occurred
        return
    }
    if currentView.Viewnum > pb.view.Viewnum {
        primaryChanged = pb.view.Primary != currentView.Primary
        backupChanged = pb.view.Backup != currentView.Backup
        pb.view = currentView
        pb.vs.Ping(pb.view.Viewnum) // ack
    }
    //log.Printf("%v\n%v", pb.view.Primary, pb.view.Backup)
     
    // update fields	    
    pb.responseTime = time.Now()
    pb.isActive = true
    if primaryChanged == true || backupChanged == true {
        if pb.me == pb.view.Primary {
            // is current primary
            if pb.isPrimary != true {
	            // just promoted to primary
                pb.isPrimary = true
                pb.isBackup = false
            } else if backupChanged == true {
                // send database to backup
                pb.needTransfer = true
            }
        } else if pb.me == pb.view.Backup {
            // is current backup
            if pb.isBackup != true {
                // just changed to backup
                pb.isPrimary = false
                pb.isBackup = true
            }
        } else {
            // is currently idle
            pb.isPrimary = false
            pb.isBackup = false
        }
    }
    if pb.isPrimary == true && pb.view.Backup != "" {
        if pb.needTransfer == true {
            pb.transferBackup()
        }
    } else {
        pb.needTransfer = false
    }
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	deadInterval := viewservice.PingInterval * 3
	if pb.isdead() == false {
	    // check if is active
	    pb.mu.Lock()
	    if pb.view.Viewnum == 0 || time.Now().Sub(pb.responseTime) > deadInterval {
	        pb.isActive = false
	        pb.updateView()
	    }
	    pb.mu.Unlock()
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.database = make(map[string]string)
	pb.handleHistory = make(map[int64]bool)
	pb.view = viewservice.View{0, "", ""}
	pb.isPrimary = false
	pb.needTransfer = false
	pb.isBackup = false
	pb.isActive = false

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
