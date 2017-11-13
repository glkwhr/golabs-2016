package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	pingHistory map[string]time.Time
	currentView View
	primaryResponse bool
	backupResponse bool
	idleServers []string
}

func (vs *ViewServer) getNewBackup() string {
    deadInterval := PingInterval * DeadPings
    now := time.Now()
    for len(vs.idleServers) > 0 {
        candidateServer := vs.idleServers[0]
        if len(vs.idleServers) > 0 {
            vs.idleServers = vs.idleServers[1:]
        }
        diff := now.Sub(vs.pingHistory[candidateServer])
        if diff < deadInterval {
            // is an active idle server
            return candidateServer
        }
    }
    return ""
}

func (vs *ViewServer) promoteToPrimary() {
    vs.currentView.Viewnum += 1
    vs.currentView.Primary = ""
    if vs.currentView.Backup != "" {
        vs.currentView.Primary = vs.currentView.Backup
        vs.currentView.Backup = vs.getNewBackup()
    }
    vs.primaryResponse = true
    vs.backupResponse = false
}

func (vs *ViewServer) promoteToBackup() {
    vs.currentView.Viewnum += 1
    vs.currentView.Backup = vs.getNewBackup()
    vs.backupResponse = false
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()	
	vs.pingHistory[args.Me] = time.Now()
	//log.Printf("%v pings %v\n", args.Me, args.Viewnum)
	if args.Viewnum == 0 {
	    newIdleServer := true
	    if vs.currentView.Viewnum == 0 {
	        // first primary
	        newIdleServer = false
	        vs.currentView.Viewnum = 1
	        vs.currentView.Primary = args.Me
	    } else {
	        // recovered or new server
	        //log.Printf("adding a new idle server...\n")
	        if vs.currentView.Primary == args.Me {
    	        //log.Printf("primary %v was %v dead\n", args.Me, vs.primaryResponse)
	            if vs.primaryResponse {
	                vs.promoteToPrimary()
	            } else {
	                newIdleServer = false
	            }
	        } else if vs.currentView.Backup == args.Me {
	            //log.Printf("backup %v was %v dead\n", args.Me, vs.backupResponse)
	            if vs.primaryResponse {
	                vs.promoteToBackup()
	            } else {
	                newIdleServer = false
	            }
	        }
            if newIdleServer == true {
                inList := false
                for _, x := range vs.idleServers {
                    if x == args.Me {
                        inList = true
                        break
                    }
                }
                if inList == false {
                    //log.Printf("**new idle server %v\n", args.Me)
        	        vs.idleServers = append(vs.idleServers, args.Me)
        	    }
        	}
	    }
	} else if args.Viewnum == vs.currentView.Viewnum {
	    //log.Printf("%v %v", args.Viewnum, vs.currentView.Viewnum)
	    // responding server
	    if vs.currentView.Primary == args.Me {
	        //log.Printf("primary %v responds to %v\n", args.Me, args.Viewnum)
	        vs.primaryResponse = true
	    } else if vs.currentView.Backup == args.Me {
	        //log.Printf("backup %v responds to %v\n", args.Me, args.Viewnum)
	        vs.backupResponse = true
	    }
	    // else is active idle server
	}
	
	reply.View = vs.currentView
	vs.mu.Unlock()

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	reply.View = vs.currentView
	vs.mu.Unlock()

	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	deadInterval := PingInterval * DeadPings
	if vs.isdead() != true {
	    vs.mu.Lock()
	    now := time.Now()
	    if vs.currentView.Viewnum > 0 && vs.primaryResponse == true {
	        //log.Printf("checking next view...\n")
	        nextView := View{}
	        nextView.Viewnum = vs.currentView.Viewnum + 1
	        nextView.Primary = vs.currentView.Primary
	        nextView.Backup = vs.currentView.Backup
	        promoted := false
	        changed := false
	        if (vs.currentView.Primary == "") || (now.Sub(vs.pingHistory[vs.currentView.Primary]) > deadInterval) {
	            // needs a new primary server
	            //log.Printf("needs a new primary server\n")
	            nextView.Primary = ""
	            changed = vs.currentView.Primary == ""
	            if vs.currentView.Backup != "" && vs.backupResponse {
	                // promote current backup server
	                //log.Printf("promotes current backup server: %v\n", vs.currentView.Backup)
	                nextView.Primary = vs.currentView.Backup
	                nextView.Backup = ""
	                promoted = true
	            }
	        }
	        if promoted == true || vs.currentView.Backup == "" || (now.Sub(vs.pingHistory[vs.currentView.Backup]) > deadInterval) {
	            // needs a new backup server
	            //log.Printf("needs a new backup server\n")
	            nextView.Backup = vs.getNewBackup()
	            changed = nextView.Backup != vs.currentView.Backup
	        }
	        
	        if changed == true {
	            //log.Printf("changes view to %v\n", nextView.Viewnum)
	            vs.currentView = nextView
	            vs.primaryResponse = vs.currentView.Primary == ""
	            vs.backupResponse = false
	        }	        
	    }
	    //log.Printf("%v view %v:\n", vs.me, vs.currentView.Viewnum)
        //log.Printf("primary %v %v %v\n", vs.currentView.Primary, vs.primaryResponse, now.Sub(vs.pingHistory[vs.currentView.Primary]))
        //log.Printf("backup %v %v %v\n", vs.currentView.Backup, vs.backupResponse, now.Sub(vs.pingHistory[vs.currentView.Backup]))
        //log.Printf("dead interval %v\n", deadInterval)
        //log.Printf("idle servers: %v\n", len(vs.idleServers))
	    vs.mu.Unlock()
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.pingHistory = make(map[string]time.Time)
	vs.currentView = View{}
	vs.currentView.Viewnum = 0
	vs.currentView.Primary = ""
	vs.currentView.Backup = ""
	vs.primaryResponse = false
	vs.backupResponse = false
	vs.idleServers = make([]string, 0)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
