package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Op    string
    Seq   int64
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.
type TransferArgs struct {
    Database map[string]string
    HandleHistory map[int64]bool
    Me string
}

type TransferReply struct {
    Err Err
}

type ForwardArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Op    string
    Seq   int64
    Me    string
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type ForwardReply struct {
	Err Err
}
