package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type OpArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientId int64
	OpId     int
}

type OpReply struct {
	Err   Err
	Value string
}
