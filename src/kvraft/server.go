package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key      string
	Value    string
	ClientId int64
	OpId     int
	Op       string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	opId map[int64]int
	data map[string]string
	recv map[int]chan Op

	// Your definitions here.
}

func (kv *KVServer) ClientRequest(args *OpArgs, reply *OpReply) {
	kv.mu.Lock()
	if kv.opId[args.ClientId] >= args.OpId {
		if args.Op == "Get" {
			if value, ok := kv.data[args.Key]; ok {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	commend := Op{Key: args.Key, Value: args.Value, ClientId: args.ClientId, OpId: args.OpId, Op: args.Op}
	index, _, isLeader := kv.rf.Start(commend)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//fmt.Println(args)
	kv.mu.Lock()
	kv.recv[index] = make(chan Op)
	kv.mu.Unlock()

	select {
	case item := <-kv.recv[index]:
		if item.ClientId != args.ClientId || item.OpId != args.OpId {
			reply.Err = ErrWrongLeader
		} else {
			if item.Op == "Get" {
				kv.mu.Lock()
				if value, ok := kv.data[item.Key]; ok {
					reply.Err = OK
					reply.Value = value
				} else {
					//fmt.Println("<<<<<<")
					reply.Err = ErrNoKey
					reply.Value = ""
				}
				kv.mu.Unlock()
			} else {
				reply.Err = OK
			}
		}
	case <-time.After(time.Second):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	close(kv.recv[index])
	delete(kv.recv, index)
	kv.mu.Unlock()

	return
}

func (kv *KVServer) upDate() {
	for kv.killed() == false {
		select {
		case item := <-kv.applyCh:
			if item.CommandValid {
				kv.mu.Lock()
				op := item.Command.(Op)
				if op.OpId > kv.opId[op.ClientId] {
					kv.opId[op.ClientId] = op.OpId
					//fmt.Println(kv.opId[op.ClientId])
					//fmt.Println(op.Key)
					switch op.Op {
					case "Put":
						//fmt.Println(op.Value)
						kv.data[op.Key] = op.Value
					case "Append":
						//fmt.Println(op.Value)
						kv.data[op.Key] += op.Value
					}
				}
				if _, ok := kv.recv[item.CommandIndex]; ok {
					kv.recv[item.CommandIndex] <- op
				}
				kv.mu.Unlock()
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)
	kv.opId = make(map[int64]int)
	kv.recv = make(map[int]chan Op)
	go kv.upDate()

	return kv
}
