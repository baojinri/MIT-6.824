package kvraft

import (
	"6.824/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	mu      sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientId int64
	OpId     int
	leaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.ClientId = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	ck.OpId++
	ck.mu.Unlock()
	value := ck.sendRequest(key, "", "Get")
	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	_ = ck.sendRequest(key, value, op)
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.mu.Lock()
	ck.OpId++
	ck.mu.Unlock()
	ck.PutAppend(key, value, "Put")
	return
}
func (ck *Clerk) Append(key string, value string) {
	ck.mu.Lock()
	ck.OpId++
	ck.mu.Unlock()
	ck.PutAppend(key, value, "Append")
	return
}

func (ck *Clerk) sendRequest(key string, value string, op string) string {
	for {
		args := OpArgs{key, value, op, ck.ClientId, ck.OpId}
		reply := OpReply{}
		ok := ck.sendRPCRequest(ck.leaderId, &args, &reply)
		if ok {
			if reply.Err == OK {
				//fmt.Println("0000")
				//fmt.Println(reply.Value)
				return reply.Value
			} else if reply.Err == ErrNoKey {
				//fmt.Println("1111")
				return reply.Value
			} else {
				ck.mu.Lock()
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
				ck.mu.Unlock()
				//fmt.Println("3333")
			}
		} else {
			ck.mu.Lock()
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			ck.mu.Unlock()
			//fmt.Println("4444")
		}
	}
}

func (ck *Clerk) sendRPCRequest(leaderId int, args *OpArgs, reply *OpReply) bool {

	ok := ck.servers[leaderId].Call("KVServer.ClientRequest", args, reply)

	return ok
}
