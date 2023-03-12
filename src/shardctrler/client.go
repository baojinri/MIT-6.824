package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	mu      sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientId int64
	OpId     int
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

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	ck.OpId++
	ck.mu.Unlock()
	args := &CommandArgs{ClientId: ck.ClientId, OpId: ck.OpId, Op: "Query"}

	args.QueryArgs.Num = num
	for {
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardCtrler.ClientRequest", args, &reply)
			if ok && reply.Err == OK {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	ck.OpId++
	ck.mu.Unlock()
	args := &CommandArgs{ClientId: ck.ClientId, OpId: ck.OpId, Op: "Join"}
	args.JoinArgs.Servers = servers

	for {
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardCtrler.ClientRequest", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	ck.OpId++
	ck.mu.Unlock()
	args := &CommandArgs{ClientId: ck.ClientId, OpId: ck.OpId, Op: "Leave"}
	args.LeaveArgs.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardCtrler.ClientRequest", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	ck.OpId++
	ck.mu.Unlock()
	args := &CommandArgs{ClientId: ck.ClientId, OpId: ck.OpId, Op: "Move"}
	args.MoveArgs.Shard = shard
	args.MoveArgs.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardCtrler.ClientRequest", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
