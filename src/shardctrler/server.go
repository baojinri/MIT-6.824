package shardctrler

import (
	"6.824/raft"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	opId map[int64]int
	recv map[int]chan Op

	configs []Config // indexed by config num
}

type Op struct {
	Command CommandArgs
}

func (sc *ShardCtrler) ClientRequest(args *CommandArgs, reply *CommandReply) {
	sc.mu.Lock()
	if sc.opId[args.ClientId] >= args.OpId {
		if args.Op == "Query" {
			reply.Config, reply.Err = sc.Query(args.QueryArgs.Num)
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()
	commend := Op{*args}
	index, _, isLeader := sc.rf.Start(commend)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	sc.mu.Lock()
	sc.recv[index] = make(chan Op)
	sc.mu.Unlock()

	select {
	case item := <-sc.recv[index]:
		if item.Command.ClientId != args.ClientId || item.Command.OpId != args.OpId {
			reply.Err = ErrWrongLeader
		} else {
			if item.Command.Op == "Query" {
				sc.mu.Lock()
				reply.Config, reply.Err = sc.Query(args.QueryArgs.Num)
				sc.mu.Unlock()
			} else {
				reply.Err = OK
			}
		}

	case <-time.After(time.Second):
		reply.Err = ErrWrongLeader
	}

	sc.mu.Lock()
	close(sc.recv[index])
	delete(sc.recv, index)
	sc.mu.Unlock()

	return
}

func (sc *ShardCtrler) upDate() {
	for {
		select {
		case item := <-sc.applyCh:
			if item.CommandValid {
				sc.mu.Lock()
				op := item.Command.(Op)
				if op.Command.OpId > sc.opId[op.Command.ClientId] {
					sc.opId[op.Command.ClientId] = op.Command.OpId
					switch op.Command.Op {
					case "Move":
						sc.Move(op.Command.MoveArgs.Shard, op.Command.MoveArgs.GID)
					case "Join":
						sc.Join(op.Command.JoinArgs.Servers)
					case "Leave":
						sc.Leave(op.Command.LeaveArgs.GIDs)
					case "Query":
					}
				}
				if _, ok := sc.recv[item.CommandIndex]; ok {
					sc.recv[item.CommandIndex] <- op
				}
				sc.mu.Unlock()
			}
		}
	}
}

func (sc *ShardCtrler) Join(groups map[int][]string) {
	lenth := len(sc.configs)
	lastconfig := sc.configs[lenth-1]
	newConfig := Config{lenth, lastconfig.Shards, deepCopy(lastconfig.Groups)}
	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}
	sc.adjustConfig(&newConfig)
	sc.configs = append(sc.configs, newConfig)
	return
}

func (sc *ShardCtrler) Leave(gids []int) {
	lenth := len(sc.configs)
	lastconfig := sc.configs[lenth-1]
	newConfig := Config{lenth, lastconfig.Shards, deepCopy(lastconfig.Groups)}
	for _, gid := range gids {
		delete(newConfig.Groups, gid)
		for i, v := range newConfig.Shards {
			if v == gid {
				newConfig.Shards[i] = 0
			}
		}
	}
	sc.adjustConfig(&newConfig)
	sc.configs = append(sc.configs, newConfig)
	return
}

func (sc *ShardCtrler) Move(shard int, gid int) {
	lenth := len(sc.configs)
	lastconfig := sc.configs[lenth-1]
	newConfig := Config{lenth, lastconfig.Shards, deepCopy(lastconfig.Groups)}
	newConfig.Shards[shard] = gid
	sc.configs = append(sc.configs, newConfig)
	return
}

func (sc *ShardCtrler) Query(num int) (Config, Err) {
	if num < 0 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1], OK
	}
	return sc.configs[num], OK
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.opId = make(map[int64]int)
	sc.recv = make(map[int]chan Op)

	go sc.upDate()

	return sc
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

func (sc *ShardCtrler) adjustConfig(config *Config) {
	if len(config.Groups) == 0 {
		config.Shards = [NShards]int{}
	} else if len(config.Groups) == 1 {
		// set shards one gid
		for k, _ := range config.Groups {
			for i, _ := range config.Shards {
				config.Shards[i] = k
			}
		}
	} else if len(config.Groups) <= NShards {
		avg := NShards / len(config.Groups)
		// 每个 gid 分 avg 个 shard
		otherShardsCount := NShards - avg*len(config.Groups)
		needLoop := false
		lastGid := 0

	LOOP:
		var keys []int
		for k := range config.Groups {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		for _, gid := range keys {
			lastGid = gid
			count := 0
			for _, val := range config.Shards {
				if val == gid {
					count += 1
				}
			}

			if count == avg {
				continue
			} else if count > avg && otherShardsCount == 0 {
				c := 0
				for i, val := range config.Shards {
					if val == gid {
						if c == avg {
							config.Shards[i] = 0
						} else {
							c += 1
						}
					}
				}

			} else if count > avg && otherShardsCount > 0 {
				c := 0
				for i, val := range config.Shards {
					if val == gid {
						if c == avg+otherShardsCount {
							config.Shards[i] = 0
						} else {
							if c == avg {
								otherShardsCount -= 1
							} else {
								c += 1
							}

						}
					}
				}

			} else {
				for i, val := range config.Shards {
					if count == avg {
						break
					}
					if val == 0 && count < avg {
						config.Shards[i] = gid
					}
				}

				if count < avg {
					needLoop = true
				}

			}

		}

		if needLoop {
			needLoop = false
			goto LOOP
		}

		if lastGid != 0 {
			for i, val := range config.Shards {
				if val == 0 {
					config.Shards[i] = lastGid
				}
			}
		}

	} else {
		gids := make(map[int]int)
		emptyShards := make([]int, 0, NShards)
		for i, gid := range config.Shards {
			if gid == 0 {
				emptyShards = append(emptyShards, i)
				continue
			}
			if _, ok := gids[gid]; ok {
				emptyShards = append(emptyShards, i)
				config.Shards[i] = 0
			} else {
				gids[gid] = 1
			}
		}
		n := 0
		if len(emptyShards) > 0 {
			var keys []int
			for k := range config.Groups {
				keys = append(keys, k)
			}
			sort.Ints(keys)
			for _, gid := range keys {
				if _, ok := gids[gid]; !ok {
					config.Shards[emptyShards[n]] = gid
					n += 1
				}
				if n >= len(emptyShards) {
					break
				}
			}
		}
	}
	return
}
