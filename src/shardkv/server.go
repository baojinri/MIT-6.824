package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

type Op struct {
	Key      string
	Value    string
	ClientId int64
	OpId     int
	Op       string
}

type RaftLogCommand struct {
	CommandType string
	Command     interface{}
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	dead         int32

	sm         *shardctrler.Clerk
	prevConfig shardctrler.Config
	currConfig shardctrler.Config

	data map[int]*Shard
	recv map[int]chan RaftLogCommand
	OpId map[int64]int

	lastIndex int
}

type Shard struct {
	Status string // valid，invalid，wait
	Kv     map[string]string
}

func (kv *ShardKV) ClientRequest(args *OpArgs, reply *OpReply) {
	shard := key2shard(args.Key)

	if kv.currConfig.Shards[shard] != kv.gid || kv.data[shard].Status != "valid" {
		reply.Err = ErrWrongGroup
		return
	}

	kv.mu.Lock()
	if kv.OpId[args.ClientId] >= args.OpId {
		if args.Op == "Get" {
			if value, ok := kv.data[shard].Kv[args.Key]; ok {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = OK
		}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	commend := Op{Key: args.Key, Value: args.Value, ClientId: args.ClientId, OpId: args.OpId, Op: args.Op}

	index, _, isLeader := kv.rf.Start(newRaftLogCommand("client", commend))

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	kv.recv[index] = make(chan RaftLogCommand)
	kv.mu.Unlock()

	select {
	case msg := <-kv.recv[index]:
		item := msg.Command.(Op)
		if item.ClientId != args.ClientId || item.OpId != args.OpId {
			reply.Err = ErrWrongLeader
		} else {
			if item.Op == "Get" {
				kv.mu.Lock()
				if value, ok := kv.data[key2shard(item.Key)].Kv[item.Key]; ok {
					reply.Err = OK
					reply.Value = value
				} else {
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

func (kv *ShardKV) applyOp(command *RaftLogCommand) {
	op := command.Command.(Op)
	shard := key2shard(op.Key)
	if op.OpId > kv.OpId[op.ClientId] {
		kv.OpId[op.ClientId] = op.OpId
		switch op.Op {
		case "Put":
			kv.data[shard].Kv[op.Key] = op.Value
		case "Append":
			kv.data[shard].Kv[op.Key] += op.Value
		}
	}
	return
}

func (kv *ShardKV) config() {
	canFetchConf := true
	kv.mu.Lock()
	currConfNum := kv.currConfig.Num
	for _, shard := range kv.data {
		if shard.Status == "wait" {
			canFetchConf = false
			break
		}
	}
	kv.mu.Unlock()

	if canFetchConf {
		latestConfig := kv.sm.Query(currConfNum + 1)
		if latestConfig.Num == currConfNum+1 {
			kv.rf.Start(newRaftLogCommand("config", latestConfig))
		}
	}
}

func (kv *ShardKV) applyConfig(command *RaftLogCommand) {
	config := command.Command.(shardctrler.Config)
	if config.Num == kv.currConfig.Num+1 {
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.data[i].Status == "valid" {
				kv.data[i].Status = ""
			}
		}
		for shardID, gid := range config.Shards {
			if gid == kv.gid {
				if kv.data[shardID].Status == "invalid" {
					if kv.currConfig.Num == 0 {
						kv.data[shardID].Status = "valid"
					} else {
						kv.data[shardID].Status = "wait"
					}
				}
				if kv.data[shardID].Status == "" {
					kv.data[shardID].Status = "valid"
				}
			}
		}
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.data[i].Status == "" {
				kv.data[i].Status = "invalid"
			}
		}
		kv.prevConfig = kv.currConfig
		kv.currConfig = config
	}
	return
}

type ShardOperationRequest struct {
	ConfigNum int
	Shards    []int
}

type ShardOperationResponse struct {
	Err
	Data      map[int]map[string]string
	ConfigNum int
	OpId      map[int64]int
}

func (kv *ShardKV) shard() {
	kv.mu.Lock()
	shards := make([]int, 0)
	for shardId, shard := range kv.data {
		if shard.Status == "wait" {
			shards = append(shards, shardId)
		}
	}

	if len(shards) == 0 {
		kv.mu.Unlock()
		return
	}
	gid2shardIDs := make(map[int][]int)
	for _, shardId := range shards {
		gid2shardIDs[kv.prevConfig.Shards[shardId]] = append(gid2shardIDs[kv.prevConfig.Shards[shardId]], shardId)
	}
	var wg sync.WaitGroup
	for gid, shardIDs := range gid2shardIDs {
		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			pullTaskRequest := ShardOperationRequest{configNum, shardIDs}
			for _, server := range servers {
				var pullTaskResponse ShardOperationResponse
				srv := kv.make_end(server)
				if srv.Call("ShardKV.GetShardsData", &pullTaskRequest, &pullTaskResponse) && pullTaskResponse.Err == OK {
					kv.rf.Start(newRaftLogCommand("shard", pullTaskResponse))
				}
			}
		}(kv.prevConfig.Groups[gid], kv.currConfig.Num, shardIDs)
	}
	kv.mu.Unlock()
	wg.Wait()
	return
}

func (kv *ShardKV) GetShardsData(request *ShardOperationRequest, response *ShardOperationResponse) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.currConfig.Num < request.ConfigNum {
		response.Err = ErrWrongLeader
		return
	}

	response.Data = make(map[int]map[string]string)
	for _, shardID := range request.Shards {
		response.Data[shardID] = deepCopy(kv.data[shardID].Kv)
	}

	response.OpId = make(map[int64]int)
	for clientId, opId := range kv.OpId {
		response.OpId[clientId] = opId
	}

	response.ConfigNum = request.ConfigNum

	response.Err = OK

	return
}

func (kv *ShardKV) applyShard(command *RaftLogCommand) {
	shards := command.Command.(ShardOperationResponse)

	if shards.ConfigNum == kv.currConfig.Num {
		for shardId, shard := range shards.Data {
			kv.data[shardId].Kv = deepCopy(shard)
			kv.data[shardId].Status = "valid"
		}

		for clientId, opId := range shards.OpId {
			if lastOperation, ok := kv.OpId[clientId]; !ok || lastOperation < opId {
				kv.OpId[clientId] = opId
			}
		}
	}
	return
}

func (kv *ShardKV) apply() {
	for !kv.killed() {
		select {
		case item := <-kv.applyCh:
			if item.CommandValid {
				kv.mu.Lock()
				if item.CommandIndex <= kv.lastIndex {
					kv.mu.Unlock()
					continue
				}
				kv.lastIndex = item.CommandIndex

				command := item.Command.(RaftLogCommand)
				switch command.CommandType {
				case "client":
					kv.applyOp(&command)
				case "config":
					kv.applyConfig(&command)
				case "shard":
					kv.applyShard(&command)
				}

				if _, ok := kv.recv[item.CommandIndex]; ok {
					kv.recv[item.CommandIndex] <- command
				}

				if kv.rf.Need(kv.maxraftstate) && kv.maxraftstate != -1 {
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(kv.data)
					e.Encode(kv.OpId)
					e.Encode(kv.prevConfig)
					e.Encode(kv.currConfig)
					kv.rf.Snapshot(kv.lastIndex, w.Bytes())
				}
				kv.mu.Unlock()
			}
			if item.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(item.SnapshotTerm, item.SnapshotIndex, item.Snapshot) {
					kv.lastIndex = item.SnapshotIndex
					kv.saveSnapShot(item.Snapshot)
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *ShardKV) saveSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.data) != nil || d.Decode(&kv.OpId) != nil || d.Decode(&kv.prevConfig) != nil || d.Decode(&kv.currConfig) != nil {
	}
	return
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(RaftLogCommand{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardOperationResponse{})
	labgob.Register(Shard{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.sm = shardctrler.MakeClerk(kv.ctrlers)
	kv.prevConfig = shardctrler.Config{}
	kv.currConfig = kv.sm.Query(-1)
	kv.currConfig.Num = 0

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[int]*Shard, shardctrler.NShards)
	kv.OpId = make(map[int64]int)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.data[i] = &Shard{Status: "invalid"}
		kv.data[i].Kv = make(map[string]string)
	}
	kv.recv = make(map[int]chan RaftLogCommand)
	kv.lastIndex = 0
	kv.saveSnapShot(persister.ReadSnapshot())
	go kv.apply()
	go kv.Monitor(kv.config)
	go kv.Monitor(kv.shard)

	return kv
}

func (kv *ShardKV) Monitor(action func()) {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newRaftLogCommand(commandType string, data interface{}) RaftLogCommand {
	return RaftLogCommand{
		CommandType: commandType,
		Command:     data,
	}
}

func deepCopy(data map[string]string) map[string]string {
	newShard := make(map[string]string)
	for k, v := range data {
		newShard[k] = v
	}
	return newShard
}
