package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntries struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	votedFor    int
	status      string
	currentTerm int
	voteCount   int
	heartbeat   chan bool
	winElection chan bool

	logs        []LogEntries
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg

	lastIncludeIndex int
	lastIncludeTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//fmt.Println(rf.status)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.status == "leader"
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	return
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var logs []LogEntries
	var lastIncludeIndex int
	var lastIncludeTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&lastIncludeIndex) != nil || d.Decode(&lastIncludeTerm) != nil {
		fmt.Println("error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
		rf.commitIndex = lastIncludeIndex
		rf.lastApplied = lastIncludeIndex
	}
	return
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.lastIncludeIndex {
		return false
	}

	var newLog []LogEntries
	newLog = append(newLog, LogEntries{Term: lastIncludedTerm})

	if lastIncludedIndex >= len(rf.logs)-1+rf.lastIncludeIndex {
		if rf.commitIndex < lastIncludedIndex {
			rf.commitIndex = lastIncludedIndex
		}
	} else if rf.logs[lastIncludedIndex-rf.lastIncludeIndex].Term != lastIncludedTerm {
		rf.commitIndex = lastIncludedIndex
	} else {
		newLog = append(newLog, rf.logs[lastIncludedIndex+1-rf.lastIncludeIndex:]...)
		if rf.commitIndex < lastIncludedIndex {
			rf.commitIndex = lastIncludedIndex
		}
	}
	rf.logs = newLog
	rf.lastIncludeIndex = lastIncludedIndex
	rf.lastIncludeTerm = lastIncludedTerm
	rf.lastApplied = lastIncludedIndex

	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)

	go rf.ApplyMsg()

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	if index <= rf.lastIncludeIndex {
		return
	}

	rf.lastIncludeTerm = rf.logs[index-rf.lastIncludeIndex].Term
	rf.logs = rf.logs[index-rf.lastIncludeIndex:]
	rf.logs[0].Command = nil

	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	rf.lastIncludeIndex = index

	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)

	go rf.ApplyMsg()

	return
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	rf.status = "follower"
	rf.currentTerm = args.Term
	rf.votedFor = -1
	reply.Term = args.Term
	rf.heartbeat <- true
	rf.persist()

	if args.LastIncludeIndex <= rf.lastIncludeIndex {
		return
	}

	rf.applyCh <- ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotTerm: args.LastIncludeTerm, SnapshotIndex: args.LastIncludeIndex}

	return
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntries
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConfTerm  int
	ConfIndex int
}

func (rf *Raft) ApplyMsg() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.logs[i-rf.lastIncludeIndex].Command, CommandIndex: i}
		rf.lastApplied = i
	}
	return
}

func (rf *Raft) IsUpToDate(args *RequestVoteArgs) bool {
	lastIndex := len(rf.logs) - 1 + rf.lastIncludeIndex
	lastTerm := rf.logs[lastIndex-rf.lastIncludeIndex].Term
	if lastTerm == args.LastLogTerm {
		return args.LastLogIndex >= lastIndex
	} else {
		return args.LastLogTerm > lastTerm
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if rf.votedFor == -1 && rf.IsUpToDate(args) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else if rf.votedFor == args.CandidateId && rf.IsUpToDate(args) {
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.status = "follower"
	rf.currentTerm = args.Term
	rf.votedFor = -1
	reply.Term = args.Term
	rf.heartbeat <- true

	if args.PrevLogIndex > len(rf.logs)-1+rf.lastIncludeIndex {
		reply.Success = false
		reply.ConfIndex = len(rf.logs) + rf.lastIncludeIndex
		reply.ConfTerm = -1
		return
	}

	if args.PrevLogIndex < rf.lastIncludeIndex {
		reply.Success = false
		reply.ConfIndex = rf.lastIncludeIndex + 1
		reply.ConfTerm = -1
		return
	}

	if rf.logs[args.PrevLogIndex-rf.lastIncludeIndex].Term == args.PrevLogTerm {
		reply.Success = true
		rf.logs = rf.logs[:args.PrevLogIndex+1-rf.lastIncludeIndex]
		rf.logs = append(rf.logs, args.Entries...)
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < len(rf.logs)-1+rf.lastIncludeIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.logs) - 1 + rf.lastIncludeIndex
			}
			go rf.ApplyMsg()
		}
	} else {
		reply.Success = false
		reply.ConfTerm = rf.logs[args.PrevLogIndex-rf.lastIncludeIndex].Term
		for i := args.PrevLogIndex - rf.lastIncludeIndex; i >= 0; i-- {
			if rf.logs[i].Term == reply.ConfTerm {
				reply.ConfIndex = i + rf.lastIncludeIndex
			} else {
				break
			}
		}
	}

	return
}

func (rf *Raft) sendInstallSnapshot(server int) {
	args := InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me, LastIncludeIndex: rf.lastIncludeIndex, LastIncludeTerm: rf.lastIncludeTerm, Data: rf.persister.ReadSnapshot()}
	reply := InstallSnapshotReply{}
	if rf.status != "leader" {
		return
	}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != "leader" || rf.currentTerm != args.Term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.status = "follower"
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		return
	}
	rf.nextIndex[server] = rf.lastIncludeIndex + 1
	rf.matchIndex[server] = rf.lastIncludeIndex
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {

	reply := AppendEntriesReply{}
	if rf.status != "leader" {
		return
	}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != "leader" || rf.currentTerm != args.Term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.status = "follower"
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		return
	}
	if reply.Success {
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatchIndex
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		if reply.ConfTerm == -1 {
			rf.nextIndex[server] = reply.ConfIndex
		} else {
			i := args.PrevLogIndex - rf.lastIncludeIndex
			for ; i > 0; i-- {
				if rf.logs[i].Term == reply.ConfTerm {
					rf.nextIndex[server] = i + 1 + rf.lastIncludeIndex
					break
				}
			}
			if i == 0 {
				rf.nextIndex[server] = reply.ConfIndex
			}
		}
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}

	for n := len(rf.logs) - 1 + rf.lastIncludeIndex; n > rf.commitIndex; n-- {
		cnt := 1
		if rf.logs[n-rf.lastIncludeIndex].Term == rf.currentTerm {
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					cnt += 1
				}
			}
		}
		if cnt > len(rf.peers)/2 {
			rf.commitIndex = n
			go rf.ApplyMsg()
			break
		}
	}

	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != "candidate" || args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = "follower"
		rf.votedFor = -1
		rf.persist()
	}

	if reply.VoteGranted {

		rf.voteCount++

		if rf.voteCount > len(rf.peers)/2 {

			rf.status = "leader"
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))

			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.logs) + rf.lastIncludeIndex
				rf.matchIndex[i] = 0
			}
			rf.winElection <- true

		}
	}
	return
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != "leader" {
		return -1, rf.currentTerm, false
	}

	rf.logs = append(rf.logs, LogEntries{command, rf.currentTerm})
	rf.persist()

	return len(rf.logs) - 1 + rf.lastIncludeIndex, rf.currentTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)
		//fmt.Println(rf.status)
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//rf.mu.Lock()
		//status := rf.status
		//rf.mu.Unlock()
		switch rf.status {
		case "follower":
			select {
			case <-rf.heartbeat:
			case <-time.After(randTime()):
				rf.mu.Lock()
				rf.status = "candidate"
				go rf.startElection()
				rf.mu.Unlock()
			}
		case "candidate":
			select {
			case <-rf.winElection:
			case <-rf.heartbeat:
			case <-time.After(randTime()):
				rf.mu.Lock()
				rf.status = "candidate"
				rf.votedFor = -1
				rf.mu.Unlock()
				go rf.startElection()
			}
		case "leader":
			select {
			case <-time.After(20 * time.Millisecond):
				go rf.heartBeat()
			}
		}
	}
}

func (rf *Raft) heartBeat() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != "leader" {
		return
	}

	for i := 0; i < len(rf.peers); i++ {

		if i == rf.me {
			continue
		}
		if rf.nextIndex[i] <= rf.lastIncludeIndex {
			go rf.sendInstallSnapshot(i)
		} else {
			args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, LeaderCommit: rf.commitIndex}
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.logs[args.PrevLogIndex-rf.lastIncludeIndex].Term
			entries := rf.logs[rf.nextIndex[i]-rf.lastIncludeIndex:]
			args.Entries = make([]LogEntries, len(entries))
			copy(args.Entries, entries)

			go rf.sendAppendEntries(i, &args)
		}
	}

	return
}

func (rf *Raft) startElection() {
	time.Sleep(time.Millisecond * 50)

	rf.mu.Lock()

	if rf.status != "candidate" {
		rf.mu.Unlock()
		return
	}

	if rf.votedFor != -1 && rf.votedFor != rf.me {
		rf.mu.Unlock()
		return
	}

	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.currentTerm += 1
	rf.persist()
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
	args.LastLogIndex = len(rf.logs) - 1 + rf.lastIncludeIndex
	args.LastLogTerm = rf.logs[args.LastLogIndex-rf.lastIncludeIndex].Term
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, &args)
	}
	return
}

func (rf *Raft) Need(size int) bool {
	return rf.persister.RaftStateSize() >= size
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.status = "follower"
	rf.currentTerm = 0
	rf.voteCount = 0

	rf.heartbeat = make(chan bool)
	rf.winElection = make(chan bool)

	rf.logs = append(rf.logs, LogEntries{Term: rf.currentTerm})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) shouldSnapShot(size int) bool {
	return rf.persister.RaftStateSize() >= size
}

func randTime() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Millisecond * time.Duration((r.Intn(300) + 200))
}
