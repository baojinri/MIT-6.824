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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
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

	//logs []LogE

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//fmt.Println(rf.status)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	if rf.status == "leader" {
		isleader = true
		term = rf.currentTerm
		return term, isleader
	}

	isleader = false
	term = rf.currentTerm

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
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
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else if rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.mu.Lock()
	reply.Success = true
	reply.Term = args.Term
	rf.status = "follower"
	rf.currentTerm = args.Term
	rf.heartbeat <- true
	rf.votedFor = -1
	rf.mu.Unlock()

	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {

	reply := AppendEntriesReply{}
	if rf.status != "leader" {
		return
	}
	//fmt.Printf("leader %d send heartbeat to %d \n", rf.me, server)

	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	if !ok {
		//fmt.Println("<<<<<<<<")
		return
	}
	//fmt.Printf("leader %d send heartbeat to %d \n", rf.me, server)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != "leader" || rf.currentTerm != args.Term {
		return
	}

	if !reply.Success {
		//fmt.Println("<<<<<<<")
		//fmt.Println(rf.me)
		rf.currentTerm = reply.Term
		rf.status = "follower"
		rf.votedFor = -1
		return
	}
	//fmt.Printf("leader %d send heartbeat to %d \n", rf.me, server)
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

	//fmt.Println(ok, reply.VoteGranted, rf.status)

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = "follower"
		rf.votedFor = -1
	}

	if reply.VoteGranted {
		//fmt.Printf("%d vote %d", server, args.CandidateId)
		//fmt.Println(".......")
		rf.voteCount++
		//fmt.Printf("server %d,vote %d", rf.me, rf.voteCount)
		//fmt.Println(".......")

		if rf.voteCount > len(rf.peers)/2 {
			//fmt.Println("leader success")
			rf.status = "leader"
			rf.winElection <- true
			rf.heartBeat()

			//rf.updateStatus("leader")
			//fmt.Println("leader success")
			//fmt.Println(rf.currentTerm)

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
		//fmt.Println(rf.status)
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//rf.mu.Lock()
		status := rf.status
		//rf.mu.Unlock()
		switch status {
		case "follower":
			//fmt.Println("<<<<<<<<")
			select {
			case <-rf.heartbeat:
				//fmt.Println("heartbeat")
			case <-time.After(randTime()):
				rf.mu.Lock()
				rf.status = "candidate"
				rf.mu.Unlock()
				//fmt.Println("follower")
				go rf.startElection()
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
				//fmt.Println(rf.status)
				go rf.startElection()
			}
		case "leader":
			select {
			case <-time.After(60 * time.Microsecond):
				//fmt.Println("<<<<<<<<<")
				rf.heartBeat()
			}
		}
	}
}

func (rf *Raft) heartBeat() {
	if rf.status != "leader" {
		return
	}
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(i, &args)
	}

	return
}

func (rf *Raft) startElection() {
	time.Sleep(time.Millisecond * 100)

	rf.mu.Lock()

	if rf.status != "candidate" {
		rf.mu.Unlock()
		return
	}

	if rf.votedFor != -1 && rf.votedFor != rf.me {
		rf.mu.Unlock()
		return
	}

	//fmt.Println("<<<<<<<<")

	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.currentTerm += 1
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, &args)
	}
	return
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

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func randTime() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Millisecond * time.Duration((r.Intn(200) + 300))
}
