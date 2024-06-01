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
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role             Role
	voteCount        int
	heartBeatChannel chan bool

	// Persistent State
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	logs        []LogEntry // log entries; each entry contains command for state machine, and term when entry was received

	// Volatile State
	commitIndex int
	lastApplied int

	// Volatile Leader State
	nextIndex  []int
	matchIndex []int
}

type Role string

const (
	Follower  Role = "Follower"
	Candidate      = "Candidate"
	Leader         = "Leader"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	if args.Term >= rf.currentTerm && rf.role != Leader {
		rf.log(fmt.Sprintf("Server %d granting vote to server %d\n", rf.me, args.CandidateId))
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Candidate {
		rf.voteCount = 1
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.voteCount = 1
		rf.role = Follower
		return ok
	}

	minRequiredVotes := len(rf.peers)/2 + 1
	if reply.VoteGranted {
		rf.log(fmt.Sprintf("Server %d has received a vote from %d\n", rf.me, server))
		rf.voteCount++
		if rf.voteCount >= minRequiredVotes {
			rf.log(fmt.Sprintf("Server %d has received enough votes, electing as leader\n", rf.me))
			rf.role = Leader
			rf.sendAppendEntriesToFollowers()
		}
	}

	return ok
}

func (rf *Raft) requestVoteFromAllServers() {
	if rf.role != Candidate {
		return
	}

	args := RequestVoteArgs{}
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex = -1
	args.LastLogTerm = -1

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		rf.log(fmt.Sprintf("Requesting votes from %d", server))
		go rf.sendRequestVote(server, &args, &RequestVoteReply{})
	}
}

type LogEntry struct {
	// command string
	// term    int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// check page 4 of paper
	Term    int
	Success bool
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// check page 4 of paper
	rf.log(fmt.Sprintf("Recieved appendEntries from %d at term %d", args.LeaderId, args.Term))
	if rf.role == Candidate {
		rf.role = Follower
	}
	if rf.role == Leader && args.Term > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = args.Term
	}

	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
	}

	rf.voteCount = 1
	rf.heartBeatChannel <- true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.log(fmt.Sprintf("Recieved append entries reply from %d term %d", server, reply.Term))

	return ok
}

func (rf *Raft) sendAppendEntriesToFollowers() {
	if rf.role != Leader {
		return
	}

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		rf.log(fmt.Sprintf("Sending append entries to %d", server))

		args := AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.PrevLogIndex = -1
		args.PrevLogTerm = -1
		args.LeaderCommit = rf.commitIndex
		args.Entries = []LogEntry{}

		go rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.role == Leader

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) log(msg string) {
	// fmt.Printf("%d(%d): %s\n", rf.currentTerm, rf.me, msg)
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.log(fmt.Sprintf("I am a %s", rf.role))
		switch rf.role {
		case Follower:
			select {
			case <-rf.heartBeatChannel:
				rf.role = Follower
				rf.voteCount = 0
				rf.votedFor = -1

			case <-time.After(time.Millisecond * time.Duration(500+rand.Intn(1000))):
				rf.currentTerm += 1
				rf.voteCount = 1 // vote for self
				rf.role = Candidate
			}
		case Candidate:
			rf.requestVoteFromAllServers()
		case Leader:
			select {
			case <-time.After(150 * time.Millisecond):
				rf.mu.Lock()
				rf.sendAppendEntriesToFollowers()
				rf.mu.Unlock()
			}
			continue
		}
		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.role = Follower
	rf.currentTerm = 0
	rf.heartBeatChannel = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
