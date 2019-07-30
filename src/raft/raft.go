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
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "../labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	Follower = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	CurrentTerm int //当前服务器被最近一任leader通知的term，单调递增
	VotedFor    int //投给了谁

	ElectionTimer        *time.Timer //选举计时器
	ResetElectionTimerCh chan bool   //用于重置选举计时器

	State int //Follower,Candidate,Leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //发起竞选者的term
	CandidateId  int //发起竞选者的Id
	LastLogIndex int //发起竞选者的最后一条日志的index
	LastLogTerm  int //发起竞选者的最后一条日志的term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //返回当前任期，若比发起竞选者当前的大则自动退选并更新任期
	VoteGranted bool //是否同意投该候选者
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here (2A, 2B, 2C).

	rf.State = Follower
	rf.VotedFor = -1

	//300-500ms
	rf.ElectionTimer = time.NewTimer(time.Millisecond * time.Duration(300+rand.Intn(50)*4))
	rf.ResetElectionTimerCh = make(chan bool)

	//开始参与竞选
	go startElectionDaemon(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

//关于竞选事务的守护进程
func startElectionDaemon(rf *Raft) {
	for {
		select {

		//重置计时器
		case <-rf.ResetElectionTimerCh:
			if !rf.ElectionTimer.Stop() {
				<-rf.ElectionTimer.C
			}
			//150-300ms
			rf.ElectionTimer.Reset(time.Millisecond * time.Duration(300+rand.Intn(50)*4))

		//选举计时器超时，自己成为竞选者
		case <-rf.ElectionTimer.C:
			changingIntoCandidate(rf)
		}
	}
}

//Follower计时器超时，转变为Candidate
func changingIntoCandidate(rf *Raft){
	var requestVoteArgs RequestVoteArgs

	rf.mu.Lock()
	rf.VotedFor = rf.me
	rf.CurrentTerm += 1
	rf.State = Candidate
	requestVoteArgs.Term = rf.CurrentTerm
	requestVoteArgs.CandidateId = rf.me
	rf.mu.Unlock()

	//该竞选者得票数
	votes := 0

	for id := 0; id < len(rf.peers); id++ {
		if id != rf.me {
			//对每一个peer开启一个go程，发送RequestVote并处理reply
			go func(id int) {
				var reply RequestVoteReply
				isReceived := rf.sendRequestVote(id, &requestVoteArgs, &reply)
				if isReceived {
					//根据reply的情况改变自己的状态，需要加锁
					rf.mu.Lock()
					//假如还没有在别的go程中竞选成功变成leader或失败成为follower
					if rf.State == Candidate {
						//收到同意票
						if reply.VoteGranted{
							votes++
							if votes > len(rf.peers)/2 {
								rf.State = Leader


							}
						}
						//从别处得知已经存在更高任期的leader，自动退位成follower
						if reply.Term > requestVoteArgs.Term {
							rf.CurrentTerm = reply.Term
							rf.State = Follower
							rf.VotedFor = -1
							//关键的状态部分改完后进行持久化，以便灾后恢复
							rf.persist()
							//重置选举计时器
							rf.ResetElectionTimerCh <- true
						}
					}

					rf.mu.Unlock()
				}
			}(id)
		}
	}
}
