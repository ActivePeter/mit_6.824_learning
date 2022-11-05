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
	"log"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
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

const (
	StateFollower = 0
	StateCandi    = 1
	StateLeader   = 2
)

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
	recentHeartBeatRecved bool
	term                  int
	state                 int
	thisTermVoted         bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	term = rf.term
	isleader = rf.state == StateLeader
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int //候选人任期
	CandidateId int //候选人 id

	LastLogIndex int //候选人 的logs 需要保证
	LastLogTerm  int // 同上
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term  int  //投票方term
	Voted bool //是否投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	log.Println(rf.me, "recv req vote from", args.CandidateId)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()



	reply.Term = rf.term
	switch rf.state {
	case StateLeader:
		if args.Term > rf.term {
			reply.Voted = true
			rf.thisTermVoted = true
			rf.state = StateFollower
			rf.term=args.Term
		} else {
			reply.Voted = false
		}
		//log.Println("request vote leader handle not impl")

		//if args.Term>rf.term{
		//	rf.state=StateFollower
		//
		//}
	case StateFollower:
		//follower保证请求者term更大，且当前term还没投过票

		if args.Term > rf.term && !rf.thisTermVoted {
			rf.thisTermVoted = true
			reply.Voted = true
			rf.recentHeartBeatRecved = true
			log.Printf("follower %v vote %v\n", rf.me, args.CandidateId)
		} else {
			reply.Voted = false
			log.Printf("follower %v didnt vote %v ,term smaller %v,term voted %v\n",
				rf.me, args.CandidateId,args.Term > rf.term,rf.thisTermVoted)
		}
		if args.Term>rf.term{
			rf.term=args.Term
			rf.thisTermVoted=false
		}
	case StateCandi:
		if args.Term > rf.term {
			rf.state = StateFollower
			rf.thisTermVoted = false
			reply.Voted = true
			rf.term=args.Term
			log.Printf("candi %v vote %v and become follower\n", rf.me, args.CandidateId)
		} else {
			//竞选者不投
			log.Printf("candi %v didnt vote %v\n", rf.me, args.CandidateId)
			reply.Voted = false
		}
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
	return ok
}

type AppendEntryArg struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
}
type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) HandleAppendEntry(args *AppendEntryArg, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch rf.state {
	case StateLeader:
		if args.Term > rf.term {
			rf.term = args.Term
			rf.thisTermVoted = false
			rf.state = StateFollower
		}
		//log.Println("not handled append entry recv when leader")
	case StateFollower:
		log.Println("follower", rf.me, "recv append from", args.LeaderId)
		rf.recentHeartBeatRecved = true
		if args.Term > rf.term {
			rf.term = args.Term
			rf.thisTermVoted = false
		}
	case StateCandi:
		log.Println("candi", rf.me, "recv append from", args.LeaderId)
		reply.Term = rf.term
		if args.Term >= rf.term {
			if args.Term > rf.term {
				rf.thisTermVoted = false
			}
			rf.term = args.Term
			rf.state = StateFollower
		}
		//log.Println("not handled append entry recv when candi")
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArg, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.HandleAppendEntry", args, reply)
	return ok
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
	isLeader := true

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

// raft 超时开始竞选
const (
	//单位ms
	electTimeoutMax = 350
	electTimeoutMin = 250
)

func getRandElectTimeout() int {
	timeout := rand.Intn(electTimeoutMax-electTimeoutMin) + electTimeoutMin
	return timeout
}
func (rf *Raft) randElectTimeoutAndSleep() {
	timeout := rand.Intn(electTimeoutMax-electTimeoutMin) + electTimeoutMin
	time.Sleep(time.Duration(timeout) * time.Millisecond)
}

func (rf *Raft) candi() {
	log.Println(rf.me, "become candi")
	//1.自增任期号，转变角色
	rf.term++
	rf.thisTermVoted = false
	rf.state = StateCandi
	timebegin := time.Now().UnixMilli()
	timeout := getRandElectTimeout() * 5
	//todo commit 信息
	argModel := RequestVoteArgs{}
	argModel.Term = rf.term
	argModel.CandidateId = rf.me

	replyModel := RequestVoteReply{}
	votes := 1 //给自己投一票
	doneCnts := 1
	for key := range rf.peers {
		if key == rf.me {
			continue
		}
		keyclone := key
		task := func() {
			reply := replyModel
			log.Println(rf.me, "send req vote to", keyclone)
			ok := rf.sendRequestVote(keyclone, &argModel, &reply)
			//fmt.Printf("req vote ok ? %v\n", ok)
			if ok && reply.Voted {
				log.Println(rf.me, "get vote from", keyclone)
				votes++
			}
			doneCnts++
		}
		go task()
	}

	//等达到半数,
	// 或者没到半数但全部发出，
	// 或者已经不是竞选者
	// 或者随机的选举超时
	for votes <= len(rf.peers)/2 &&
		rf.state == StateCandi &&
		doneCnts < len(rf.peers) {
		timenow := time.Now().UnixMilli()

		//选举超时，可能选票被瓜分
		if timenow-timebegin > int64(timeout) {
			log.Println(rf.me, "candi timeout")
			break
		}
	}

	//有半数票。并且没有取消candi身份
	rf.mu.Lock()
	if votes > len(rf.peers)/2 &&rf.state==StateCandi{
		//达到半数，进入leader
		log.Println( rf.me,"become leader in term",rf.term)
		rf.state = StateLeader
	} else {
		rf.state = StateFollower
	}
	rf.mu.Unlock()
}

func (rf *Raft) LeaderBroadcastHeartbeat() {
	rf.mu.Lock()
	argmodel := AppendEntryArg{
		Term:     rf.term,
		LeaderId: rf.me,
		//todo about commit info
	}
	replymodel := AppendEntryReply{Term: 0}
	rf.mu.Unlock()
	for key := range rf.peers {
		if key == rf.me {
			continue
		}
		reply := replymodel
		keyclone := key
		send := func() {
			rf.sendAppendEntry(
				keyclone, &argmodel, &reply)
			// leader to follower
			rf.mu.Lock()
			if reply.Term > rf.term {
				if rf.state == StateLeader {
					rf.state = StateFollower
					rf.thisTermVoted = false
				}
			}
			rf.mu.Unlock()
		}
		go send()
	}
}

func (rf *Raft) ifLeaderThenHold() {
	//if rf.state != StateLeader {
	//	return
	//}
	//群发空append
	for rf.state == StateLeader {
		rf.LeaderBroadcastHeartbeat()
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		//休眠一段时间
		rf.randElectTimeoutAndSleep()
		//看是否有心跳
		rf.mu.Lock()
		if rf.recentHeartBeatRecved {
			rf.recentHeartBeatRecved = false
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		//没收到心跳开始竞选
		rf.candi()
		rf.ifLeaderThenHold()
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
	rf.term = 0
	rf.recentHeartBeatRecved = false
	rf.state = StateFollower

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
