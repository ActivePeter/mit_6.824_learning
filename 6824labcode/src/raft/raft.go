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
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

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
type PeerType int

const (
	PeerTypeFollower PeerType = iota
	PeerTypeCandidate
	PeerTypeLeader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	rd        sync.Mutex          // Lock to protect shared access to this peer's state
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//per持久化
	perCurrentTerm   int // 当前已知任期号
	perVotedFor      int // 当前任期给谁了
	peertype         PeerType
	cancel_candidate bool
}

const (
	//单位ms
	electTimeoutMax = 300
	electTimeoutMin = 200
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.perCurrentTerm
	isleader = rf.peertype == PeerTypeLeader
	rf.mu.Unlock()
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
	Term    int
	FromWho int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Ok bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//println(rf.me, "handle ", args.FromWho, "'s request vote")
	rf.mu.Lock()
	//rf.mu.Lock()
	// Your code here (2A, 2B).
	// 1.
	switch rf.peertype {
	case PeerTypeCandidate:
		if args.Term >= rf.perCurrentTerm {
			//fmt.Printf("%v transfer cand to %v\n", rf.me, args.FromWho)
			rf.perCurrentTerm = args.Term
			rf.peertype = PeerTypeFollower
			rf.perVotedFor = args.FromWho
			reply.Ok = true
		} else {
			reply.Ok = false
		}

		//非候选者，直接同意
	case PeerTypeFollower:
		if args.Term > rf.perCurrentTerm || rf.perVotedFor == -1 {
			reply.Ok = true
			rf.perCurrentTerm = args.Term
			rf.perVotedFor = args.FromWho
		} else {
			//fmt.Printf("follower dont agree %v %v %v\n", args.Term, rf.perCurrentTerm, rf.peertype)
			reply.Ok = false
		}
	case PeerTypeLeader:
		//if args.Term > rf.perCurrentTerm {
		//	println("leaderleave4", rf.me)
		//	rf.peertype = PeerTypeFollower
		//	reply.Ok = true
		//} else
		{

			reply.Ok = false
		}
		//return
	}

	rf.mu.Unlock()
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
	ok := rf.peers[server].Call("Raft.HandleRequestVote", args, reply)
	return ok
}

type RequestHeartBeatArgs struct {
	Term int
}

type RequestHeartBeatReply struct {
	Expire  bool
	Newterm int
}

func (rf *Raft) HandleRequestHeartBeat(args *RequestHeartBeatArgs, reply *RequestHeartBeatReply) {
	rf.mu.Lock()
	if args.Term >= rf.perCurrentTerm {
		rf.perCurrentTerm = args.Term
		if rf.peertype == PeerTypeCandidate {
			rf.cancel_candidate = true
		} else if rf.peertype == PeerTypeLeader {
			//println("leaderleave3", rf.me)
			rf.peertype = PeerTypeFollower
		}
	} else {
		reply.Expire = true
		reply.Newterm = rf.perCurrentTerm
	}
	rf.mu.Unlock()
}
func (rf *Raft) sendRequestHeartBeat(server int, args *RequestHeartBeatArgs, reply *RequestHeartBeatReply) bool {
	ok := rf.peers[server].Call("Raft.HandleRequestHeartBeat", args, reply)
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

func initRand() {
	rand.Seed(time.Now().UnixMilli())
}
func (rf *Raft) randElectTimeoutAndSleep() {
	timeout := rand.Intn(electTimeoutMax-electTimeoutMin) + electTimeoutMin
	time.Sleep(time.Duration(timeout) * time.Millisecond)
}

func (rf *Raft) becomeCandi() {
	rf.mu.Lock()
	if rf.peertype == PeerTypeLeader {
		rf.mu.Unlock()
		return
	}
	////如果已经vote了，就取消
	//if rf.perVotedFor != rf.me && rf.perVotedFor != -1 {
	//	//fmt.Printf("%v voted %v, cancel candidate\n", rf.me, rf.perVotedFor)
	//	rf.mu.Unlock()
	//	return
	//}
	//rf.perVotedFor = rf.me
	rf.perCurrentTerm++
	rf.perVotedFor = -1 //任期改变，votedfor必须跟着变
	//fmt.Printf("%v start candidate,term:%v\n", rf.me, rf.perCurrentTerm)
	rf.peertype = PeerTypeCandidate

	//getvote := 1
	//var votes []int

	rf.mu.Unlock()
	type Res struct {
		rpl *RequestVoteReply
		who int
	}
	res := make(chan Res)
	for key := range rf.peers {

		rf.mu.Lock()
		term := rf.perCurrentTerm
		me := rf.me
		rf.mu.Unlock()
		if key == me {
			continue
		}
		taskkey := key

		send := func() {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(taskkey, &RequestVoteArgs{
				Term:    term,
				FromWho: me,
			}, reply) {
				//出错
				//panic("sendRequestVote fail?")
				reply.Ok = false
			}
			res <- Res{
				rpl: reply,
				who: taskkey,
			}
		}
		go send()
		//rf.mu.Unlock()
	}
	peercnt := len(rf.peers)
	okcnt := 1
	//fmt.Printf("recv ")
	for i := 0; i < peercnt-1; i++ {
		take := <-res
		if take.rpl.Ok {
			//fmt.Printf("%v,", take.who)
			okcnt++
			if (peercnt%2 == 0 && okcnt >= peercnt/2) ||
				(peercnt%2 == 1 && okcnt > peercnt/2) {
				break
			}
		}
	}
	//println(okcnt)
	rf.mu.Lock()
	{
		if rf.peertype == PeerTypeFollower {
			//转让？
			rf.mu.Unlock()
			return
		}
		//println("cadisucc", rf.me)
		//println("finish", okcnt)
		if peercnt%2 == 0 {
			if okcnt >= peercnt/2 {

				//println(rf.me, "becomeleader2")
				rf.peertype = PeerTypeLeader

				rf.mu.Unlock()
				return
			}
		} else {
			if okcnt > peercnt/2 {
				//println(rf.me, "becomeleader")
				rf.peertype = PeerTypeLeader

				rf.mu.Unlock()
				return
			}
		}
	}

	//rf.mu.Lock()
	rf.peertype = PeerTypeFollower
	rf.mu.Unlock()
}
func (rf *Raft) holdLeader() {
	reply := &RequestHeartBeatReply{
		Expire: false,
	}
	if rf.peertype == PeerTypeLeader {
		//println("holdleader:", rf.me, "term:", rf.perCurrentTerm)
	}
	for rf.peertype == PeerTypeLeader {
		failcnt := 0
		for key, _ := range rf.peers {
			if key == rf.me {
				continue
			}
			term := rf.perCurrentTerm
			key1 := key
			call := func() {
				res := rf.sendRequestHeartBeat(key1, &RequestHeartBeatArgs{
					Term: term,
				}, reply)
				rf.mu.Lock()
				type_ := rf.peertype
				rf.mu.Unlock()
				if type_ == PeerTypeLeader {
					if !res {
						failcnt++
						//if rf.peertype==PeerTypeLeader{
						if failcnt == len(rf.peers)-1 {
							//println("leaderleave2", rf.me)
							rf.mu.Lock()
							rf.peertype = PeerTypeFollower
							rf.mu.Unlock()
						}
						//}
					} else if reply.Expire {
						//println("leaderleave1", rf.me)
						rf.mu.Lock()
						rf.peertype = PeerTypeFollower
						rf.perCurrentTerm = reply.Newterm
						rf.mu.Unlock()
						return
					}
				}
			}
			go call()

			//if failcnt > 0 {
			//	println("holdleader failcnt > 0", rf.me, "term:", rf.perCurrentTerm, failcnt)
			//}
			//if failcnt == len(rf.peers)-1 {
			//	println("leaderleave2", rf.me)
			//	rf.peertype = PeerTypeFollower
			//	return
			//}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	initRand()
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//1.随机选取超时
		rf.randElectTimeoutAndSleep()
		//2.0 如果有心跳，那么就取消竞选
		rf.mu.Lock()
		if rf.cancel_candidate {
			rf.cancel_candidate = false
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		//2.成为竞选者
		rf.becomeCandi() //有vote就代表最近有收到heartbeat
		rf.holdLeader()  //如果成了leader就在里面循环
	}
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
	rf.perCurrentTerm = 0
	rf.perVotedFor = -1
	rf.peertype = PeerTypeFollower
	rf.cancel_candidate = false

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
