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
	"fmt"
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

	//2a
	recentHeartBeatRecved bool
	term                  int
	state                 int
	thisTermVoted         bool

	//2b
	logs               []LogEntry
	commitIndex        int //已经commit的, 比如1，那么0已经被commit了
	matchIndexs        []int
	appendingEntry     []sync.Mutex
	appendingEntryTime []int64
	applyChan          chan ApplyMsg
}
type LogEntry struct {
	Term      int
	AppendCnt int
	Command   interface{}
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

	//todo 2b 选举限制
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

func LogASameAsLogB(ATerm int, AIndex int, BTerm int, BIndex int) bool {
	return ATerm == BTerm && AIndex == BIndex
}
func LogANewerThanLogB(ATerm int, AIndex int, BTerm int, BIndex int) bool {
	//Raft 通过比较两份日志中最后一条日志条目的索引值和任期号定义谁的日志比较新。
	// 如果两份日志最后的条目 的任期号不同，那么任期号大的日志更加新。
	// 如果两份日志最后的条目任期号相同，那么日志比较长的那个就 更加新。
	if ATerm != BTerm {
		return ATerm > BTerm
	}
	return AIndex > BIndex
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	log.Println(rf.me, "recv req vote from", args.CandidateId)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	decidevote := func() {
		thisLastLogTerm := 0
		thisLastLogIndex := -1
		if rf.commitIndex-1 > -1 {
			thisLastLogIndex = rf.commitIndex - 1
			thisLastLogTerm = rf.logs[rf.commitIndex-1].Term
		}
		if args.Term > rf.term &&
			!rf.thisTermVoted && //当前term还没有投过票（不过一般投过票就不在这个term了
			//自己比别人新>要拒绝 <=> 接收自不比别人新<=
			!LogANewerThanLogB(thisLastLogTerm, thisLastLogIndex, args.LastLogTerm, args.LastLogIndex) {
			rf.thisTermVoted = true
			reply.Voted = true
			rf.recentHeartBeatRecved = true
			log.Printf("follower %v vote %v\n", rf.me, args.CandidateId)
		} else {
			reply.Voted = false
			log.Printf("follower %v didnt vote %v ,term smaller %v,term voted %v\n",
				rf.me, args.CandidateId, args.Term > rf.term, rf.thisTermVoted)
		}
		if reply.Voted {
			//fix 投票后，不能让他立即成为candi，
			rf.recentHeartBeatRecved = true
		}
		if args.Term > rf.term {

			rf.term = args.Term
			rf.thisTermVoted = false
		}
	}
	switch rf.state {
	case StateLeader:
		if args.Term > rf.term {
			//reply.Voted = true
			//rf.thisTermVoted = true
			rf.state = StateFollower
			decidevote()
			rf.term = args.Term
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
		decidevote()
	case StateCandi:
		if args.Term > rf.term {
			rf.state = StateFollower
			decidevote()
			//rf.thisTermVoted = false
			//reply.Voted = true
			rf.term = args.Term
			//log.Printf("candi %v vote %v and become follower\n", rf.me, args.CandidateId)
		} else {
			//竞选者不投
			//log.Printf("candi %v didnt vote %v\n", rf.me, args.CandidateId)
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
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int //同步leader的commit
}

const (
	AppendEntryResult_NotFollower   = 0
	AppendEntryResult_LeaderTermOut = 1
	AppendEntryResult_PrevMatch     = 2
	AppendEntryResult_PrevNotMatch  = 3
)

type AppendEntryReply struct {
	Term              int
	AppendEntryResult int
	//follower发现缺失log时，告诉leader自己的index数，
	// 这样leader 大于这个index的pre都不用发了，必然不匹配
	// leader直接校准 match index到这个
	FollowerIndex int
}

func (rf *Raft) HandleAppendEntry(args *AppendEntryArg, reply *AppendEntryReply) {
	println(rf.me, "bf lock recv ae from", args.LeaderId)
	// dirty set flag
	rf.recentHeartBeatRecved = true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.recentHeartBeatRecved = true
	switch rf.state {
	case StateLeader:
		log.Println("leader", rf.me, "recv append from", args.LeaderId, "t:", args.Term, rf.term)
		if args.Term > rf.term {
			rf.term = args.Term
			rf.thisTermVoted = false
			rf.state = StateFollower
			rf.recentHeartBeatRecved = true
		}
		//{
		//	println("ae two leader conflict", rf.me, args.LeaderId, "at term", rf.term, args.Term)
		//}
		//log.Println("not handled append entry recv when leader")
	case StateFollower:
		log.Println("follower", rf.me, "recv append from", args.LeaderId)
		rf.recentHeartBeatRecved = true
		if args.Term > rf.term {
			rf.term = args.Term
			rf.thisTermVoted = false
		}
	case StateCandi:
		log.Println("candi", rf.me, "recv append from", args.LeaderId, "t", rf.term, args.Term)

		if args.Term >= rf.term {
			if args.Term > rf.term {
				rf.thisTermVoted = false
			}
			rf.term = args.Term
			rf.state = StateFollower
			rf.recentHeartBeatRecved = true
			log.Println("candi", rf.me, "back to follower")
		}
		//log.Println("not handled append entry recv when candi")
	}
	reply.Term = rf.term
	//如果arg term更小，说明这个leader无效 fix2b
	if rf.state != StateFollower {
		reply.AppendEntryResult = AppendEntryResult_NotFollower
	} else if rf.term != args.Term {
		reply.AppendEntryResult = AppendEntryResult_LeaderTermOut
	} else { //bugfix 2b 可能之前不是follower，变成了follower
		//确认之前的commit 对应
		// fix 2b ->
		// fix x2 这一块的prevlog比对即使没有entry也要做
		//  有可能follower有不同步的commit，然后又由于leader的commit index更大，导致
		//arg中的prevlog 为有效已commit范围
		if len(rf.logs) > args.PrevLogIndex+1 {
			//超出prevlog的都是无效的，跟leader不同步的
			// 裁掉
			rf.logs = rf.logs[:args.PrevLogIndex+1]
			println(rf.me,"sub logs to sync leader log by prevlog info")
		}
		logsFoundPrev := true
		if args.PrevLogIndex == -1 {
			//没有prevlog限制，直接append
		} else if len(rf.logs) == args.PrevLogIndex+1 {
			if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
				logsFoundPrev = false
			}
		} else {
			logsFoundPrev = false
		}
		//<-
		if logsFoundPrev {
			reply.AppendEntryResult = AppendEntryResult_PrevMatch
		} else {
			fmt.Printf("%v prev not match %+v %v\n", rf.me, args, rf.logs)
			reply.AppendEntryResult = AppendEntryResult_PrevNotMatch
			reply.FollowerIndex = rf.commitIndex - 1
		}
		if len(args.Entries) > 0 {

			if logsFoundPrev {
				//todo prev log 检查，
				//args.PrevLogIndex
				//if rf.logs[len(rf.logs)-1].Term
				//fmt.Printf("%v recv ae term:%v cmd:%v\n",
				//	rf.me, args.Entries[0].Term, args.Entries[0].Command)
				fmt.Printf("follower %v append cmd %+v at %v\n",
					rf.me, args.Entries[0], len(rf.logs)+1)
				rf.logs = append(rf.logs, args.Entries...)
				//reply.Success = true
			} else {
				fmt.Printf("%v recv ae prelog not match, self t: i:,"+
					"arg t:%v i:%v\n", rf.me, args.PrevLogTerm, args.PrevLogIndex)
				//reply.Success = false
			}
		}
		//没找到prev。意味着可能缺失，或者冲突，不能轻易commit
		// 比如 leader  0 1 new  prev 是1 leader commit index为1的下标
		//     this    0 2 没找到 1，但是2对应的正好为leader commit index,
		//                 2会被commit,但是2 不应该被commit
		if logsFoundPrev {
			for args.LeaderCommitIndex > rf.commitIndex {
				if rf.commitIndex >= len(rf.logs) {
					//当前leader 新 log还没有同步到该节点，还没有append
					break
				}
				fmt.Printf("follower %v commit cmd %v at %v\n",
					rf.me, rf.logs[rf.commitIndex].Command, rf.commitIndex+1)
				//maybe like this?
				rf.applyChan <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.commitIndex].Command,
					CommandIndex: rf.commitIndex + 1, //对外逻辑index
				}
				rf.commitIndex++
				println(rf.me, "commit index to", rf.commitIndex, "handle ae")
			}
		}
	}
	rf.recentHeartBeatRecved = true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != StateLeader {
		return len(rf.logs), rf.term, false
	}
	rf.logs = append(rf.logs, LogEntry{
		Term:      rf.term,
		AppendCnt: 1,
		Command:   command,
	})
	fmt.Printf("cmd come %v at %v\n", rf.logs[len(rf.logs)-1], len(rf.logs))
	//返回的index为程序逻辑中index+1,比如 0->1
	index := len(rf.logs)
	term := rf.term
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
	electTimeoutMax = 400
	electTimeoutMin = 300
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
	rf.mu.Lock()
	log.Println(rf.me, "become candi in term", rf.term+1)
	//1.自增任期号，转变角色
	rf.term++
	rf.thisTermVoted = false
	rf.state = StateCandi
	rf.mu.Unlock()
	timebegin := time.Now().UnixMilli()
	timeout := getRandElectTimeout() * 5

	argModel := RequestVoteArgs{}
	argModel.Term = rf.term
	argModel.CandidateId = rf.me
	//2b
	if len(rf.logs) > 0 {
		argModel.LastLogIndex = rf.commitIndex - 1
		if argModel.LastLogIndex > -1 {
			argModel.LastLogTerm = rf.logs[argModel.LastLogIndex].Term //最后一个log
		} else {
			argModel.LastLogTerm = 0 //最后一个log
		}
	} else { //还没有log
		argModel.LastLogIndex = -1
		argModel.LastLogTerm = 0
	}

	replyModel := RequestVoteReply{}
	votes := 1 //给自己投一票
	doneCnts := 1
	for key := range rf.peers {
		if key == rf.me {
			continue
		}
		keyclone := key
		task := func() {
			if rf.state == StateCandi {
				reply := replyModel
				log.Println(rf.me, "send req vote to", keyclone)
				ok := rf.sendRequestVote(keyclone, &argModel, &reply)
				//fmt.Printf("req vote ok ? %v\n", ok)
				if ok && reply.Voted {
					log.Println(rf.me, "get vote from", keyclone)
					votes++
				}
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
	if votes > len(rf.peers)/2 && rf.state == StateCandi {
		//达到半数，进入leader
		log.Println(rf.me, "become leader in term", rf.term)
		rf.state = StateLeader
	} else {
		rf.state = StateFollower
	}
	rf.mu.Unlock()
}

// lock rf before call it
func (rf *Raft) TryCommit() {
	//commit index 到 len(log)-1区间内的记录都是待commit的，
	//commit之前得确保计数达到半数，
	//此函数调用时，可能后面的entry先完成，前面的entry还没有到半数，此时会在前面的索引出即停止commit，
	//  直到后续前面的commit到达半数后，连同后面的一起提交
	for i := rf.commitIndex; i < len(rf.logs); i++ {
		if rf.logs[i].AppendCnt > len(rf.peers)/2 {
			fmt.Printf("leader %v commit %v at %v\n", rf.me, rf.logs[i], i+1)
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i + 1, //对外的逻辑index为程序 index+1
			}
			rf.commitIndex++
			println(rf.me, "commit index to", rf.commitIndex, "leader")
		} else {
			break
		}
	}
}
func (rf*Raft)stateStr() string{
	switch rf.state {
	case StateLeader:
		return "StateLeader"
	case StateCandi:
		return "StateCandi"
	}
	return "StateFollower"
}
func (rf *Raft) LeaderBroadcastHeartbeat() {
	//rf.mu.Lock()
	replymodel := AppendEntryReply{Term: 0}
	//rf.mu.Unlock()
	for key := range rf.peers {
		key:=key
		keyclone := key

		if key == rf.me {
			continue
		}
		if !rf.appendingEntry[key].TryLock() {
			fmt.Printf("%v to %v appending flag true,%v %v\n", rf.me,
				key, time.Now().UnixMilli()-rf.appendingEntryTime[key], rf.appendingEntryTime[key])
			continue
		} else {
			sendtag := rand.Int()
			println(sendtag,"lock",rf.me,rf.stateStr(),"to",key)

			rf.appendingEntryTime[key] = time.Now().UnixMilli()
			appendtime := rf.appendingEntryTime[key]
			println(key, "append start at", appendtime)
			timeoutMu := sync.Mutex{}
			isTimeout := false
			rpcFinish := false
			timeout := func() {
				keyc := key
				//println(keyc, "append timeout start sleep at", appendtime)
				time.Sleep(time.Duration(700) * time.Millisecond)
				//println(keyc, "=append timeout start sleep at", appendtime)
				timeoutMu.Lock()
				//println(keyc, "==append timeout start sleep at", appendtime)
				if !rpcFinish {
					//println(keyc, "append timeout done", appendtime)
					println(sendtag,"unlock",rf.me,"to",key)
					rf.appendingEntry[keyc].Unlock()
					isTimeout = true
					fmt.Printf("ae time out %v", key)
				} else {
					//println(keyc, "append not timeout done", appendtime)
				}
				timeoutMu.Unlock()
			}
			//println(key, "go append start at", appendtime)

			//println(key, "aftgo append start at", appendtime)
			rf.mu.Lock()
			if rf.state!=StateLeader{
				println(rf.me,"cancel append",key,"because not candi anymore")
				rf.mu.Unlock()
				continue
			}
			go timeout()
			argmodel := AppendEntryArg{
				Term:     rf.term,
				LeaderId: rf.me,
				//todo about commit info
				LeaderCommitIndex: rf.commitIndex,
			}
			//prev info   ||fix 2b
			//带上prev commit信息，消除follower缺失或冲突
			// 2b
			if rf.matchIndexs[key] > len(rf.logs){
				println(rf.me,"fatal match index > lenlogs",key,rf.matchIndexs[key] ,len(rf.logs),)
				//argmodel.PrevLogIndex = -1
			}
			argmodel.PrevLogTerm = 0
			argmodel.PrevLogIndex = -1
			if rf.matchIndexs[key] > 0 { //对应entry有前一个log
				argmodel.PrevLogIndex = rf.matchIndexs[key] - 1
				argmodel.PrevLogTerm = rf.logs[argmodel.PrevLogIndex].Term
			}

			if rf.matchIndexs[key] < len(rf.logs) {

				argmodel.Entries = append(argmodel.Entries, rf.logs[rf.matchIndexs[key]:]...)
				println("ae match index", rf.matchIndexs[key], key)
				fmt.Printf("%v %v on send rf state %v %v log %v %+v \n",
					sendtag, key, rf.commitIndex, rf.term, len(rf.logs), rf.logs)
			} else {
			}
			rf.mu.Unlock()
			reply := replymodel
			send := func() {
				if len(argmodel.Entries) > 0 {
					fmt.Printf("%v leader %v send ae %+v to %v\n", sendtag, rf.me, argmodel, keyclone)
				}
				ok := rf.sendAppendEntry(
					keyclone, &argmodel, &reply)

				timeoutMu.Lock()
				if isTimeout {
					// if rf.appendingEntry[keyclone].Load(){
					// 	println("-s-s-s-s-dadyusaihduiausihduaoi")
					// }
					// fmt.Printf("timeout leader %v send ae %+v to %v\n", rf.me, argmodel, keyclone)
					timeoutMu.Unlock()
					//rf.appendingEntry[keyclone].Store(false)
					return
				}
				if !ok {
					//fmt.Printf("sendfail leader %v send ae %+v to %v\n", rf.me, argmodel, keyclone)
				}
				rpcFinish = true
				timeoutMu.Unlock()
				//成功与否，都已经结束一次append了
				// leader to follower
				if ok { //后面的逻辑大前提是网络通的 fix 2b
					rf.mu.Lock()
					if rf.term!=argmodel.Term||rf.state!=StateLeader{
						//do nothing, expired append
					}else if reply.Term > rf.term {
						//term过期 早已不是leader
						if rf.state == StateLeader {
							rf.state = StateFollower
							rf.thisTermVoted = false
						}
					} else {
						//term 没过期，还需要完成leader该完成的事情
						if len(argmodel.Entries) > 0 {
							fmt.Printf("%v %v on recv rf state %v %v %+v \n",
								sendtag, keyclone, rf.commitIndex, rf.term, rf.logs)
							if reply.AppendEntryResult == AppendEntryResult_PrevMatch {
								if len(argmodel.Entries) > 0 {
									fmt.Printf("%v succ ae %+v to %v\n", sendtag, argmodel, keyclone)
								}
								for i := 0; i < len(argmodel.Entries); i++ {
									println("after ae match index", rf.matchIndexs[keyclone], keyclone, i)
									//append entry成功
									//对应index的log如果到达半数成功，需要commit
									rf.logs[rf.matchIndexs[keyclone]].AppendCnt++
									rf.matchIndexs[keyclone]++ //准备发送下一个log
								}
								println(rf.me,"to",keyclone,"match index upto",rf.matchIndexs[keyclone])
								rf.TryCommit()
							}
						}

						if reply.AppendEntryResult == AppendEntryResult_PrevNotMatch {
							if len(argmodel.Entries) > 0 {
								fmt.Printf("fail ae %+v to %v\n", argmodel, keyclone)
							}

							//之前的append缺失，（新leader上任时设置的index跟leader是一样的，
							// 					但是不保证所有的follower都已经完整的得到了logs
							if reply.FollowerIndex > -1 && reply.FollowerIndex < rf.matchIndexs[keyclone] {
								// follower index 最大为1，match index 为50，
								//  那么 1-50的matchindex都不会有效
								rf.matchIndexs[keyclone] = reply.FollowerIndex
							} else {
								rf.matchIndexs[keyclone]--
							}
							if rf.matchIndexs[keyclone] < 0 {
								fmt.Printf("=== fatal err match index minus to  <0\n")
							}
						}
					}
					rf.mu.Unlock()
				}
				println(sendtag,"unlock",rf.me,"to",keyclone)
				rf.appendingEntry[keyclone].Unlock()
				println(keyclone, "append rpc intime done", appendtime)
			}
			go send()
		}
	}
}

func (rf *Raft) ifLeaderThenHold() {
	//if rf.state != StateLeader {
	//	return
	//}
	//群发空append
	rf.mu.Lock()
	if rf.state == StateLeader {
		//当一个领导人刚获得权力的时候，他初始化所有的 nextIndex 值为自己的最后一条日志的 index 加 1
		//（图 7 中的 11）。如果一个跟随者的日志和领导人不一致，那么在下一次的附加日志
		// RPC 时的一致性检查就会失 败。在被跟随者拒绝之后，领导人就会减小 nextIndex 值并进行重试。
		//最终 nextIndex 会在某个位置使得领导 人和跟随者的日志达成一致。
		//当这种情况发生，附加日志 RPC 就会成功，这时就会把跟随者冲突的日志条目全 部
		//删除并且加上领导人的日志。一旦附加日志 RPC 成功，那么跟随者的日志就会和领导人保持一致，
		//并且在接 下来的任期里一直继续保持。
		if rf.commitIndex> len(rf.logs){
			println("fatal err wield, commit index > logs")
			rf.commitIndex=len(rf.logs)
		}
		for i, _ := range rf.matchIndexs {
			rf.matchIndexs[i] = rf.commitIndex
		}
		fmt.Printf("leader %v with old logs %v \n", rf.me, rf.logs)
		//not sure: should i do this?
		if rf.commitIndex+1 < len(rf.logs) {
			println(rf.me,"sub logs when init leader")
			rf.logs = rf.logs[:rf.commitIndex]
		}
		fmt.Printf("leader %v with committed logs %v \n", rf.me, rf.logs)
	}
	rf.mu.Unlock()
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

	// 2b
	rf.matchIndexs = make([]int, len(rf.peers))
	rf.commitIndex = 0
	rf.logs = make([]LogEntry, 0)
	rf.applyChan = applyCh
	rf.appendingEntry = make([]sync.Mutex, len(rf.peers))
	rf.appendingEntryTime = make([]int64, len(rf.peers))
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
