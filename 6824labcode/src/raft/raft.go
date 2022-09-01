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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//per持久化
	perCurrentTerm int // 当前已知任期号
	perVotedFor    int // 当前任期给谁了
	perEntries     []Entry

	//非持久化
	peertype               PeerType
	cancel_start_candidate bool
	commitIndex            int //已经commit的最大log索引
	lastApplied            int //最近一次已经被持久化了
	//leader
	leader_nextIndex  []int //每个服务器下一个需要接收到的logentry
	leader_matchIndex []int //每个服务器已经持久化的最高索引
	applyCh           chan ApplyMsg
	agreementStates   *AgreementStates

	lastheartbeatMillis atomic.Value
}
type AgreementStates struct {
	notAppendedEntries []Entry
	appendChan         []EntryQueue //用于向各个客户端广播新的append entry
	//lackNotifyCond            []*sync.Cond
	curAppenddingEntry        Entry
	curAppenddingEntrySuccCnt int
	lackSyncing               []bool
}

func (ag *AgreementStates) end() {
	//ag.appendWaitCond.Signal()
}
func AgreementStates_make(peercnt int) *AgreementStates {
	ret := &AgreementStates{
		//lackSyncing: false,
	}
	ret.notAppendedEntries = make([]Entry, 0)
	ret.appendChan = make([]EntryQueue, peercnt)
	for i := 0; i < peercnt; i++ {
		ret.appendChan[i] = EntryQueue{}
	}
	ret.lackSyncing = make([]bool, peercnt)
	for i := 0; i < peercnt; i++ {
		ret.lackSyncing[i] = false
	}

	//ret.lackNotifyCond = make([]*sync.Cond, peercnt)
	//for i := 0; i < peercnt; i++ {
	//	ret.lackNotifyCond[i] = sync.NewCond(&sync.Mutex{})
	//}
	return ret
}

const (
	//单位ms
	electTimeoutMax = 350
	electTimeoutMin = 250
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

func (rf *Raft) nolock_getPrevEntry() (int, Entry) {
	index := len(rf.perEntries) - 1
	if index == -1 {
		return index, Entry{Term: -1}
	} else {
		return index, rf.perEntries[index]
	}
}
func (rf *Raft) nolock_getPrevOfIndexEntry(index1 int) (int, Entry) {
	index := index1 - 1
	if index <= -1 {
		return index, Entry{Term: -1}
	} else {
		return index, rf.perEntries[index]
	}
}

func (rf *Raft) nolock_stateTrans_leader2follwer(term int) {
	rf.peertype = PeerTypeFollower
	rf.agreementStates.end()
	states := rf.agreementStates
	rf.agreementStates = nil

	if states != nil {
		//println("nolock_stateTrans_leader2follwer", rf.me)
		//for _, v := range states.commitChan {
		//	close(*v)
		//}
		//for _, v := range states.appendChan {
		//	close(*v)
		//}
	}
	rf.perCurrentTerm = term
	rf.alignEntries2CommitIndex()
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
	Term         int
	FromWho      int
	LastLogIndex int //若没有log，则为-1
	LastLogTerm  int //若没有log,则为-1
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Ok bool
}

func IsAPrevEntryNewer(AIndex int, ATerm int, BIndex int, BTerm int) bool {
	if ATerm > BTerm {
		return true
	}
	if ATerm == BTerm && AIndex > BIndex {
		return true
	}
	return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//println(rf.me, "handle ", args.FromWho, "'s request vote")
	rf.mu.Lock()
	previ, preventry := rf.nolock_getPrevOfIndexEntry(rf.commitIndex + 1)

	//如果当前的log臂请求的log新，那么不给他投票
	if IsAPrevEntryNewer(previ, preventry.Term, args.LastLogIndex, args.LastLogTerm) {
		reply.Ok = false
		if args.Term >= rf.perCurrentTerm {
			//println("HandleRequestVote", "update term", args.Term)
			rf.perCurrentTerm = args.Term
		}
		rf.mu.Unlock()
		return
	}
	//rf.mu.Lock()
	// Your code here (2A, 2B).
	// 1.
	switch rf.peertype {
	case PeerTypeCandidate:
		if args.Term >= rf.perCurrentTerm {
			//fmt.Printf("%v transfer cand to %v\n", rf.me, args.FromWho)
			//println("_1")
			rf.nolock_stateTrans_leader2follwer(args.Term)
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

type Entry struct {
	Cmd   interface{}
	Term  int
	Index int
}

func (e *Entry) print() {
	switch t := e.Cmd.(type) {
	case int:
		fmt.Printf("Entry) print "+"cmd with value %v\n", t)
	case string:
		fmt.Printf("Entry) print " + "cmd with String\n")
	case nil:
		fmt.Printf("Entry) print " + "nil value: nothing to check?\n")
	default:
		fmt.Printf("Entry) print "+"Unexpected type %T\n", t)
	}

}

//type CommitEntryArgs struct {
//	Entryidx int
//}
//type CommitEntryReply struct{}

//func (rf *Raft) HandleCommitEntry(args *CommitEntryArgs, reply *CommitEntryReply) {
//	//{ //心跳
//	//	rf.cancel_start_candidate = true
//	//}
//	rf.mu.Lock()
//	rf.nolock_commitEntry(args.Entryidx)
//	rf.mu.Unlock()
//}
//func (rf *Raft) sendCommitEntry(server int, args *CommitEntryArgs, reply *CommitEntryReply) bool {
//	ok := rf.peers[server].Call("Raft.HandleCommitEntry", args, reply)
//	return ok
//}

func (rf *Raft) nolock_commitEntry(entryidx int) {
	//println(rf.me, "commit set", entryidx)
	//rf.perEntries[entryidx].print()
	for i := rf.commitIndex + 1; i <= entryidx; i++ {
		entryidx := i

		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.perEntries[entryidx].Cmd,
			CommandIndex: entryidx + 1,
		}
		rf.applyCh <- msg

		fmt.Printf("%v commit at %v with %v\n", rf.me, entryidx, msg)
		//if(rf.peertype==PeerTypeLeader){
		//	rf.mu.Lock()
	}
	rf.commitIndex = entryidx //变更到最新的commit index
	//rf.mu.Unlock()
	//}
}

type AppendEntryRequest struct {
	Term     int // 领导者任期
	LeaderId int // 领导者ID 因此跟随者可以对客户端进行重定向
	// （译者注：跟随者根据领导者id把客户端的 请求重定向到领导者，比如有时客户端把请求发给了跟随者而不是领导者）
	PrevLogIndex int     //紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int     //紧邻新日志条目之前的那个日志条目的任期
	Entries      []Entry //记录的日志，
	LeaderCommit int     // 领导者的已知已提交的最高的日志条目的索引
}
type AppendEntryReply struct {
	Term    int // 领导者任期
	Success bool
}

func (rf *Raft) nolock_appendEntries(entries []Entry) {
	if len(entries) > 0 {
		rf.perEntries = append(rf.perEntries, entries...)
	}
}

func UtilMin(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
func (rf *Raft) HandleAppendEntryRequest(args *AppendEntryRequest, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) == 0 {
		//println("HandleAppendEntryRequest heart beat", rf.me)
	}
	reply.Term = rf.perCurrentTerm

	//1. 返回假 如果领导者的任期 小于 接收者的当前任期（译者注：这里的接收者是指跟随者或者候选者） （5.1 节）
	if rf.perCurrentTerm > args.Term {

		//if rf.peertype == PeerTypeLeader {
		//	rf.nolock_stateTrans_leader2follwer(rf.perCurrentTerm)
		//}
		//println("HandleAppendEntryRequest", "leader term not ok,cur-leader", rf.perCurrentTerm, args.Term)
		reply.Success = false
		return
	}
	//if rf.perCurrentTerm <= args.Term {

	{ //rf.perCurrentTerm <= args.Term
		rf.perCurrentTerm = args.Term
		if rf.peertype == PeerTypeFollower {
			rf.cancel_start_candidate = true
		} else if rf.peertype == PeerTypeCandidate {
			rf.peertype = PeerTypeFollower
		} else if rf.peertype == PeerTypeLeader {
			rf.nolock_stateTrans_leader2follwer(args.Term)
		}
	}
	//}
	//这下面的term都<=leader term

	//2. 返回假 如果接收者日志中没有包含这样一个条目 即该条目的任期在prevLogIndex上能和prevLogTerm匹 配上
	// （译者注：在接收者日志中 如果能找到一个和prevLogIndex以及prevLogTerm一样的索引和任期的 日志条目 则继续执行下面的步骤 否则返回假）（5.3 节）
	prevEntryOk := false
	if len(rf.perEntries) > args.PrevLogIndex {
		if args.PrevLogIndex > -1 { //存在上一个entry，检验term一致性
			if rf.perEntries[args.PrevLogIndex].Term == args.PrevLogTerm {
				prevEntryOk = true
			} else { //
				//println("HandleAppendEntryRequest", "prev index", args.PrevLogIndex, "term", rf.perEntries[args.PrevLogIndex].Term, args.PrevLogTerm)
				//3. 如果一个已经存在的条目和新条目（译者注：即刚刚接收到的日志条目）发生了冲突（因为索引相同， 任期不同），那么就删除这个已经存在的条目以及它之后的所有条目 （5.3 节）
				rf.perEntries = rf.perEntries[:args.PrevLogIndex]
			}
		} else {
			prevEntryOk = true
		}
	} else {
		//fmt.Printf("HandleAppendEntryRequest prev not exist entrycnt:%v argindex:%v\n", len(rf.perEntries), args.PrevLogIndex)
	}
	if !prevEntryOk {
		//println("HandleAppendEntryRequest", "prev entry not ok")
		reply.Success = false
		return
	}
	if len(rf.perEntries) > args.PrevLogIndex+1 {

		rf.perEntries = rf.perEntries[:args.PrevLogIndex+1]
	}

	reply.Success = true
	//4. 追加日志中尚未存在的任何新条目
	rf.nolock_appendEntries(args.Entries)
	//5. 如果领导者的已知已经提交的最高的日志条目的索引leaderCommit 大于 接收者的已知已经提交的最高 的日志条目的索引commitIndex
	// 则把 接收者的已知已经提交的最高的日志条目的索引commitIndex 重置 为 领导者的已知已经提交的最高的日志条目的索引leaderCommit 或者是 上一个新条目的索引 取两者的 最小值
	if args.LeaderCommit > rf.commitIndex {
		if len(args.Entries) == 0 {
			println("heart beat handle by", rf.me, ",leadercommit", args.LeaderCommit)
		}
		rf.nolock_commitEntry(UtilMin(args.LeaderCommit, args.PrevLogIndex+1))
		//rf.commitIndex =
	}
}
func (rf *Raft) sendAppendEntryRequest(server int, args *AppendEntryRequest, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.HandleAppendEntryRequest", args, reply)
	return ok
}

type RequestHeartBeatArgs struct {
	Term int //结果为真 如果跟随者所含有的条目和prevLogIndex以及prevLogTerm匹配上了

	PrevLogIndex int
	PrevLogTerm  int
}

type RequestHeartBeatReply struct {
	Expire  bool
	Newterm int

	NeedLogIndex int //需要补充 append
}

func (rf *Raft) peerTypeName() string {
	switch rf.peertype {
	case PeerTypeCandidate:
		return "PeerTypeCandidate"
	case PeerTypeFollower:
		return "PeerTypeFollower"
	case PeerTypeLeader:
		return "PeerTypeLeader"
	}
	return ""
}

func (rf *Raft) HandleRequestHeartBeat(args *AppendEntryRequest, reply *AppendEntryReply) {
	rf.HandleAppendEntryRequest(args, reply)
	//rf.mu.Lock()
	//if args.Term >= rf.perCurrentTerm {
	//	rf.perCurrentTerm = args.Term
	//
	//	//println("_cancel_start_candidate_2", rf.me)
	//	rf.cancel_start_candidate = true
	//	if rf.peertype == PeerTypeLeader {
	//		println("_2")
	//		rf.nolock_stateTrans_leader2follwer(args.Term)
	//		//println("leaderleave3", rf.me)
	//		//rf.peertype = PeerTypeFollower
	//	}
	//
	//	index, entry := rf.nolock_getPrevEntry()
	//	if IsAPrevEntryNewer(args.PrevLogIndex, args.PrevLogTerm, index, entry.Term) {
	//		println("HandleRequestHeartBeat", "NeedLogIndex,arg:", args.PrevLogIndex, args.PrevLogTerm, " me:", index, entry.Term)
	//		//请求值更新，说明需要补给
	//		reply.NeedLogIndex = args.PrevLogIndex
	//	}
	//} else {
	//	reply.Expire = true
	//	reply.Newterm = rf.perCurrentTerm
	//	println(rf.peerTypeName(), rf.me, "find leader expired, me-leader", rf.perCurrentTerm, args.Term)
	//}
	//rf.mu.Unlock()
}
func (rf *Raft) sendRequestHeartBeat(server int, args *AppendEntryRequest, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.HandleRequestHeartBeat", args, reply)
	return ok
}

//判断数量是否达到一半
func (rf *Raft) matchMost(okcnt int) bool {
	peercnt := len(rf.peers)
	return (peercnt%2 == 0 && okcnt >= peercnt/2) ||
		(peercnt%2 == 1 && okcnt > peercnt/2)
}

//判断数量是否刚好达到一半
func (rf *Raft) matchMostEdge(okcnt int) bool {
	peercnt := len(rf.peers)
	return (peercnt%2 == 0 && okcnt == peercnt/2) ||
		(peercnt%2 == 1 && okcnt == peercnt/2+1)
}

//判断
func (rf *Raft) leaderHandleOneFollowerEntryAppend(entry Entry) {
	//直到这个append被多数client采纳，
	rf.mu.Lock()
	if len(rf.agreementStates.notAppendedEntries) > 0 {
		if entry == rf.agreementStates.notAppendedEntries[0] {
			rf.agreementStates.curAppenddingEntrySuccCnt++
			//保证只在刚到临界点时调用一次append
			if rf.matchMostEdge(rf.agreementStates.curAppenddingEntrySuccCnt + 1) {
				//println("leaderHandleOneFollowerEntryAppend cur cnt", rf.agreementStates.curAppenddingEntrySuccCnt)
				rf.agreementStates.notAppendedEntries = rf.agreementStates.notAppendedEntries[1:]
				rf.agreementStates.curAppenddingEntrySuccCnt = 0
				rf.nolock_commitEntry(entry.Index)

				rf.sendImmediateHeartBeat2()
				//for key := range rf.peers {
				//	if key == rf.me {
				//		continue //跳过自己
				//	}
				//}
				//entries := make([]Entry, 1)
				//entries[0] = entry
				//rf.nolock_appendEntries(entries) //确认append了
			} else {
				//println("_leaderHandleOneFollowerEntryAppend cur cnt", rf.agreementStates.curAppenddingEntrySuccCnt)
			}
		} else {

			//println(" entry != rf.agreementStates.curAppenddingEntry ")
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) StartAgreementCoroutineIfNot() {
	rf.mu.Lock()
	if rf.agreementStates == nil {
		//rf.mu.Unlock()
		rf.agreementStates = AgreementStates_make(len(rf.peers))

		//println("AgreementStates_made")
		//task := func()
		{
			//遍历跟随者创建协程监听通道任务
			for key := range rf.peers {
				if key == rf.me {
					continue //跳过自己
				}
				key := key
				call := func() {
					for { //一个循环一个entry
						if rf.peertype != PeerTypeLeader {
							//终止协程
							break
						}
						//println("follower send coroutine", key, " waiting for new entry or lackcheck")
						rf.mu.Lock()
						if rf.agreementStates == nil {
							return
						}
						e := rf.agreementStates.appendChan[key].Pop()
						rf.mu.Unlock()
						//select
						{
							if e != nil { //新的entry
								entry := *e
								if rf.peertype != PeerTypeLeader {
									//终止协程
									return
								}
								//fmt.Printf("follower send coroutine %v got one entry to send %v\n", key, entry)
								pindex, pentry := rf.nolock_getPrevOfIndexEntry(entry.Index) //相对于当前这个append之前的一次entry
								////循环append一个entry直到成功
								backwardTryStack := make([]int, 0) //对应
								failtime := 0
								for {
									if rf.agreementStates == nil {
										//终止协程
										return
									}
									if rf.perCurrentTerm > entry.Term {
										return
									}
									reply := AppendEntryReply{}
									//rf.mu.Lock()
									entries := make([]Entry, 1)
									entries[0] = entry
									//rf.mu.Unlock()
									req := AppendEntryRequest{
										Term:         rf.perCurrentTerm,
										LeaderId:     rf.me,
										PrevLogIndex: pindex,
										PrevLogTerm:  pentry.Term,
										Entries:      entries,
										LeaderCommit: rf.commitIndex,
									}
									if len(backwardTryStack) > 0 {
										//println("\nreq with try back data")
										index_ := backwardTryStack[len(backwardTryStack)-1]
										entry_ := rf.perEntries[index_]
										pindex, pentry := rf.nolock_getPrevOfIndexEntry(index_)
										//req.Term = entry_.Term
										req.PrevLogTerm = pentry.Term
										req.PrevLogIndex = pindex
										req.Entries[0] = entry_
									} else {
										//println("\nreq with new entry")
									}

									//fmt.Printf("follower send coroutine %v start sendAppendEntryRequest\n"+
									//	"       -> %+v\n", key, req)

									//fmt.Printf("       -> %+v\n", req)
									ok := rf.sendAppendEntryRequest(key, &req, &reply)
									if ok {
										//println()
										//fmt.Printf(
										//	"follower send coroutine %v --------------------------------------- finish sendAppendEntryRequest"+
										//		"\n            -> %+v\n", key, reply)
										rf.mu.Lock()
										if reply.Term > rf.perCurrentTerm {
											//当前term是旧的，不再是leader
											//close(rf.appendChan)

											//println("_3")
											rf.nolock_stateTrans_leader2follwer(reply.Term)
											rf.mu.Unlock()
											break
										}
										rf.mu.Unlock()
										if reply.Success {
											if len(backwardTryStack) > 0 {
												//println("try back succ")
												backwardTryStack = backwardTryStack[:len(backwardTryStack)-1]
											} else {
												//直到成功发送后跳出循环,发送下一个entry
												//commit，并且通知其他的都commit,
												rf.leaderHandleOneFollowerEntryAppend(entry)
												break
											}
										} else {
											//失败了，5.3对nextIndex描述，需要往回试到可以
											stacklen := len(backwardTryStack)
											if stacklen == 0 {
												if pindex > -1 {
													//println("fail and try back 1", pindex)
													//println()
													backwardTryStack = append(backwardTryStack, pindex)
												}
											} else {
												lastIndex := backwardTryStack[stacklen-1]
												if lastIndex-1 > -1 {
													//println("fail and try back 2", lastIndex-1)
													//println()
													backwardTryStack = append(backwardTryStack, lastIndex-1)
												}
											}
										}
									} else {
										failtime++
										//fmt.Printf(
										//	"follower send coroutine --------------------------------------- send fail timeout %v\n", failtime)
									}
									time.Sleep(1 * time.Millisecond)
								}
								if rf.peertype == PeerTypeLeader {
									if rf.agreementStates.lackSyncing[key] {
										//println("end lackSync")
										rf.agreementStates.lackSyncing[key] = false
									}
								}
							}

							//case rf.agreementStates.lackNotifyCond[key].Wait():

						}
						time.Sleep(1 * time.Millisecond)
					}
				}
				go call()

			}
		}
		//go task()

	}

	rf.mu.Unlock()
}

func (rf *Raft) StartAgreement(entry Entry) {
	rf.StartAgreementCoroutineIfNot()

	fmt.Printf("new entry in :%v\n", entry)
	broadcast := func() {
		rf.mu.Lock()
		if rf.agreementStates == nil {
			//println("broadcast fail,rf.agreementStates == nil")
			rf.mu.Unlock()
			return
		}
		rf.agreementStates.notAppendedEntries = append(rf.agreementStates.notAppendedEntries, entry)
		rf.mu.Unlock()
		//println("new entry in")
		//entry.print()
		//println("broadcast")
		//向所有follwer广播传递新的要append的entry
		for key := range rf.peers {
			if key == rf.me {
				continue //跳过自己
			}
			//println("start send entry to follower co ", key)
			key := key
			//send := func() {
			rf.mu.Lock()
			if rf.agreementStates == nil {
				rf.mu.Unlock()
				return
			}
			rf.agreementStates.appendChan[key].Push(entry)
			rf.mu.Unlock()
			//println("sent entry to client ", key)
			//}
			//go send()
		}
	}
	//lazy方式启动同步协程

	go broadcast()
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
// the first return value is the index that the command will appear at if it's ever committed.
// the second return value is the current term.
//  the third return value is true if this server believes it is the leader.

func (rf *Raft) Start(command interface{}) (int, int, bool) {

	isLeader := rf.peertype == PeerTypeLeader
	index := -1
	term := -1
	// Your code here (2B).
	if isLeader {
		rf.mu.Lock()
		index = rf.commitIndex + 2 //+1为当前标准索引，再加1为下一个log索引
		term = rf.perCurrentTerm
		//rf.mu.Lock()
		//开始agreement
		pi, _ := rf.nolock_getPrevEntry()
		entry := Entry{
			Cmd:   command,
			Term:  rf.perCurrentTerm,
			Index: pi + 1,
		}
		//rf.mu.Unlock()
		index = entry.Index + 1
		entries := make([]Entry, 1)
		entries[0] = entry
		rf.nolock_appendEntries(entries)
		rf.mu.Unlock()

		rf.StartAgreement(entry)
	} else {
		//rf.mu.Unlock()
	}

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

//func (rf *Raft) candiCancel(term int) {
//	rf.cancel_start_candidate = true;
//
//}

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
	res := make(chan Res, len(rf.peers))
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
			rf.mu.Lock()
			previndex, prevEntry := rf.nolock_getPrevOfIndexEntry(rf.commitIndex + 1)
			rf.mu.Unlock()
			if !rf.sendRequestVote(taskkey, &RequestVoteArgs{
				Term:         term,
				FromWho:      me,
				LastLogIndex: previndex,
				LastLogTerm:  prevEntry.Term,
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
		if rf.peertype == PeerTypeFollower {
			return
		}
		if take.rpl.Ok {
			//fmt.Printf("%v,", take.who)
			okcnt++
			//println("candidator", rf.me, "has vote", okcnt)
			if (peercnt%2 == 0 && okcnt >= peercnt/2) ||
				(peercnt%2 == 1 && okcnt > peercnt/2) {

				//println("candidator", rf.me, "has enough vote", okcnt)
				rf.peertype = PeerTypeLeader
				return
				//rf.mu.Unlock()
			}
		}
	}
	//println(okcnt)
	rf.mu.Lock()

	//println("candidator", rf.me, "back to follower")
	//{
	//	if rf.peertype == PeerTypeFollower {
	//		//转让？
	//		rf.mu.Unlock()
	//		return
	//	}
	//	//println("cadisucc", rf.me)
	//	//println("finish", okcnt)
	//	if peercnt%2 == 0 {
	//		if okcnt > peercnt/2 {
	//
	//			//println(rf.me, "becomeleader2")
	//
	//			return
	//		}
	//	} else {
	//		if okcnt > peercnt/2 {
	//			//println(rf.me, "becomeleader")
	//			rf.peertype = PeerTypeLeader
	//
	//			rf.mu.Unlock()
	//			return
	//		}
	//	}
	//}

	//rf.mu.Lock()
	rf.peertype = PeerTypeFollower
	rf.mu.Unlock()
}

func (rf *Raft) alignEntries2CommitIndex() {
	if rf.commitIndex+1 < len(rf.perEntries) {
		rf.perEntries = rf.perEntries[:rf.commitIndex+1]
	}
}

func (rf *Raft) sendImmediateHeartBeat2() {
	rf.lastheartbeatMillis.Store(rf.lastheartbeatMillis.Load().(int64) - 70)
}

//给其他服务器发心跳
func (rf *Raft) holdLeader() {
	rf.mu.Lock()
	if rf.peertype == PeerTypeLeader {
		println("holdleader:", rf.me, "term:", rf.perCurrentTerm)
		rf.alignEntries2CommitIndex()
	}
	rf.mu.Unlock()
	for rf.peertype == PeerTypeLeader {

		now := time.Now().UnixMilli()

		last := rf.lastheartbeatMillis.Load().(int64)
		if now-last > 100 {
			rf.lastheartbeatMillis.Store(now)
			failcnt := 0
			//println("leader heartbeat", rf.me, "term:", rf.perCurrentTerm)
			for key, _ := range rf.peers {
				if key == rf.me {
					continue
				}
				term := rf.perCurrentTerm
				key1 := key
				call := func() {
					pi, pe := rf.nolock_getPrevEntry()
					reply := &AppendEntryReply{}
					//println("leader heart beat",rf.me,"to",key1)
					res := rf.sendRequestHeartBeat(key1, &AppendEntryRequest{
						Term:         term,
						LeaderId:     rf.me,
						PrevLogIndex: pi,
						PrevLogTerm:  pe.Term,
						LeaderCommit: rf.commitIndex,
					}, reply)
					//rf.mu.Lock()
					type_ := rf.peertype
					if type_ == PeerTypeLeader {
						if !res {
							failcnt++
							//if rf.peertype==PeerTypeLeader{
							if failcnt == len(rf.peers)-1 {
								//println("leaderleave2", rf.me)
								//println("_4")
								rf.mu.Lock()
								rf.nolock_stateTrans_leader2follwer(rf.perCurrentTerm)
								rf.mu.Unlock()
							}
							//}
						} else {
							//fmt.Printf("                  sendRequestHeartBeat reply %+v ,cur term:%v, me:%v\n", reply, rf.perCurrentTerm, rf.me)
							if reply.Term > rf.perCurrentTerm {
								//println("leader term old and leave", rf.me)
								rf.mu.Lock()
								//println("_5")
								rf.nolock_stateTrans_leader2follwer(reply.Term)
								rf.mu.Unlock()
								return
							} else if !reply.Success {

								////确保append entry发送协程启动
								//rf.StartAgreementCoroutineIfNot()
								////补充发送缺失的append entry
								//rf.mu.Lock()
								//if !rf.agreementStates.lackSyncing[key1] {
								//	rf.agreementStates.lackSyncing[key1] = true
								//	println("start lackSync", key1, reply.NeedLogIndex)
								//	*rf.agreementStates.appendChan[key1] <- rf.perEntries[reply.NeedLogIndex]
								//}
								//rf.mu.Unlock()
								////rf.agreementStates.lackNotifyCond[key1].Signal()
							}
						}
					}
					//rf.mu.Unlock()
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

		} else {

		}
		time.Sleep(1 * time.Millisecond)
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
		if rf.cancel_start_candidate {
			rf.cancel_start_candidate = false
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
	rf.cancel_start_candidate = false
	rf.applyCh = applyCh
	rf.commitIndex = -1
	rf.lastheartbeatMillis.Store(time.Now().UnixMilli())

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
