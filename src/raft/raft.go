package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

import "bytes"
import "../labgob"

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

type LogEntry struct {
	Command interface{}
	Term    int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role        Role
	currentTerm int
	votedFor    int
	logs        []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	applyCh chan<- ApplyMsg

	electionTimer  *time.Timer
	heartbeatTimer *time.Ticker
}

const (
	heartbeatInterval  = 100 * time.Millisecond
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 500 * time.Millisecond
)

type Role uint8

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

func (r Role) String() string {
	switch r {
	case RoleFollower:
		return "F"
	case RoleCandidate:
		return "C"
	case RoleLeader:
		return "L"
	default:
		return "unknown"
	}
}

func (rf *Raft) String() string {
	return fmt.Sprintf("[%d %v, term=%d]", rf.me, rf.role, rf.currentTerm)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == RoleLeader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		panic("read persist failed")
	}
	rf.mu.Lock()
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
	rf.mu.Unlock()
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.becomeFollower(args.Term)
		rf.persist()
	}

	lastLogIdx := len(rf.logs) - 1
	if args.LastLogTerm < rf.logs[lastLogIdx].Term ||
		args.LastLogTerm == rf.logs[lastLogIdx].Term && args.LastLogIndex < lastLogIdx {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persist()
	rf.resetElectionTimer()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)
	return ok
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
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.resetElectionTimer()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.persist()
	}

	if len(rf.logs)-1 < args.PrevLogIndex ||
		rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	unMatchIdx := -1
	for i := range args.Entries {
		absIdx := args.PrevLogIndex + i + 1
		if len(rf.logs)-1 < absIdx || rf.logs[absIdx].Term != args.Entries[i].Term {
			unMatchIdx = i
			break
		}
	}

	if unMatchIdx != -1 {
		rf.logs = rf.logs[:args.PrevLogIndex+1+unMatchIdx]
		rf.logs = append(rf.logs, args.Entries[unMatchIdx:]...)
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit <= len(rf.logs)-1 {
			rf.setCommitIndex(args.LeaderCommit)
		} else {
			rf.setCommitIndex(len(rf.logs) - 1)
		}
	}

	reply.Success = true
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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != RoleLeader {
		return -1, rf.currentTerm, false
	}
	index = len(rf.logs)
	rf.logs = append(rf.logs, LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	})
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	rf.persist()
	rf.broadcastHeartbeat()
	return index, rf.currentTerm, true
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = RoleFollower
	rf.electionTimer = time.NewTimer(getRandomElectionTimeoutDuration())
	rf.heartbeatTimer = time.NewTicker(heartbeatInterval)

	rf.applyCh = applyCh
	rf.logs = make([]LogEntry, 1)
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				rf.startElection()

			case <-rf.heartbeatTimer.C:
				rf.mu.Lock()
				if rf.role == RoleLeader {
					rf.broadcastHeartbeat()
				}
				rf.mu.Unlock()
			}
		}
	}()
	return rf
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.role = RoleCandidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	lastLogIdx := len(rf.logs) - 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIdx,
		LastLogTerm:  rf.logs[lastLogIdx].Term,
	}
	DPrintf("%s Start election", rf)
	rf.mu.Unlock()

	votesCh := make(chan bool, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(i, &args, &reply) {
				DPrintf("%s send RequestVote to %d success, reply %+v", rf, i, reply)
				votesCh <- reply.VoteGranted
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
				}
				rf.mu.Unlock()
			} else {
				votesCh <- false
				DPrintf("%s send RequestVote to %d failed", rf, i)
			}
		}(i)
	}
	total := len(rf.peers)
	threshold := total/2 + 1
	votedCount := 1
	finish := 1
	for {
		result := <-votesCh
		finish += 1
		if result {
			votedCount += 1
		}
		if finish == total || votedCount >= threshold || (finish-votedCount) > (total-threshold) {
			break
		}
	}

	rf.mu.Lock()
	if rf.role == RoleCandidate && votedCount >= threshold && rf.currentTerm == args.Term {
		rf.becomeLeader()
	}
	rf.mu.Unlock()
}

// call with lock
func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(getRandomElectionTimeoutDuration())
}

// call with lock
func (rf *Raft) becomeFollower(term int) {
	DPrintf("%s Become Follower", rf)
	rf.role = RoleFollower
	rf.currentTerm = term
	rf.resetElectionTimer()
	rf.heartbeatTimer.Stop()
	rf.votedFor = -1
}

// call with lock
func (rf *Raft) becomeLeader() {
	DPrintf("%s Become Leader", rf)
	rf.role = RoleLeader
	//for i := range rf.nextIndex {
	//	rf.nextIndex[i] = len(rf.logs)
	//	rf.matchIndex[i] = 0
	//}
	rf.electionTimer.Stop()
	rf.heartbeatTimer = time.NewTicker(heartbeatInterval)
	rf.broadcastHeartbeat()
}

// call with lock
func (rf *Raft) broadcastHeartbeat() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(dst int) {
			rf.mu.Lock()
			if rf.role != RoleLeader {
				rf.mu.Unlock()
				return
			}

			prevLogIndex := rf.nextIndex[dst] - 1

			entries := make([]LogEntry, len(rf.logs)-rf.nextIndex[dst])
			copy(entries, rf.logs[rf.nextIndex[dst]:])

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.logs[prevLogIndex].Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(dst, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if ok {
				DPrintf("%s Send heartbeat %+v to %d success, reply %+v", rf, args, dst, reply)
				if rf.role != RoleLeader {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					return
				}

				if reply.Success {
					rf.matchIndex[dst] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[dst] = rf.matchIndex[dst] + 1

					for i := len(rf.logs) - 1; i > rf.commitIndex; i-- {
						cnt := 0
						for _, matchIdx := range rf.matchIndex {
							if matchIdx >= i {
								cnt += 1
							}
						}
						if cnt > len(rf.peers)/2 && rf.logs[i].Term == rf.currentTerm {
							rf.setCommitIndex(i)
							break
						}
					}
				} else {
					rf.nextIndex[dst] -= 1
				}
			} else {
				DPrintf("%s Send heartbeat %+v to %d failed", rf, args, dst)
			}
		}(i)
	}
}

func (rf *Raft) setCommitIndex(index int) {
	rf.commitIndex = index
	if rf.commitIndex > rf.lastApplied {
		DPrintf("%s Apply from index %d to %d", rf, rf.lastApplied, rf.commitIndex)

		applyEntries := append([]LogEntry{}, rf.logs[rf.lastApplied+1:rf.commitIndex+1]...)
		go func(startIdx int, applyEntries []LogEntry) {
			for i, entry := range applyEntries {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: startIdx + i,
				}
				rf.applyCh <- msg
				rf.mu.Lock()
				if rf.lastApplied < msg.CommandIndex {
					rf.lastApplied = msg.CommandIndex
				}
				rf.mu.Unlock()
			}

		}(rf.lastApplied+1, applyEntries)
	}
}

func getRandomElectionTimeoutDuration() time.Duration {
	lo := electionTimeoutMin.Nanoseconds()
	hi := electionTimeoutMax.Nanoseconds()
	num := lo + rand.Int63n(hi-lo)
	return time.Duration(num)
}
