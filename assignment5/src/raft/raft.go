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
	"assign5/labrpc"
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const leader = "Leader"
const follower = "Follower"
const candidate = "Candidate"

var heartBeatTimeout = 50 * time.Millisecond
var rcvLogChan chan LogRequestReply
var rcvVoteChan chan VoteRequestReply

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Entry struct {
	Term    int
	Command interface{}
	Index   int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	sync.RWMutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	dead      int32

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// channel to send message to client
	messageChan chan ApplyMsg

	// persistent data
	currentTerm int
	votedFor    int
	log         []Entry

	// volatile data
	commitLength        int
	currentRole         string
	currentLeader       int
	votesReceived       map[int]int // key: voterId
	sentLength          []int       // key: nodeId, value: how many log entries have been sent to nodeId
	ackedLength         []int       // key: nodeId, value: how many log entries have been acknowledged as received by nodeId
	cancelElecTimerChan chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here.
	var term int
	var isLeader bool

	rf.RLock()
	if rf.currentRole == leader {
		term = rf.currentTerm
		isLeader = true
	} else {
		term = rf.currentTerm
		isLeader = false
	}
	rf.RUnlock()

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	rf.Lock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	rf.Unlock()
}

//
// example RequestVote RPC arguments structure.
//
type VoteRequestArgs struct {
	// Your data here.
	CId        int
	CTerm      int // C for candidate
	CLogLength int
	CLogTerm   int
}

//
// example RequestVote RPC reply structure.
//
type VoteRequestReply struct {
	// Your data here.
	VoterId int
	Term    int
	Granted bool
}

type LogRequestArgs struct {
	LeaderId     int
	Term         int
	PrefixLen    int
	PrefixTerm   int
	LeaderCommit int
	Suffix       []Entry
	Type         string
}

type LogRequestReply struct {
	FId     int // follower Id
	Fterm   int
	Ack     int // number of log received
	Success bool
	Type    string
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear as
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.Lock()

	if rf.currentRole == leader {
		savedTerm, savedCommitLen := rf.currentTerm, rf.commitLength

		savedCommitLen++

		message := Entry{
			Term:    savedTerm,
			Command: command,
			Index:   savedCommitLen,
		}
		rf.log = append(rf.log, message)
		rf.persist()
		log.Printf("MESSAGE: %v, log(after): %v\n", message, rf.log)

		rf.ackedLength[rf.me] = len(rf.log)
		rf.Unlock()

		for peerId := 0; peerId < len(rf.peers); peerId++ {
			if peerId != rf.me {
				go rf.ReplicateLog(peerId)
			}
		}

		return savedCommitLen, savedTerm, true
	} else {
		rf.Unlock()

		return rf.commitLength + 1, rf.currentTerm, false
	}

}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
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
	rf.messageChan = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitLength = 0

	rf.log = make([]Entry, 0)
	rf.currentRole = follower
	rf.currentLeader = -1
	rf.votesReceived = make(map[int]int)
	rf.sentLength = make([]int, len(peers)+1)
	rf.ackedLength = make([]int, len(peers)+1)
	rf.dead = 0

	// set up cancel election timer
	rf.cancelElecTimerChan = make(chan bool)

	go rf.StartElecTimer(getRandomTimeout())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) StartElecTimer(ElectionTimeout time.Duration) {

	for rf.killed() == false {
		select {
		case <-rf.cancelElecTimerChan: // reset election timer
			//log.Printf("elec timer #%d started %d\n", rf.me, ElectionTimeout)
			continue
		case <-time.After(ElectionTimeout): // start new election
			rf.Lock()
			rf.currentRole = candidate
			rf.Unlock()

			log.Printf("Election of #%d scheduled\n", rf.me)
			go rf.StartElection()

		}
	}
}

// === VOTING PROCESS ===

func (rf *Raft) StartElection() {
	rf.Lock()
	rf.currentTerm++
	rf.currentRole = candidate
	rf.votedFor = rf.me
	rf.persist()
	rf.votesReceived[rf.me] = 1
	logLength := len(rf.log)
	lastTerm := 0

	if len(rf.log) > 0 {
		lastTerm = rf.log[len(rf.log)-1].Term
	}

	voteRequestArgs := VoteRequestArgs{
		CId:        rf.me,
		CTerm:      rf.currentTerm,
		CLogLength: logLength,
		CLogTerm:   lastTerm,
	}
	rf.Unlock()

	rf.cancelElecTimerChan <- true // reset election timer before sending Vote Req

	for i, _ := range rf.peers {
		if i != rf.me {
			go func(i int) {
				rep := VoteRequestReply{}
				ok := rf.sendVoteRequest(i, voteRequestArgs, &rep)

				if ok { // handle vote rep
					rf.cancelElecTimerChan <- true // reset election timer when receiving Vote Rep
					rf.Lock()
					defer rf.Unlock()

					n := len(rf.peers)

					if rep.Term > rf.currentTerm {
						rf.BecomeFollower(rep.Term)
						return
					}

					log.Printf("VOTERESPONSE[%d-%d]: %v\n", rf.me, rep.VoterId, rep)

					if rf.currentRole == candidate && rep.Term == rf.currentTerm && rep.Granted {
						rf.votesReceived[rep.VoterId] = 1

						if len(rf.votesReceived) >= (n/2)+1 {
							rf.currentRole = leader
							rf.currentLeader = rf.me
							for peerId := 0; peerId < len(rf.peers); peerId++ {
								if peerId != rf.me {
									rf.sentLength[peerId] = len(rf.log)
									rf.ackedLength[peerId] = 0
								}
							}

							log.Printf("%d became leader in term %d\n", rf.me, rf.currentTerm)
							go rf.StartLeader()

							return
						}
					}
				}
			}(i)
		}
	}

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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendVoteRequest(server int, args VoteRequestArgs, reply *VoteRequestReply) bool {
	//fmt.Printf("%d send request vote to %d at %d\n", rf.me, server, int(time.Now().UnixMilli()))
	ok := rf.peers[server].Call("Raft.ReceiveVoteRequest", args, reply)

	return ok
}

// RPC method called on receiving server
func (rf *Raft) ReceiveVoteRequest(args VoteRequestArgs, reply *VoteRequestReply) {
	// Your code here.
	if rf.killed() == true {
		return
	}

	rf.Lock()
	defer rf.Unlock()
	log.Printf("VOTEREQUEST[%d-%d]: %v\n", args.CId, rf.me, args)
	rf.cancelElecTimerChan <- true

	if args.CTerm > rf.currentTerm {
		rf.BecomeFollower(args.CTerm)
	}

	lastTerm := 0
	if len(rf.log) > 0 {
		lastTerm = rf.log[len(rf.log)-1].Term
	}

	logOk := (args.CLogTerm > lastTerm) ||
		(args.CLogTerm == lastTerm && args.CLogLength >= len(rf.log))

	termOk := args.CTerm > rf.currentTerm ||
		(args.CTerm == rf.currentTerm && rf.votedFor == -1 || rf.votedFor == args.CId)

	//log.Printf("VOTEREQUEST[%d-%d]: logOk: %v, termOk: %v\n", args.CId, rf.me, logOk, termOk)

	if logOk && termOk {
		rf.votedFor = args.CId
		rf.persist()
		reply.Granted = true
	} else {
		reply.Granted = false
	}

	reply.VoterId = rf.me
	reply.Term = rf.currentTerm

	//fmt.Fprintf(file, "VOTEREQUEST[%d-%d] done: %v at %d\n", args.CId, rf.me, reply, int(time.Now().UnixNano()))
}

// =====================

// === SENDING LOGS PROCESS ===

// periodically sends heart beat at nodeId
func (rf *Raft) StartLeader() {
	ticker := time.NewTicker(heartBeatTimeout)
	<-ticker.C

	for {
		<-ticker.C
		//log.Printf("Leader's log: %v\n", rf.log)
		rf.SendHBLogs()

		if _, isLeader := rf.GetState(); !isLeader {
			return
		}
	}

}

func (rf *Raft) SendHBLogs() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.ReplicateLog(i)
		}
	}
}

// RPC method called on receiving server
func (rf *Raft) ReceiveLogRequest(args LogRequestArgs, reply *LogRequestReply) {
	if rf.killed() == true {
		return
	}

	log.Printf("LOGREQUEST[%d-%d]: %v\n", args.LeaderId, rf.me, args)
	rf.cancelElecTimerChan <- true

	rf.Lock()
	if args.Term > rf.currentTerm {
		rf.BecomeFollower(args.Term)
	}

	if args.Term == rf.currentTerm {
		rf.currentRole = follower
		rf.currentLeader = args.LeaderId
	}

	logOk := len(rf.log) >= args.PrefixLen &&
		(args.PrefixLen == 0 || rf.log[args.PrefixLen-1].Term == args.PrefixTerm)

	if args.Term == rf.currentTerm && logOk {
		rf.Unlock()
		rf.AppendEntries(args.PrefixLen, args.LeaderCommit, args.Suffix)
		rf.Lock()
		ack := args.PrefixLen + len(args.Suffix)
		reply.Ack = ack
		reply.Success = true
	} else {
		reply.Ack = 0
		reply.Success = false
	}
	rf.Unlock()
	reply.FId = rf.me
	reply.Fterm = rf.currentTerm

	// for debugging
	reply.Type = args.Type
}

func (rf *Raft) sendLogRequest(server int, args LogRequestArgs, reply *LogRequestReply) bool {
	//fmt.Printf("%d send request vote to %d at %d\n", rf.me, server, int(time.Now().UnixMilli()))
	ok := rf.peers[server].Call("Raft.ReceiveLogRequest", args, reply)

	return ok
}

// === HELPER METHODS ===

func getRandomTimeout() time.Duration {
	return time.Millisecond * time.Duration(150+rand.Intn(150))
}

func (rf *Raft) AppendEntries(prefixLen, leaderCommit int, suffix []Entry) {
	rf.Lock()
	defer rf.Unlock()

	//log.Printf("APPENDENTRIES BEFORE[%d]: prefixLen: %d, leaderCommit: %d, suffix: %v\n", rf.me, prefixLen, leaderCommit, suffix)
	if len(suffix) > 0 && len(rf.log) > prefixLen {
		index := Min(len(rf.log), prefixLen+len(suffix)) - 1

		if rf.log[index].Term != suffix[index-prefixLen].Term {
			rf.log = rf.log[:prefixLen]
			rf.persist()
		}
	}

	if prefixLen+len(suffix) > len(rf.log) {
		rf.log = append(rf.log, suffix[(len(rf.log)-prefixLen):]...)
		rf.persist()
	}

	if leaderCommit > rf.commitLength {
		for i := rf.commitLength; i <= leaderCommit-1; i++ {
			if i > 0 {
				rf.SendToClient(i+1, rf.log[i].Command)
			} else {
				rf.SendToClient(1, rf.log[0].Command)
			}
		}
		rf.commitLength = leaderCommit
	}
	//log.Printf("APPENDENTRIES AFTER[%d]: prefixLen: %d log: %v, commitLen: %d\n", rf.me, prefixLen, rf.log, rf.commitLength)
}

func (rf *Raft) ReplicateLog(followerId int) {
	rf.RLock()

	prefixLen := rf.sentLength[followerId]
	var suffix []Entry

	if len(rf.log) > 0 {
		suffix = rf.log[prefixLen:len(rf.log)]
	} else {
		suffix = make([]Entry, 0)
	}
	prefixTerm := 0

	if prefixLen > 0 {
		prefixTerm = rf.log[prefixLen-1].Term
	}

	args := LogRequestArgs{
		LeaderId:     rf.me,
		Term:         rf.currentTerm,
		PrefixLen:    prefixLen,
		PrefixTerm:   prefixTerm,
		LeaderCommit: rf.commitLength,
		Suffix:       suffix,
	}
	rf.RUnlock()

	rep := LogRequestReply{}
	ok := rf.sendLogRequest(followerId, args, &rep)

	if ok {
		rf.Lock()
		log.Printf("LOGRESPONSE[%d-%d]: %v\n", rf.me, rep.FId, rep)

		rf.cancelElecTimerChan <- true
		if rep.Fterm == rf.currentTerm && rf.currentRole == leader {
			if rep.Success && rep.Ack >= rf.ackedLength[rep.FId] {
				rf.sentLength[rep.FId] = rep.Ack
				rf.ackedLength[rep.FId] = rep.Ack
				//log.Printf("BEFORECOMMIT[%d-%d]: sentLen: %v, ackedLen: %v\n", rf.me, rep.FId, rf.sentLength, rf.ackedLength)
				rf.Unlock()
				rf.CommitLogEntries(rep.FId)
			} else if rf.sentLength[rep.FId] > 0 {
				rf.sentLength[rep.FId]--
				rf.Unlock()
				rf.ReplicateLog(rep.FId)
			} else {
				rf.Unlock()
			}
		} else if rep.Fterm > rf.currentTerm {
			rf.Unlock()
			rf.BecomeFollower(rep.Fterm)
		} else {
			rf.Unlock()
		}

	}
}

// para followerId is for debug purposes only
func (rf *Raft) CommitLogEntries(followerId int) {
	var acks int
	rf.Lock()
	defer rf.Unlock()

	//log.Printf("COMMIT PRECEDE[%d-%d]: commitLen: %d, logLen: %d\n", rf.me, followerId, rf.commitLength, len(rf.log))

	for rf.commitLength < len(rf.log) {
		acks = 0

		for nodeId := 0; nodeId < len(rf.peers); nodeId++ {
			if rf.ackedLength[nodeId] > rf.commitLength {
				acks++
			}
		}

		if acks >= (len(rf.peers)+2)/2 {
			if rf.commitLength > 0 {
				rf.SendToClient(rf.commitLength+1, rf.log[rf.commitLength].Command)
			} else {
				rf.SendToClient(1, rf.log[0].Command)
			}
			rf.commitLength++
		} else {
			break
		}
	}

	log.Printf("COMMIT AFTER %d: acks: %d commitLen: %d, logLen: %d\n", rf.me, acks, rf.commitLength, len(rf.log))
}

func (rf *Raft) SendToClient(index int, message interface{}) {
	log.Printf("<<<%d send %v index %d to client\n", rf.me, message, index)

	rf.messageChan <- ApplyMsg{
		Index:   index,
		Command: message,
	}
}

func (rf *Raft) BecomeFollower(term int) {
	rf.currentTerm = term
	rf.currentRole = follower
	rf.votedFor = -1
	rf.persist()
}

func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}

}
