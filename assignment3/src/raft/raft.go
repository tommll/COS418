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
	"assign3/labrpc"
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

	// persistent data
	currentTerm  int
	votedFor     int
	log          []Entry
	commitLength int

	// volatile data
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
	rf.RLock()
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	rf.RUnlock()
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

	if rf.currentRole == leader {
		return rf.commitLength + 1, rf.currentTerm, true
	} else {
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
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.log = make([]Entry, 0)
	rf.commitLength = 0
	rf.currentRole = follower
	rf.currentLeader = -1
	rf.votesReceived = make(map[int]int)
	rf.sentLength = make([]int, len(peers)+1)
	rf.ackedLength = make([]int, len(peers)+1)
	rf.dead = 0

	//fmt.Printf("Initializing %d, timeout: %d\n", me, int(ElectionTimeout))

	// set up cancel election timer
	rf.cancelElecTimerChan = make(chan bool)

	go rf.StartElecTimer(getRandomTimeout())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) StartElecTimer(ElectionTimeout time.Duration) {
	//fmt.Fprintf(file, "%d start elec timer #%d at %d\n", rf.me, rand, int(time.Now().UnixNano()))

	for rf.killed() == false {

		select {
		case <-rf.cancelElecTimerChan: // reset election timer
			log.Printf("elec timer #%d started\n", rf.me)
			continue
		case <-time.After(ElectionTimeout): // start new election
			//fmt.Fprintf(file, "Election #%d timeout at %d\n", rand, int(time.Now().UnixNano()))
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
	//fmt.Fprintf(file, "Start election on server %d at %d\n", rf.me, int(time.Now().UnixNano()))

	rf.Lock()
	rf.currentTerm++
	rf.currentRole = candidate
	rf.votedFor = rf.me
	rf.votesReceived[rf.me] = 1
	lastTerm := 0

	if len(rf.log) > 0 {
		lastTerm = rf.log[len(rf.log)-1].Term
	}

	voteRequestArgs := VoteRequestArgs{
		CId:        rf.me,
		CTerm:      rf.currentTerm,
		CLogLength: len(rf.log),
		CLogTerm:   lastTerm,
	}
	rf.Unlock()

	rf.cancelElecTimerChan <- true // reset election timer before sending Vote Req

	for i, _ := range rf.peers {
		if i != rf.me {
			go func(i int) {
				voteReponse := VoteRequestReply{}
				ok := rf.sendVoteRequest(i, voteRequestArgs, &voteReponse)

				if ok { // handle vote rep
					rf.cancelElecTimerChan <- true // reset election timer when receiving Vote Rep

					voterId := voteReponse.VoterId
					term := voteReponse.Term
					granted := voteReponse.Granted
					n := len(rf.peers)

					rf.Lock()
					currentRole := rf.currentRole
					currentTerm := rf.currentTerm
					rf.Unlock()

					log.Printf("VOTERESPONSE[%d-%d]: %v\n", rf.me, voterId, voteReponse)

					if currentRole == candidate && term == currentTerm && granted {
						rf.votesReceived[voterId] = 1

						if len(rf.votesReceived) >= (n+1)/2 {
							rf.Lock()
							rf.currentRole = leader
							rf.currentLeader = rf.me
							log.Printf("%d became leader in term %d\n", rf.me, rf.currentTerm)
							go rf.MonitorHBs()
							rf.Unlock()
							return
						}
					} else if term > rf.currentTerm {
						rf.Lock()
						rf.currentTerm = term
						rf.currentRole = follower
						rf.votedFor = -1
						rf.Unlock()
						return
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

	log.Printf("VOTEREQUEST[%d-%d]: %v\n", args.CId, rf.me, args)
	rf.cancelElecTimerChan <- true

	cId := args.CId
	cTerm := args.CTerm
	cLogLength := args.CLogLength
	cLogTerm := args.CLogTerm

	rf.Lock()
	if cTerm > rf.currentTerm {
		rf.currentTerm = cTerm
		rf.currentRole = follower
		rf.votedFor = -1
	}
	rf.Unlock()

	lastTerm := 0

	if len(rf.log) > 0 {
		lastTerm = rf.log[len(rf.log)-1].Term
	}

	logOk := (cLogTerm > lastTerm) ||
		(cLogTerm == lastTerm && cLogLength >= len(rf.log))

	//fmt.Fprintf(file, "Vote condition[%d-%d]: logOk(%v), voteFor(%v)\n", args.CId, rf.me, logOk, rf.votedFor)

	rf.Lock()
	termOk := cTerm > rf.currentTerm ||
		(cTerm == rf.currentTerm && rf.votedFor == -1 || rf.votedFor == cId)
	rf.Unlock()

	if logOk && termOk {
		rf.votedFor = cId
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
func (rf *Raft) MonitorHBs() {
	ticker := time.NewTicker(heartBeatTimeout)

	for {
		<-ticker.C
		rf.SendHeartBeats()

		if _, isLeader := rf.GetState(); !isLeader {
			return
		}
	}

}

func (rf *Raft) SendHeartBeats() {
	n := len(rf.peers)

	rf.cancelElecTimerChan <- true // reset election timer before sending HBs

	for i := 0; i < n; i++ {
		if i != rf.me {
			go func(i int) {
				//fmt.Fprintf(file, "%d sending HB to %d at %d\n", rf.me, i, int(time.Now().UnixNano()))

				prefixLen := rf.sentLength[i]
				suffix := rf.log[prefixLen:]
				prefixTerm := 0

				if prefixLen > 0 {
					prefixTerm = rf.log[prefixLen-1].Term
				}

				var args LogRequestArgs

				rf.RLock()
				{
					args = LogRequestArgs{
						LeaderId:     rf.me,
						Term:         rf.currentTerm,
						PrefixLen:    prefixLen,
						PrefixTerm:   prefixTerm,
						LeaderCommit: rf.commitLength,
						Suffix:       suffix,
						Type:         "HB",
					}
				}
				rf.RUnlock()
				rep := LogRequestReply{}
				ok := rf.sendLogRequest(i, args, &rep) // send HB req

				if ok { // handle HB rep
					log.Printf("LOGRESPONSE[%d-%d]: %v\n", rf.me, rep.FId, rep)
					rf.cancelElecTimerChan <- true // reset election timer when receiving HBs

					term := rep.Fterm
					followerId := rep.FId
					ack := rep.Ack
					success := rep.Success

					rf.RLock()
					currentTerm := rf.currentTerm
					currentRole := rf.currentRole
					rf.RUnlock()

					if term == currentTerm && currentRole == leader {
						if success == true && ack >= rf.ackedLength[followerId] {
							rf.sentLength[followerId] = ack
							rf.ackedLength[followerId] = ack
							rf.CommitLogEntries()
						} else if rf.sentLength[followerId] > 0 {
							rf.sentLength[followerId] = rf.sentLength[followerId] - 1
							//rf.ReplicateLog(rf.me, followerId, rep.Type)
						}
					} else if term > currentTerm {
						rf.Lock()
						rf.currentTerm = term
						rf.currentRole = follower
						rf.votedFor = -1
						rf.Unlock()
					}
				}

			}(i)
		}
	}
}

func (rf *Raft) HandleLogResponse(rep LogRequestReply) {
	if rf.killed() == true {
		return
	}

	log.Printf("LOGRESPONSE[%d-%d]: %v\n", rf.me, rep.FId, rep)
	rf.cancelElecTimerChan <- true

	term := rep.Fterm
	followerId := rep.FId
	ack := rep.Ack
	success := rep.Success

	rf.RLock()
	currentTerm := rf.currentTerm
	currentRole := rf.currentRole
	rf.RUnlock()

	if term == currentTerm && currentRole == leader {
		if success == true && ack >= rf.ackedLength[followerId] {
			rf.sentLength[followerId] = ack
			rf.ackedLength[followerId] = ack
			rf.CommitLogEntries()
		} else if rf.sentLength[followerId] > 0 {
			rf.sentLength[followerId] = rf.sentLength[followerId] - 1
			rf.ReplicateLog(rf.me, followerId, rep.Type)
		}
	} else if term > currentTerm {
		rf.Lock()
		rf.currentTerm = term
		rf.currentRole = follower
		rf.votedFor = -1
		rf.Unlock()
		//fmt.Fprintf(file, "LOGRESPONSE[%d-%d] cancel ElecTimer cuz > term at %d\n", rf.me, rep.FId, int(time.Now().UnixNano()))
	}
}

func (rf *Raft) ReplicateLog(leaderId, followerId int, data string) {
	//fmt.Fprintf(file, "Replicate log [%d-%d]: %s\n", leaderId, followerId, data)

	prefixLen := rf.sentLength[followerId]
	suffix := rf.log[prefixLen:]
	prefixTerm := 0

	if prefixLen > 0 {
		prefixTerm = rf.log[prefixLen-1].Term
	}

	var args LogRequestArgs

	rf.RLock()
	{
		args = LogRequestArgs{
			LeaderId:     leaderId,
			Term:         rf.currentTerm,
			PrefixLen:    prefixLen,
			PrefixTerm:   prefixTerm,
			LeaderCommit: rf.commitLength,
			Suffix:       suffix,
			Type:         data,
		}
	}
	rf.RUnlock()
	reply := LogRequestReply{}
	rf.sendLogRequest(followerId, args, &reply)
	rcvLogChan <- reply
}

// RPC method called on receiving server
func (rf *Raft) ReceiveLogRequest(args LogRequestArgs, reply *LogRequestReply) {
	if rf.killed() == true {
		return
	}

	log.Printf("LOGREQUEST[%d-%d]: %v\n", args.LeaderId, rf.me, args)
	rf.cancelElecTimerChan <- true

	leaderId := args.LeaderId
	term := args.Term
	prefixLen := args.PrefixLen
	prefixTerm := args.PrefixTerm
	leaderCommit := args.LeaderCommit
	suffix := args.Suffix

	nodeId := rf.me

	rf.Lock()
	{
		if term > rf.currentTerm {
			rf.currentTerm = term
			rf.votedFor = -1
			//fmt.Fprintf(file, "LOGREQUEST[%d-%d] cancel ElecTimer cuz > term at %d\n", args.LeaderId, rf.me, int(time.Now().UnixNano()))
		}

		if term == rf.currentTerm {
			rf.currentRole = follower
			rf.currentLeader = leaderId
		}
	}
	rf.Unlock()

	logOk := len(rf.log) >= prefixLen &&
		(prefixLen == 0 || rf.log[prefixLen-1].Term == prefixTerm)

	if term == rf.currentTerm && logOk {
		rf.AppendEntries(prefixLen, leaderCommit, suffix)
		ack := prefixLen + len(suffix)
		reply.Ack = ack
		reply.Success = true
	} else {
		reply.Ack = 0
		reply.Success = false
	}

	reply.FId = nodeId
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

}

func (rf *Raft) CommitLogEntries() {

}
