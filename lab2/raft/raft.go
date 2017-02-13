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
	"labrpc"
	"math"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// States of a single Raft peer.
//
const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

//
// Timers (in milliseconds).
//
const (
	HEARTBEAT_TIMEOUT_MS    = 100
	MIN_ELECTION_TIMEOUT_MS = 300
	MAX_ELECTION_TIMEOUT_MS = 600
)

//
// Log entry including term and command.
//
type LogEntry struct {
	Term    int
	Command interface{}
}

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state.
	commitIndex    int
	lastApplied    int
	state          int
	newTermChannel chan bool     // notify server of new term's peer
	applyCh        chan ApplyMsg // notify client of committed command

	// Volatile state (leader).
	nextIndex  []int
	matchIndex []int

	// Volatile state (condidate).
	voteNum            int       // the number of votes got from peers
	voteSuccessChannel chan bool // notify server of successful vote
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	// DPrintf("me: %d, readPersist: %s\n", rf.me, d)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	if len(rf.log) == 0 {
		rf.log = append(rf.log, LogEntry{})
	}
	// DPrintf("[readPersist server(%d)] byte: %s\n", rf.me, data)
	// DPrintf("[readPersist server(%d)] currentTerm: %d, votedFor: %d, log: %s\n",
	//	rf.me, rf.currentTerm, rf.votedFor, rf.log[0])
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()

	if args.Entries != nil {
		//DPrintf("[AppendEntries server(%d) term(%d)]: term:%d, leaderId:%d, prevLogIndex:%d, prevLogTerm:%d, entries:%s, leaderCommit:%d\n",
		//	rf.me, rf.currentTerm, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
	}
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		if args.PrevLogIndex > 0 && (args.PrevLogIndex >= len(rf.log) ||
			rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
			reply.Success = false
			rf.log = rf.log[:int(math.Min(float64(args.PrevLogIndex), float64(len(rf.log))))]
			// Persist state.
			rf.persist()
			if args.PrevLogIndex >= len(rf.log) {
				reply.ConflictTerm = -1
				reply.ConflictIndex = len(rf.log)
			} else {
				reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
				for i, e := range rf.log {
					if e.Term == reply.ConflictTerm {
						reply.ConflictIndex = i
						break
					}
				}
			}
		} else {
			reply.Success = true
			for i, e := range args.Entries {
				idx := args.PrevLogIndex + i + 1
				if idx >= len(rf.log) {
					rf.log = append(rf.log, e)
					// Persist state.
					rf.persist()
				} else {
					if rf.log[idx].Term != e.Term {
						rf.log = rf.log[:idx]
						rf.log = append(rf.log, e)
						// Persist state.
						rf.persist()
					}
				}
			}
			// DPrintf("[AppendEntries server(%d) term(%d)]: before applyCh prevLogIndex:%d, log:%s, leaderCommit:%d, rf.commitIdex:%d\n",
			//            rf.me, rf.currentTerm, args.PrevLogIndex, rf.log, args.LeaderCommit, rf.commitIndex)

			if args.LeaderCommit > rf.commitIndex {
				prevIndex := rf.commitIndex
				rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
				// Apply commands to state machine.
				if rf.commitIndex > prevIndex {
					go func() {
						rf.mu.Lock()
						for rf.lastApplied < rf.commitIndex {
							rf.lastApplied++
							idx := rf.lastApplied
							cmd := rf.log[rf.lastApplied].Command
							/*
							   							DPrintf("[AppendEntries server(%d) term(%d)]: applyCh command:%d, idx:%d, log:%s\n",
							                     rf.me, rf.currentTerm, cmd, idx, rf.log)
							*/
							/*
								DPrintf("(candidate) [server:(%d) term(%d) idx(%d) command(%d) len_log(%d) state(%d)]: votedFor:%d, commitIndex:%d, lastApplied:%d, log:%s\n\n",
									rf.me, rf.currentTerm, idx, cmd, len(rf.log), rf.state, rf.votedFor, rf.commitIndex, rf.lastApplied, rf.log)
							*/
							rf.applyCh <- ApplyMsg{idx, cmd, false, nil}
						}
						rf.mu.Unlock()
					}()
				}
			}
		}
	}
	if args.Term >= rf.currentTerm {
		// DPrintf("[AppendEnt server(%d) term(%d)]: get leader %d\n",
		// rf.me, rf.currentTerm, args.LeaderId)
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			// Persist state.
			rf.persist()
			// Notify doXXX() that hearing from a new term peer.
			go func() {
				rf.newTermChannel <- true
			}()
		} else if args.Term == rf.currentTerm {
			rf.votedFor = args.LeaderId
			// Notify doXXX() that hearing from a new term peer.
			go func() {
				rf.newTermChannel <- true
			}()
		}
	}
}

//
// Send a AppendEntries RPC to a server.
//
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	var r bool
	if server == rf.me {
		r = true
	} else {
		r = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		// Persist state.
		rf.persist()
		// Notify doXXX() that hearing from a new term peer.
		go func() {
			rf.newTermChannel <- true
		}()
	}
	rf.mu.Unlock()
	return r
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	// TODO: 5.4 (RequestVote RPC)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		// DPrintf("[RequestVote server(%d) term(%d)]: get candidate %d\n",
		// rf.me, rf.currentTerm, args.CandidateId)
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term &&
				args.LastLogIndex >= len(rf.log)-1) {
			/*
				DPrintf("(cheng): [server:%d, term:%d, votedFor:%d] voted 1 for [server:%d, term:%d]!!\n",
					rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term)
			*/
			rf.votedFor = args.CandidateId
			// Persist state.
			rf.persist()
			reply.VoteGranted = true
		} else {
			/*
				DPrintf("(cheng): [server:%d, term:%d, votedFor:%d] reject 1 for [server:%d, term:%d]!! (args.LastLogTerm:%d, my.LastLogTerm:%d, args.LastLogIndex: %d, my.LastLogIndex:%d)\n",
					rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term,
					args.LastLogTerm, rf.log[len(rf.log)-1].Term, args.LastLogIndex, len(rf.log)-1)
			*/
			rf.votedFor = -1
			// Persist state.
			rf.persist()
			reply.VoteGranted = false
		}
		// Notify doXXX() that hearing from a new term peer.
		go func() {
			rf.mu.Lock()
			rf.currentTerm = args.Term
			// Persist state.
			rf.persist()
			rf.mu.Unlock()
			rf.newTermChannel <- true
		}()
	} else if args.Term == rf.currentTerm {
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
			(args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
				(args.LastLogTerm == rf.log[len(rf.log)-1].Term &&
					args.LastLogIndex >= len(rf.log)-1)) {
			/*
			   DPrintf("(cheng): [server:%d, term:%d, votedFor:%d] voted 2 for [server:%d, term:%d]!!\n",
			     rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term)
			*/
			rf.votedFor = args.CandidateId
			// Persist state.
			rf.persist()
			reply.VoteGranted = true
		} else {
			/*
			   			DPrintf("(cheng): [server:%d, term:%d, votedFor:%d] reject 2 for [server:%d, term:%d]!! (args.LastLogTerm:%d, my.LastLogTerm:%d, args.LastLogIndex: %d, my.LastLogIndex:%d)\n",
			           rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term,
			           args.LastLogTerm, rf.log[len(rf.log)-1].Term, args.LastLogIndex, len(rf.log)-1)
			*/
			reply.VoteGranted = false
		}
	} else {
		/*
		   DPrintf("(cheng): [server:%d, term:%d, votedFor:%d] reject 3 for [server:%d, term:%d]!! (args.LastLogTerm:%d, my.LastLogTerm:%d, args.LastLogIndex: %d, my.LastLogIndex:%d)\n",
		       rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term,
		       args.LastLogTerm, rf.log[len(rf.log)-1].Term, args.LastLogIndex, len(rf.log)-1)
		*/
		reply.VoteGranted = false
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	var r bool
	if server == rf.me {
		r = true
	} else {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		r = ok && reply.VoteGranted
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		// Notify doXXX() that hearing from a new term peer.
		go func() {
			rf.mu.Lock()
			rf.currentTerm = args.Term
			rf.votedFor = -1
			// Persist state.
			rf.persist()
			rf.mu.Unlock()
			rf.newTermChannel <- true
		}()
	}
	rf.mu.Unlock()
	return r
}

func (rf *Raft) heartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		server := i
		lastIdx := len(rf.log)
		nextIdx := rf.nextIndex[server]
		currentTerm := rf.currentTerm
		commitIdx := rf.commitIndex
		rf.mu.Unlock()

		go func() {
			var reply AppendEntriesReply
			for nextIdx >= 1 {
				rf.mu.Lock()
				/*
					DPrintf("[heartbeat server(%d) term(%d)]: log:%s idx:%d server:%d\n",
						rf.me, currentTerm, rf.log, nextIdx, server)
				*/
				if lastIdx > len(rf.log) {
					lastIdx = len(rf.log)
				}
				if nextIdx > lastIdx {
					rf.mu.Unlock()
					return
				}
				args := AppendEntriesArgs{currentTerm, rf.me, nextIdx - 1,
					rf.log[nextIdx-1].Term, rf.log[nextIdx:lastIdx], commitIdx}
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(server, args, &reply)
				rf.mu.Lock()
				if rf.state != LEADER || rf.currentTerm != currentTerm {
					rf.mu.Unlock()
					return
				}
				if ok && !reply.Success {
					lastConflictTermIdx := -1
					for i, e := range rf.log {
						if e.Term == reply.ConflictTerm {
							lastConflictTermIdx = i
						}
					}
					if lastConflictTermIdx != -1 {
						nextIdx = lastConflictTermIdx + 1
					} else {
						nextIdx = reply.ConflictIndex
					}
				}
				rf.mu.Unlock()
				if ok && reply.Success {
					rf.mu.Lock()
					// Update nextIndex and matchIndex.
					if rf.nextIndex[server] >= lastIdx || rf.matchIndex[server] >= lastIdx-1 {
						rf.mu.Unlock()
						break
					}
					rf.nextIndex[server] = lastIdx
					rf.matchIndex[server] = lastIdx - 1
					/*
					   					DPrintf("[Start server(%d) term(%d)]: command:%d, idx:%d server:%d nextIndex:%d\n",
					             	rf.me, rf.currentTerm, rf.log[nextIdx], nextIdx, server, rf.nextIndex[server])
					*/
					count := 0
					minIdx := 0
					for i, _ := range rf.peers {
						if rf.matchIndex[i] > rf.commitIndex {
							count++
							if minIdx == 0 || minIdx > rf.matchIndex[i] {
								minIdx = rf.matchIndex[i]
							}
						}
					}
					if count >= len(rf.peers)/2+1 && rf.log[minIdx].Term == rf.currentTerm {
						rf.commitIndex = minIdx
						go func() {
							rf.mu.Lock()
							for rf.lastApplied < rf.commitIndex {
								rf.lastApplied++
								idx := rf.lastApplied
								cmd := rf.log[rf.lastApplied].Command
								/*
								   								DPrintf("[Start server(%d) term(%d)]: applyCh command:%d, idx:%d\n",
								                       rf.me, rf.currentTerm, cmd, idx)
								*/
								/*
									DPrintf("(LEADER) [server:(%d) term(%d) idx(%d) command(%d) len_log(%d) state(%d)]: votedFor:%d, commitIndex:%d, lastApplied:%d nextIndex:%s, matchIndex:%s, log:%s\n\n",
										rf.me, rf.currentTerm, idx, cmd, len(rf.log), rf.state, rf.votedFor, rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex,
										rf.log)
								*/
								rf.applyCh <- ApplyMsg{idx, cmd, false, nil}
							}
							rf.mu.Unlock()
						}()
					}
					rf.mu.Unlock()
					break
				} else {
					nextIdx--
				}
			}
		}()
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	rf.mu.Lock()
	term = rf.currentTerm
	// If server is leader.
	if rf.state == LEADER {
		isLeader = true
		index = len(rf.log)
		// Append command to log.
		rf.log = append(rf.log, LogEntry{term, command})
		// Persist state.
		rf.persist()
		// DPrintf("(cheng) Start server(%d) append: term: %d command:%d, log:%s\n", rf.me, term, command, rf.log)
		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me] = len(rf.log) - 1
	}
	rf.mu.Unlock()

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
// Get a new randomized election timeout.
//
func getTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(MAX_ELECTION_TIMEOUT_MS-MIN_ELECTION_TIMEOUT_MS) +
		MIN_ELECTION_TIMEOUT_MS)
}

//
// Follower's function.
//
func (rf *Raft) doFollower() {
	/*
	 DPrintf("[doFollower server(%d) term(%d)] enter follower\n",
	 rf.me, rf.currentTerm)
	*/
loop:
	for {
		// Wait for election timeout.
		timeout := getTimeout()
		// DPrintf("[doFollower server(%d) term(%d)] before time.After(): timeout: %d\n",
		//	rf.me, rf.currentTerm, timeout);
		select {
		case <-rf.newTermChannel:
			// If follower gets note from new term peer,
			// just loopfollower.
		case <-time.After(timeout * time.Millisecond):
			//DPrintf("after time.After: me: %d\n", rf.me);
			// If follower has election timeout,
			// change state to candidate, increment its current term and return.
			DPrintf("[doFollower server(%d) term(%d)] TIMEOUT!!\n",
				rf.me, rf.currentTerm)
			rf.mu.Lock()
			rf.state = CANDIDATE
			rf.currentTerm++
			// Persist state.
			rf.persist()
			rf.mu.Unlock()
			break loop
		}
	}
}

//
// Candidate's function.
//
func (rf *Raft) doCandidate() {
	//	DPrintf("[doCandidate server(%d) term(%d)] enter\n", rf.me, rf.currentTerm)
loop:
	for {
		var args RequestVoteArgs
		rf.mu.Lock()
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		// TODO: 5.4 RequestVote
		args.LastLogIndex = len(rf.log) - 1
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
		rf.voteNum = 0 // Reset the number of vote.
		rf.votedFor = rf.me
		// Persist state.
		rf.persist()
		rf.mu.Unlock()

		// Start an election.
		for i := 0; i < len(rf.peers); i++ {
			go func(server int) {
				var reply RequestVoteReply
				if rf.sendRequestVote(server, args, &reply) {
					rf.mu.Lock()
					if rf.state == CANDIDATE {
						rf.voteNum++
						// If candidate gets majority vote,
						// change state to leader, notify doCandidate function by channel.
						if rf.voteNum >= (len(rf.peers)/2 + 1) {
							rf.state = LEADER
							go func() {
								rf.voteSuccessChannel <- true
							}()
							// DPrintf("[doCandidate server(%d) term(%d)] get vote_num: %d\n",
							//	rf.me, rf.currentTerm, rf.voteNum)
						}
					}
					rf.mu.Unlock()
				}
			}(i)
		}

		// Wait for vote.
		timeout := getTimeout()
		select {
		case <-rf.voteSuccessChannel:
			// If candidate wins election (already changed state to leader),
			// return.
			//rf.mu.Lock()
			// DPrintf("[doCandidate server(%d) term(%d)] turn to leader\n",
			// rf.me, rf.currentTerm)
			//rf.mu.Unlock()
			break loop
		case <-rf.newTermChannel:
			// If candidate gets note from a new term peer,
			// change state to follower, and return.
			rf.mu.Lock()
			rf.state = FOLLOWER
			// DPrintf("[doCandidate server(%d) term(%d)] degrade to follower\n",
			// rf.me, rf.currentTerm)
			rf.mu.Unlock()
			break loop
		case <-time.After(timeout * time.Millisecond):
			// If candidate has election timeout,
			// increment its current term, and start a new election.
			rf.mu.Lock()
			rf.currentTerm++
			// Persist state.
			rf.persist()
			// DPrintf("[doCandidate server(%d) term(%d)] repeat again\n",
			// rf.me, rf.currentTerm)
			rf.mu.Unlock()
		}
	}
}

//
// Leader's function.
//
func (rf *Raft) doLeader() {
	//	DPrintf("[doLeader server(%d) term(%d)] enter\n", rf.me, rf.currentTerm)
	// Reinitialization.
	rf.mu.Lock()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, len(rf.log))
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.mu.Unlock()

loop:
	for {
		// Send heartbeat.
		go rf.heartbeat()

		// Wait for next heartbeat.
		select {
		case <-rf.newTermChannel:
			// If leader gets note from a new term peer,
			// change state to follower, and return.
			rf.mu.Lock()
			// DPrintf("[doLeader server(%d) term(%d)] degrade to follower\n",
			// rf.me, rf.currentTerm)
			rf.state = FOLLOWER
			rf.mu.Unlock()
			break loop
		case <-time.After(HEARTBEAT_TIMEOUT_MS * time.Millisecond):
			// If leader has heartbeat timeout,
			// start a new heartbeat.
			// DPrintf("[doLeader server(%d) term(%d)] heartbeat again\n",
			// rf.me, rf.currentTerm)
		}
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Your initialization code here.
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER
	rf.newTermChannel = make(chan bool)
	rf.applyCh = applyCh
	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, len(rf.log))
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	rf.voteNum = 0
	rf.voteSuccessChannel = make(chan bool)

	//DPrintf("before loop(): me: %d #peers: %d\n", me, len(peers));
	go func() {
		for {
			rf.mu.Lock()
			switch rf.state {
			case FOLLOWER:
				rf.mu.Unlock()
				rf.doFollower()
			case CANDIDATE:
				rf.mu.Unlock()
				rf.doCandidate()
			case LEADER:
				rf.mu.Unlock()
				rf.doLeader()
			}
			// DPrintf("[Make go func server(%d) term(%d)] state: %d\n",
			// rf.me, rf.currentTerm, rf.state)
		}
	}()

	return rf
}
