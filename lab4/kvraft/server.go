package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//
// Types of commands.
//
const (
	PUT = iota
	APPEND
	GET
)

//
// Timer to check server state (in milliseconds).
//
const GET_STATE_TIMEOUT_MS = 100

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	CmdType  int   // PUT or APPEND or GET
	ClientId int64 // identifier (client ID) for each put/append/get command
	TimeId   int64 // identifier (timestamp) for each put/append/get command,
	// i.e. the number of nanoseconds elapsed since January 1, 1970 UTC
	// ClientId and TimeId together uniquely identify a command.
}

type Result struct {
	WrongLeader bool   // if true, this server is not leader; o.w. this server is leader.
	Err         Err    // error message
	Value       string // return value of get command
	TimeId      int64  // identifier (timestamp) for each put/append/get command
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvs map[string]string     // key-value pairs for client
	chs map[int64]chan Result // clientID-channel pairs for command
	// RPCs (Get,PutAppend) wait on each channel for result.
	rs map[int64]Result // clientID-result pairs for command,
	// store the result of last applied command for each client.
}

func (kv *RaftKV) waitForCmd(op Op) Result {
	// Start command on Raft library.
	//DPrintf("\tbefore start: op: %s\n", op)
	kv.mu.Lock()
	// Check whether this command was the one executed previously.
	if r, ok := kv.rs[op.ClientId]; ok && r.TimeId == op.TimeId {
		kv.mu.Unlock()
		return r
	}
	//  DPrintf("\tlock in waitForCmd: %s\n", op)
	//  DPrintf("\tbefore start: %s\n", op)
	_, _, isLeader := kv.rf.Start(op)
	//  DPrintf("\tafter start: %s\n", op)
	wrongLeaderResult := Result{true, "", "", op.TimeId}
	//DPrintf("\tstart: op: %s\n", op)
	if !isLeader {
		//    DPrintf("\tnot leader: %d\n", kv.me)
		// This server is not leader.
		kv.mu.Unlock()
		//    DPrintf("\tunlock in waitForCmd: !isleader\n")
		return wrongLeaderResult
	} else {
		// Create channel if not available.
		var ch chan Result
		if _, ok := kv.chs[op.ClientId]; !ok {
			kv.chs[op.ClientId] = make(chan Result)
		}
		ch = kv.chs[op.ClientId]
		kv.mu.Unlock()
		//    DPrintf("\tunlock in waitForCmd: else isleader: %s\n", op)

		// Periodically check this server is still leader,
		// and wait for result.
		for {
			if _, isLeader := kv.rf.GetState(); !isLeader {
				//        DPrintf("\tnot leader(discover): %d!!! term: %d\n", kv.me, t)
				return wrongLeaderResult
			}

			select {
			case r := <-ch:
				// Check result timestamp equal to request timestamp or not.
				// 1. result > request: put result back to channel for future use.
				// 2. result = request: return result.
				// 3. result < request: do nothing.
				//        DPrintf("\tget result: %s\n", r)
				if r.TimeId > op.TimeId {
					//          DPrintf("\tRETURN to queue\n")
					go func() {
						ch <- r
					}()
				} else if r.TimeId == op.TimeId {
					//          DPrintf("\tRETURN: %d!!!\n", op.TimeId)
					return r
				}
			case <-time.After(GET_STATE_TIMEOUT_MS * time.Millisecond):
				// Wait indefinitely.
				//DPrintf("\tWait!\n")
			}
		}
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	r := kv.waitForCmd(Op{args.Key, "", GET, args.ClientId, args.TimeId})
	reply.WrongLeader = r.WrongLeader
	reply.Err = r.Err
	reply.Value = r.Value
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var r Result
	if args.Op == "Put" {
		r = kv.waitForCmd(Op{args.Key, args.Value, PUT, args.ClientId, args.TimeId})
	} else if args.Op == "Append" {
		r = kv.waitForCmd(Op{args.Key, args.Value, APPEND, args.ClientId, args.TimeId})
	}
	//  DPrintf("\twrongleader: %d, err: %s\n", r.WrongLeader, r.Err)
	reply.WrongLeader = r.WrongLeader
	//  DPrintf("\twrongleader: %s\n", reply.WrongLeader)
	reply.Err = r.Err
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// Apply command put or append to key-value pairs.
//
func (kv *RaftKV) putAppend(key string, value string, cmdType int, clientId int64, timeId int64) {
	//  DPrintf("\t%s before lock, key:%s, value:%s, clientId:%d, timeId:%s\n", cmdType, key, value, clientId, timeId)
	kv.mu.Lock()
	//  DPrintf("\t%s after lock, key:%s, value:%s, clientId:%d, timeId:%s\n", cmdType, key, value, clientId, timeId)
	defer kv.mu.Unlock()
	//  DPrintf("\t putAppend() BEFORE HERE: key:%s value:%s\n", key, value)
	// Check whether this command was executed before.
	if r, ok := kv.rs[clientId]; ok && r.TimeId >= timeId {
		// If this command was executed before, don't execute again.
		//    DPrintf("\t%s run before key:%s, value:%s, clientId:%d, timeId:%s, now:%s\n",
		//      cmdType, key, value, clientId, timeId, r.TimeId)
		return
	}

	//  DPrintf("\t putAppend() HERE: key:%s value:%s\n", key, value)
	// Execute command.
	r := Result{false, OK, "", timeId}
	if cmdType == PUT {
		kv.kvs[key] = value
	} else {
		// cmdType is APPEND.
		if _, ok := kv.kvs[key]; !ok {
			kv.kvs[key] = ""
		}
		kv.kvs[key] += value
	}

	// Update result of client's last applied command.
	//  DPrintf("\tbefore time: %d\n", kv.rs[clientId].TimeId)
	kv.rs[clientId] = r

	// Put result in corresponding client's channel.
	var ch chan Result
	if _, ok := kv.chs[clientId]; !ok {
		kv.chs[clientId] = make(chan Result)
	}
	ch = kv.chs[clientId]

	go func() {
		ch <- r
	}()
}

//
// Apply command get to key-value pairs.
//
func (kv *RaftKV) get(key string, clientId int64, timeId int64) {
	//  DPrintf("\tget before lock, key:%s, clientId:%d, timeId:%s\n", key, clientId, timeId)
	kv.mu.Lock()
	//  DPrintf("\tget after lock, key:%s, clientId:%d, timeId:%s\n", key, clientId, timeId)
	defer kv.mu.Unlock()

	// Check whether this command was executed before.
	if r, ok := kv.rs[clientId]; ok && r.TimeId >= timeId {
		// If this command was executed before, don't execute again.
		//    DPrintf("\tget run before: key:%s, clientId:%d, timeId:%s, now:%s\n", key, clientId, timeId, r.TimeId)
		return
	}

	// Execute command.
	r := Result{false, OK, "", timeId}
	if _, ok := kv.kvs[key]; !ok {
		r.Err = ErrNoKey
	} else {
		r.Err = OK
		r.Value = kv.kvs[key]
	}

	// Update result of client's last applied command.
	//  DPrintf("\tbefore time: %d\n", kv.rs[clientId].TimeId)
	kv.rs[clientId] = r

	// Put result in corresponding client's channel.
	var ch chan Result
	if _, ok := kv.chs[clientId]; !ok {
		kv.chs[clientId] = make(chan Result)
	}
	ch = kv.chs[clientId]

	go func() {
		ch <- r
	}()
}

//
// Apply command from channel of Raft library.
//
func (kv *RaftKV) applyCmd() {
	for {
		msg := <-kv.applyCh
		//    DPrintf("get command: %s\n", msg)
		op := msg.Command.(Op)
		//    DPrintf("server: %d After get command: %s\n", kv.me, op)
		switch op.CmdType {
		case PUT:
			// PUT calls same function as APPEND.
			kv.putAppend(op.Key, op.Value, op.CmdType, op.ClientId, op.TimeId)
			//fallthrough
		case APPEND:
			kv.putAppend(op.Key, op.Value, op.CmdType, op.ClientId, op.TimeId)
		case GET:
			kv.get(op.Key, op.ClientId, op.TimeId)
		default:
			DPrintf("Get wrong command: %s\n", msg)
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.kvs = make(map[string]string)
	kv.chs = make(map[int64]chan Result)
	kv.rs = make(map[int64]Result)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Use one goroutine to apply command per server.
	go kv.applyCmd()

	return kv
}
