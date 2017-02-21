package shardkv

// import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "encoding/gob"
import "shardmaster"
import "log"
import "time"
import "bytes"

var Debug = 0

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
	UPDATE_STATE // Used by sharded servers themselves to keep consistent state.
)

//
// Timer to check server state (in milliseconds).
//
const GET_STATE_TIMEOUT_MS = 100

//
// Timer to check configuration (in milliseconds).
//
const GET_CONFIG_TIMEOUT_MS = 10

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Config   shardmaster.Config
	KVs      map[string]string // key-value pairs for client
	Rs       map[int64]Result  // clientID-result pairs for command
	CmdType  int               // PUT or APPEND or GET or UPDATE_STATE
	ClientId int64             // identifier (client ID) for each put/append/get/updateState command
	TimeId   int64             // identifier (timestamp) for each put/append/get/updateState command,
	// i.e. the number of nanoseconds elapsed since January 1, 1970 UTC
	// ClientId and TimeId together uniquely identify a command.
}

type Result struct {
	WrongLeader bool   // if true, this server is not leader; o.w. this server is leader.
	Err         Err    // error message
	Value       string // return value of get command
	TimeId      int64  // identifier (timestamp) for each put/append/get/updateState command
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clientId   int64 // unique identifier (a random generated number) for itself.
	mck        *shardmaster.Clerk
	config     *shardmaster.Config
	pause_flag int        // control not to serve client during migration
	pause_lock sync.Mutex // mutex for pause_flag

	kvs map[string]string     // key-value pairs for client
	chs map[int64]chan Result // clientID-channel pairs for command
	// RPCs (Get,PutAppend) wait on each channel for result.
	rs map[int64]Result // clientID-result pairs for command,
	// store the result of last applied command for each client.
	migrate_ch chan MoveShardArgs // the migration shards information.
	persister  *raft.Persister    // persister to store server state.
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	r := kv.waitForCmd(Op{args.Key, "", shardmaster.Config{}, map[string]string{},
		map[int64]Result{}, GET, args.ClientId, args.TimeId})
	reply.WrongLeader = r.WrongLeader
	reply.Err = r.Err
	reply.Value = r.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var r Result
	if args.Op == "Put" {
		r = kv.waitForCmd(Op{args.Key, args.Value, shardmaster.Config{}, map[string]string{},
			map[int64]Result{}, PUT, args.ClientId, args.TimeId})
	} else if args.Op == "Append" {
		r = kv.waitForCmd(Op{args.Key, args.Value, shardmaster.Config{}, map[string]string{},
			map[int64]Result{}, APPEND, args.ClientId, args.TimeId})
	}
	//  DPrintf("\twrongleader: %d, err: %s\n", r.WrongLeader, r.Err)
	reply.WrongLeader = r.WrongLeader
	//  DPrintf("\twrongleader: %s\n", reply.WrongLeader)
	reply.Err = r.Err
}

//
// The RPC to receive coming shard for leader.
//
func (kv *ShardKV) MoveShard(args *MoveShardArgs, reply *MoveShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Check whether the server is leader.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	// Check whether the configuration number is larger than current number.
	if args.ConfigNum <= kv.config.Num {
		reply.Err = ErrWrongConfig
		return
	}

	// Move shard information to channel.
	go func() {
		kv.migrate_ch <- *args
	}()

	reply.WrongLeader = false
	reply.Err = OK
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) waitForCmd(op Op) Result {
	// Start command on Raft library.
	//DPrintf("\tbefore start: op: %s\n", op)

	wrongLeaderResult := Result{true, "", "", op.TimeId}
	// Check whether server should pause.
	if op.CmdType != UPDATE_STATE {
		kv.pause_lock.Lock()
		flag := kv.pause_flag
		kv.pause_lock.Unlock()
		if flag == 1 {
			// Return result as wrong leader.
			return wrongLeaderResult
		}
	}

	kv.mu.Lock()
	// Check whether this command was the one executed previously.
	if r, ok := kv.rs[op.ClientId]; ok && r.TimeId == op.TimeId {
		kv.mu.Unlock()
		return r
	}

	// Check whether the server is responsible for the key.
	if op.CmdType != UPDATE_STATE {
		shard := key2shard(op.Key)
		gid := kv.config.Shards[shard]
		if gid != kv.gid {
			kv.mu.Unlock()
			// Return result as wrong group.
			return Result{false, ErrWrongGroup, "", op.TimeId}
		}
	}

	//  DPrintf("\tlock in waitForCmd: %s\n", op)
	//  DPrintf("\tbefore start: %s\n", op)
	_, _, isLeader := kv.rf.Start(op)
	//  DPrintf("\tafter start: %s\n", op)
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

//
// Apply command put or append to key-value pairs.
//
func (kv *ShardKV) putAppend(key string, value string, cmdType int, clientId int64, timeId int64) {
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

	// Check whether server should pause.
	kv.pause_lock.Lock()
	flag := kv.pause_flag
	kv.pause_lock.Unlock()
	if flag == 1 {
		// Check whether server should pause.
		// Return result as wrong leader.
		r = Result{true, "", "", timeId}
	} else {
		// Execute command.
		if cmdType == PUT {
			kv.kvs[key] = value
		} else {
			// cmdType is APPEND.
			if _, ok := kv.kvs[key]; !ok {
				kv.kvs[key] = ""
			}
			kv.kvs[key] += value
		}
	}

	if _, isLeader := kv.rf.GetState(); isLeader {
		if cmdType == APPEND {
			/*      DPrintf("\t append gid:%d shard:%d key:%s value:%s now:%s flag:%d\n",
			        kv.gid, key2shard(key), key, value, kv.kvs[key], kv.pause_flag)
			*/
		}
	}

	// Update result of client's last applied command.
	//  DPrintf("\tbefore time: %d\n", kv.rs[clientId].TimeId)
	if flag != 1 {
		kv.rs[clientId] = r
	}
	// Put result in corresponding client's channel.
	var ch chan Result
	if _, ok := kv.chs[clientId]; !ok {
		kv.chs[clientId] = make(chan Result)
	}
	ch = kv.chs[clientId]

	go func() {
		ch <- r
	}()

	// Persist state.
	kv.persist()
}

//
// Apply command get to key-value pairs.
//
func (kv *ShardKV) get(key string, clientId int64, timeId int64) {
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

	// Check whether server should pause.
	kv.pause_lock.Lock()
	flag := kv.pause_flag
	kv.pause_lock.Unlock()
	if flag == 1 {
		// Check whether server should pause.
		// Return result as wrong leader.
		r = Result{true, "", "", timeId}
	} else {
		// Execute command.
		if _, ok := kv.kvs[key]; !ok {
			r.Err = ErrNoKey
		} else {
			r.Err = OK
			r.Value = kv.kvs[key]
		}
	}

	// Update result of client's last applied command.
	//  DPrintf("\tbefore time: %d\n", kv.rs[clientId].TimeId)
	if flag != 1 {
		kv.rs[clientId] = r
	}

	// Put result in corresponding client's channel.
	var ch chan Result
	if _, ok := kv.chs[clientId]; !ok {
		kv.chs[clientId] = make(chan Result)
	}
	ch = kv.chs[clientId]

	go func() {
		ch <- r
	}()

	// Persist state.
	kv.persist()
}

//
// Update state on this server.
//
func (kv *ShardKV) updateState(config shardmaster.Config, kvs map[string]string,
	rs map[int64]Result, clientId int64, timeId int64) {
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
	// (1). Update configuration.
	kv.config.Num = config.Num
	//DPrintf("\tserver:%d, gid:%d, confignum:%d\n", kv.me, kv.gid, kv.config.Num)
	for i := 0; i < len(config.Shards); i++ {
		kv.config.Shards[i] = config.Shards[i]
	}
	kv.config.Groups = map[int][]string{}
	for k, v := range config.Groups {
		kv.config.Groups[k] = v
	}

	// (2).Update key-value pairs.
	for k, v := range kvs {
		kv.kvs[k] = v
	}

	// (3).Update client's last command result.
	for k, v := range rs {
		// Only update if it is a newer result.
		if r, ok := kv.rs[k]; !ok || r.TimeId < v.TimeId {
			kv.rs[k] = v
		}
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

	// Persist state.
	kv.persist()
}

//
// Apply command from channel of Raft library.
//
func (kv *ShardKV) applyCmd() {
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
		case UPDATE_STATE:
			kv.updateState(op.Config, op.KVs, op.Rs, op.ClientId, op.TimeId)
		default:
			DPrintf("Get wrong command: %s\n", msg)
		}
	}
}

//
// Check configuration update.
//
func (kv *ShardKV) checkConfig() {
	for {
		// Wait for next check timeout.
		time.Sleep(GET_CONFIG_TIMEOUT_MS * time.Millisecond)

		// Only leader checks configuration, other servers don't check.
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}

		// Ask shardmaster for the latest configuration.
		config := kv.mck.Query(-1)

		kv.mu.Lock()
		prevConfigNum := kv.config.Num
		kv.mu.Unlock()

		//DPrintf("here[1]: config.Num: %d\n", config.Num)
		if config.Num <= prevConfigNum {
			continue
		}

		// Begin update, refuse to accept any more client operations.
		kv.pause_lock.Lock()
		kv.pause_flag = 1
		kv.pause_lock.Unlock()

		DPrintf("\tnewConfig: %+v, oldConfig:%d, gid:%d, map:%+v\n",
			config, kv.config, kv.gid, kv.kvs)

		if prevConfigNum == 0 /*&& config.Num == 1*/ {
			//DPrintf("here: config.Num:%d, prev:%d\n", config.Num, prevConfigNum)
			// If it's the first configuration,
			// no need to move shards, just update configuration for each server.
			kv.waitForCmd(Op{"", "", config, map[string]string{}, map[int64]Result{},
				UPDATE_STATE, kv.clientId, time.Now().UnixNano()})

			// After configuration is updated on each server,
			// resume server to accept client operations.
			kv.pause_lock.Lock()
			kv.pause_flag = 0
			kv.pause_lock.Unlock()
			//DPrintf("Done!\n")
		} else {
			//DPrintf("other: config.Num:%d, prev:%d\n", config.Num, prevConfigNum)
			kv.mu.Lock()
			// 1.First move shards to other servers.
			for i, g := range config.Shards {
				if kv.config.Shards[i] == kv.gid && g != kv.gid {
					// Copy key-value map and client-result map.
					copyKvs := make(map[string]string)
					for k, v := range kv.kvs {
						if key2shard(k) == i {
							copyKvs[k] = v
						}
					}
					copyRs := make(map[int64]Result)
					for k, v := range kv.rs {
						copyRs[k] = v
					}
					args := MoveShardArgs{config.Num, i, copyKvs, copyRs, kv.clientId,
						time.Now().UnixNano()}
					servers := config.Groups[g]
					for i := 0; ; i = (i + 1) % len(servers) {
						var reply MoveShardReply
						srv := kv.make_end(servers[i])
						// Ask that group's leader to receive this shard.
						// Release the lock first to avoid deadlock.
						kv.mu.Unlock()
						ok := srv.Call("ShardKV.MoveShard", &args, &reply)
						// Acquire lock again.
						kv.mu.Lock()
						if ok && !reply.WrongLeader && reply.Err == OK {
							DPrintf("\t[move] shard %d in config %d success, gid:%d, copyKvs:%+v, mykvs:%+v\n",
								args.ShardNum, args.ConfigNum, kv.gid, copyKvs, kv.kvs)
							break
						}
					}
				}
			}

			// 2.Second wait shards to be moved here.
			newShards := make(map[int]bool)
			for i, g := range config.Shards {
				if kv.config.Shards[i] != kv.gid && g == kv.gid {
					DPrintf("\tshould get shard %d in config %d, gid:%d\n",
						i, config.Num, kv.gid)
					newShards[i] = true
				}
			}

			newKvs := make(map[string]string)
			newRs := make(map[int64]Result)

			for len(newShards) > 0 {
				// Release the lock first to avoid deadlock.
				kv.mu.Unlock()
				// Wait for a new migration shard.
				r := <-kv.migrate_ch
				// Acquire lock again.
				kv.mu.Lock()
				// Check configuration number.
				/*
				   if r.ConfigNum < config.Num {
				     // (1).r.ConfigNum < config.Num: ignore.
				     continue
				   } else*/if r.ConfigNum > config.Num {
					// (2).r.ConfigNum > config.Num: put it back to channel for future use.
					go func() {
						kv.migrate_ch <- r
					}()
					continue
				} else {
					// (3).r.ConfigNum == config.Num: store shard's key-value and client-result pairs.
					for k, v := range r.Kvs {
						newKvs[k] = v
					}
					for k, v := range r.Rs {
						newRs[k] = v
					}
					DPrintf("\tget shard %d in config %d, gid: %d, kvs:%+v\n",
						r.ShardNum, r.ConfigNum, kv.gid, r.Kvs)
					delete(newShards, r.ShardNum)
				}
			}

			// 3.Update configuration, key-value pairs and client's last command result.
			// Release the lock first to avoid deadlock.
			kv.mu.Unlock()

			kv.waitForCmd(Op{"", "", config, newKvs, newRs,
				UPDATE_STATE, kv.clientId, time.Now().UnixNano()})

			// After configuration is updated on each server,
			// resume server to accept client operations.
			kv.pause_lock.Lock()
			kv.pause_flag = 0
			kv.pause_lock.Unlock()
		}
	}
}

//
// Save server's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
//
func (kv *ShardKV) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.config)
	e.Encode(kv.kvs)
	e.Encode(kv.rs)
	data := w.Bytes()
	kv.persister.SaveSnapshot(data)
}

//
// Restore previously persisted state.
//
func (kv *ShardKV) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.config)
	d.Decode(&kv.kvs)
	d.Decode(&kv.rs)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	Debug = 0
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	// initialize from state persisted before a crash
	kv.readPersist(persister.ReadSnapshot())
	kv.persister = persister
	// Initialize state if never persisted before.
	if kv.config == nil {
		kv.config = &shardmaster.Config{}
		kv.config.Num = 0
		for i := 0; i < len(kv.config.Shards); i++ {
			kv.config.Shards[i] = 0
		}
		kv.config.Groups = make(map[int][]string)
	}
	if kv.kvs == nil {
		kv.kvs = make(map[string]string)
	}
	if kv.rs == nil {
		kv.rs = make(map[int64]Result)
	}

	kv.chs = make(map[int64]chan Result)
	kv.migrate_ch = make(chan MoveShardArgs)
	kv.clientId = nrand()
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.pause_flag = 0

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Use one goroutine to apply command per server.
	go kv.applyCmd()

	// Use one goroutine to check configuration update.
	go kv.checkConfig()

	return kv
}
