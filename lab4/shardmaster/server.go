package shardmaster

import "raft"
import "labrpc"
import "sync"
import "encoding/gob"
import "time"
import "log"

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
	JOIN = iota
	LEAVE
	MOVE
	QUERY
)

//
// Timer to check server state (in milliseconds).
//
const GET_STATE_TIMEOUT_MS = 100

// Command passed to raft server.
type Op struct {
	// Your data here.
	// Args for this join/leave/move/query command.
	Servers  map[int][]string // new GID -> servers mappings
	GIDs     []int
	Shard    int
	GID      int
	Num      int   // desired config number
	ClientId int64 // identifier (client ID) for this command
	TimeId   int64 // identifier (timestamp) for this command,
	// i.e. the number of nanoseconds elapsed since January 1, 1970 UTC
	// ClientId and TimeId together uniquely identify a command
	CmdType int // JOIN or LEAVE or MOVE or QUERY.
}

// Command's execution result maintained by sharded master itself.
type Result struct {
	Reply  interface{}
	TimeId int64 // identifier (timestamp) for this command.
}

// Shards per replica group.
type GroupShards struct {
	gid    int
	shards []int
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	gs  []GroupShards         // Shards per replica group.
	chs map[int64]chan Result // clientID-channel pairs for command
	// RPCs (Join,Leave,Move,Query) wait on each channel for result.
	rs map[int64]Result // clientID-result pairs for command,
	// store the result of last applied command for each client.

	configs []Config // indexed by config num
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{map[int][]string{}, nil, -1, -1, -1, args.ClientId, args.TimeId, JOIN}
	for k, v := range args.Servers {
		op.Servers[k] = v
	}
	r := sm.waitForCmd(op)
	*reply = r.Reply.(JoinReply)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{map[int][]string{}, nil, -1, -1, -1, args.ClientId, args.TimeId, LEAVE}
	op.GIDs = make([]int, len(args.GIDs))
	copy(op.GIDs, args.GIDs)
	r := sm.waitForCmd(op)
	*reply = r.Reply.(LeaveReply)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{map[int][]string{}, nil, args.Shard, args.GID, -1, args.ClientId, args.TimeId, MOVE}
	r := sm.waitForCmd(op)
	*reply = r.Reply.(MoveReply)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{map[int][]string{}, nil, -1, -1, args.Num, args.ClientId, args.TimeId, QUERY}
	r := sm.waitForCmd(op)
	*reply = r.Reply.(QueryReply)
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// Wait for command to be committed and executed.
//
func (sm *ShardMaster) waitForCmd(op Op) Result {
	// Start command on Raft library.
	//DPrintf("\tbefore start: op: %s\n", op)
	sm.mu.Lock()

	// Get clientId and timeId, and construct wrongLeaderResult.
	var wrongLeaderResult Result
	clientId := op.ClientId
	timeId := op.TimeId
	switch op.CmdType {
	case JOIN:
		wrongLeaderResult = Result{JoinReply{true, ""}, timeId}
	case LEAVE:
		wrongLeaderResult = Result{LeaveReply{true, ""}, timeId}
	case MOVE:
		wrongLeaderResult = Result{MoveReply{true, ""}, timeId}
	case QUERY:
		wrongLeaderResult = Result{QueryReply{true, "", Config{}}, timeId}
	default:
		//DPrintf("Get wrong command: %+v\n", op)
		return Result{}
	}

	// Check whether this command was the one executed previously.
	if r, ok := sm.rs[clientId]; ok && r.TimeId == timeId {
		sm.mu.Unlock()
		return r
	}
	//  DPrintf("\tlock in waitForCmd: %s\n", op)
	//  DPrintf("\tbefore start: %s\n", op)
	_, _, isLeader := sm.rf.Start(op)
	//  DPrintf("\tafter start: %s\n", op)
	//DPrintf("\tstart: op: %s\n", op)
	if !isLeader {
		//    DPrintf("\tnot leader: %d\n", kv.me)
		// This server is not leader.
		sm.mu.Unlock()
		//    DPrintf("\tunlock in waitForCmd: !isleader\n")
		return wrongLeaderResult
	} else {
		// Create channel if not available.
		var ch chan Result
		if _, ok := sm.chs[clientId]; !ok {
			sm.chs[clientId] = make(chan Result)
		}
		ch = sm.chs[clientId]
		sm.mu.Unlock()
		//    DPrintf("\tunlock in waitForCmd: else isleader: %s\n", op)

		// Periodically check this server is still leader,
		// and wait for result.
		for {
			if _, isLeader := sm.rf.GetState(); !isLeader {
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
				if r.TimeId > timeId {
					//          DPrintf("\tRETURN to queue\n")
					go func() {
						ch <- r
					}()
				} else if r.TimeId == timeId {
					//          DPrintf("\tRETURN: %d!!!\n", timeId)
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
// Function to execute join on shardmaster.
//
func (sm *ShardMaster) join(op Op) {
	// Your code here.
	//  DPrintf("\tget before lock, key:%s, clientId:%d, timeId:%s\n", key, clientId, timeId)
	sm.mu.Lock()
	//  DPrintf("\tget after lock, key:%s, clientId:%d, timeId:%s\n", key, clientId, timeId)
	defer sm.mu.Unlock()

	clientId := op.ClientId
	timeId := op.TimeId

	// Check whether this command was executed before.
	if r, ok := sm.rs[clientId]; ok && r.TimeId >= timeId {
		// If this command was executed before, don't execute again.
		//    DPrintf("\tget run before: key:%s, clientId:%d, timeId:%s, now:%s\n", key, clientId, timeId, r.TimeId)
		return
	}

	// Execute command.
	r := Result{JoinReply{false, OK}, timeId}
	var moveShards []int
	var prevGSIdx []int
	currConfig := &sm.configs[len(sm.configs)-1]

	// 1.Create a new configuration.
	var newConfig Config
	newConfig.Num = currConfig.Num + 1
	copy(newConfig.Shards[:], currConfig.Shards[:])
	newConfig.Groups = map[int][]string{}
	for k, v := range currConfig.Groups {
		newConfig.Groups[k] = v
	}

	if _, isLeader := sm.rf.GetState(); isLeader {
		DPrintf("new join: %+v, server:%d\n", op.Servers, sm.me)
		DPrintf("[before] gs:%+v, shards:%+v, groups(len:%d):%+v\n", sm.gs, currConfig.Shards,
			len(currConfig.Groups), currConfig.Groups)
	}

	// 2.Filter out existed groups in arguments.
	newGroups := map[int][]string{}
	for k, v := range op.Servers {
		if currConfig.Groups[k] == nil {
			newGroups[k] = v
		}
	}

	shardsPerGroup := NShards / (len(currConfig.Groups) + len(newGroups))
	if shardsPerGroup == 0 {
		shardsPerGroup = 1
	}

	if sm.gs == nil {
		for i := 0; i < NShards; i++ {
			moveShards = append(moveShards, i)
			prevGSIdx = append(prevGSIdx, -1)
		}
	} else {
		for i, g := range sm.gs {
			for j := shardsPerGroup; j < len(g.shards); j++ {
				moveShards = append(moveShards, g.shards[j])
				prevGSIdx = append(prevGSIdx, i)
			}
			// Move shards.
			if len(sm.gs[i].shards) > 0 {
				sm.gs[i].shards = sm.gs[i].shards[:shardsPerGroup]
			}
		}
	}
	/*
	   DPrintf("shardsPerGroup:%d, moveShards:%+v, prevGSIdx:%+v\n",
	     shardsPerGroup, moveShards, prevGSIdx)
	*/
	// 3.Move shards to new groups.
	for gid, servers := range newGroups {
		var gs GroupShards
		gs.gid = gid
		i := 0
		for ; i < shardsPerGroup && i < len(moveShards); i++ {
			s := moveShards[i]
			newConfig.Shards[s] = gid
			gs.shards = append(gs.shards, s)
		}

		sm.gs = append(sm.gs, gs)
		newConfig.Groups[gid] = servers
		//DPrintf("shardsPerGroup:%d, moveShards:%+v, prevGSIdx:%+v\n",
		//shardsPerGroup, moveShards, prevGSIdx)
		if len(moveShards) > 0 {
			moveShards = moveShards[i:len(moveShards)]
			prevGSIdx = prevGSIdx[i:len(prevGSIdx)]
		}
	}

	// 4.Restore rest shards back to previous group (move as few shards as possible).
	for i, s := range moveShards {
		sm.gs[prevGSIdx[i]].shards = append(sm.gs[prevGSIdx[i]].shards, s)
	}

	//DPrintf("sm.gs: %+v, shards: %+v, groups: %+v\n\n", sm.gs, newConfig.Shards, newConfig.Groups)

	// 5.Append new configuration.
	sm.configs = append(sm.configs, newConfig)
	//DPrintf("sm.configs[%d]: num:%d, shards:%+v, groups:%+v\n",
	//  len(sm.configs)-1, sm.configs[len(sm.configs)-1].Num,
	//  sm.configs[len(sm.configs)-1].Shards, sm.configs[len(sm.configs)-1].Groups)
	if _, isLeader := sm.rf.GetState(); isLeader {
		DPrintf("[after] gs:%+v, shards:%+v, groups(len:%d):%+v\n\n", sm.gs, newConfig.Shards,
			len(newConfig.Groups), newConfig.Groups)
	}

	// Update result of client's last applied command.
	//  DPrintf("\tbefore time: %d\n", kv.rs[clientId].TimeId)
	sm.rs[clientId] = r

	// Put result in corresponding client's channel.
	var ch chan Result
	if _, ok := sm.chs[clientId]; !ok {
		sm.chs[clientId] = make(chan Result)
	}
	ch = sm.chs[clientId]

	go func() {
		ch <- r
	}()
}

//
// Function to execute leave on shardmaster.
//
func (sm *ShardMaster) leave(op Op) {
	// Your code here.
	//  DPrintf("\tget before lock, key:%s, clientId:%d, timeId:%s\n", key, clientId, timeId)
	sm.mu.Lock()
	//  DPrintf("\tget after lock, key:%s, clientId:%d, timeId:%s\n", key, clientId, timeId)
	defer sm.mu.Unlock()

	clientId := op.ClientId
	timeId := op.TimeId

	// Check whether this command was executed before.
	if r, ok := sm.rs[clientId]; ok && r.TimeId >= timeId {
		// If this command was executed before, don't execute again.
		//    DPrintf("\tget run before: key:%s, clientId:%d, timeId:%s, now:%s\n", key, clientId, timeId, r.TimeId)
		return
	}

	// Execute command.
	//DPrintf("\tleave: GIDs: %+v\n", op.GIDs)
	r := Result{LeaveReply{false, OK}, timeId}
	currConfig := &sm.configs[len(sm.configs)-1]

	if _, isLeader := sm.rf.GetState(); isLeader {
		DPrintf("new leave: %+v, server:%d\n", op.GIDs, sm.me)
		DPrintf("[before] gs:%+v, shards:%+v, groups(len:%d):%+v\n", sm.gs, currConfig.Shards,
			len(currConfig.Groups), currConfig.Groups)
	}

	// 1.Create a new configuration.
	var newConfig Config
	newConfig.Num = currConfig.Num + 1
	copy(newConfig.Shards[:], currConfig.Shards[:])
	newConfig.Groups = map[int][]string{}
	for k, v := range currConfig.Groups {
		newConfig.Groups[k] = v
	}

	if sm.gs != nil {
		var moveShards []int
		//DPrintf("\t[before] newConfig.Shards:%+v\n", newConfig.Shards)
		// 2.Find out which shards need to be moved,
		// and remove groups.
		for _, leaveGID := range op.GIDs {
			for i := 0; i < len(sm.gs); i++ {
				if sm.gs[i].gid == leaveGID {
					for _, s := range sm.gs[i].shards {
						moveShards = append(moveShards, s)
						newConfig.Shards[s] = 0
					}
					delete(newConfig.Groups, leaveGID)
					sm.gs = append(sm.gs[:i], sm.gs[i+1:]...)
					break
				}
			}
		}
		/*
		   DPrintf("\tnewConfig.Shards:%+v, sm.gs:%+v\n",
		     newConfig.Shards, sm.gs)
		*/
		// 3.Move shards to new groups.
		if len(newConfig.Groups) > 0 {
			shardsPerGroup := NShards / len(newConfig.Groups)
			if shardsPerGroup == 0 {
				shardsPerGroup = 1
			}
			//      DPrintf("shardsPerGroup:%d\n", shardsPerGroup)
			i := 0
			for len(moveShards) > 0 && i < len(sm.gs) {
				currShardsNum := len(sm.gs[i].shards)
				addShardsNum := shardsPerGroup - currShardsNum
				if addShardsNum > 0 {
					addShards := moveShards[:addShardsNum]
					for _, s := range addShards {
						sm.gs[i].shards = append(sm.gs[i].shards, s)
						newConfig.Shards[s] = sm.gs[i].gid
					}
					moveShards = moveShards[addShardsNum:]
				}
				i++
			}
			// Move rest shards to first group.
			if len(moveShards) > 0 {
				for _, s := range moveShards {
					sm.gs[0].shards = append(sm.gs[0].shards, s)
					newConfig.Shards[s] = sm.gs[0].gid
				}
			}
		}
	}
	//  DPrintf("\n")
	// 4.Append new configuration.
	sm.configs = append(sm.configs, newConfig)

	if _, isLeader := sm.rf.GetState(); isLeader {
		DPrintf("[after] gs:%+v, shards:%+v, groups(len:%d):%+v\n\n", sm.gs, newConfig.Shards,
			len(newConfig.Groups), newConfig.Groups)
	}

	/* DPrintf("sm.configs[%d]: num:%d, shards:%+v, groups:%+v\n",
	   len(sm.configs)-1, sm.configs[len(sm.configs)-1].Num,
	   sm.configs[len(sm.configs)-1].Shards, sm.configs[len(sm.configs)-1].Groups)
	*/
	// Update result of client's last applied command.
	//  DPrintf("\tbefore time: %d\n", kv.rs[clientId].TimeId)
	sm.rs[clientId] = r

	// Put result in corresponding client's channel.
	var ch chan Result
	if _, ok := sm.chs[clientId]; !ok {
		sm.chs[clientId] = make(chan Result)
	}
	ch = sm.chs[clientId]

	go func() {
		ch <- r
	}()
}

//
// Function to execute move on shardmaster.
//
func (sm *ShardMaster) move(op Op) {
	// Your code here.
	//  DPrintf("\tget before lock, key:%s, clientId:%d, timeId:%s\n", key, clientId, timeId)
	sm.mu.Lock()
	//  DPrintf("\tget after lock, key:%s, clientId:%d, timeId:%s\n", key, clientId, timeId)
	defer sm.mu.Unlock()

	clientId := op.ClientId
	timeId := op.TimeId

	// Check whether this command was executed before.
	if r, ok := sm.rs[clientId]; ok && r.TimeId >= timeId {
		// If this command was executed before, don't execute again.
		//    DPrintf("\tget run before: key:%s, clientId:%d, timeId:%s, now:%s\n", key, clientId, timeId, r.TimeId)
		return
	}

	// Execute command.
	//DPrintf("new join: %+v\n", args.Servers)
	r := Result{MoveReply{false, OK}, timeId}
	currConfig := &sm.configs[len(sm.configs)-1]

	// 1.Create a new configuration.
	var newConfig Config
	newConfig.Num = currConfig.Num + 1
	copy(newConfig.Shards[:], currConfig.Shards[:])
	newConfig.Groups = map[int][]string{}
	for k, v := range currConfig.Groups {
		newConfig.Groups[k] = v
	}

	// 2.Move shard from previous group.
	prevGID := newConfig.Shards[op.Shard]
find:
	for i := 0; i < len(sm.gs); i++ {
		if sm.gs[i].gid == prevGID {
			for j, s := range sm.gs[i].shards {
				if s == op.Shard {
					sm.gs[i].shards = append(sm.gs[i].shards[:j], sm.gs[i].shards[j+1:]...)
					break find
				}
			}
		}
	}

	// 3.Move shards to new group.
	newConfig.Shards[op.Shard] = op.GID
	for i := 0; i < len(sm.gs); i++ {
		if sm.gs[i].gid == op.GID {
			sm.gs[i].shards = append(sm.gs[i].shards, op.Shard)
			break
		}
	}

	// 4.Append new configuration.
	sm.configs = append(sm.configs, newConfig)
	/*  DPrintf("sm.configs[%d]: num:%d, shards:%+v, groups:%+v\n",
	    len(sm.configs)-1, sm.configs[len(sm.configs)-1].Num,
	    sm.configs[len(sm.configs)-1].Shards, sm.configs[len(sm.configs)-1].Groups)
	*/
	// Update result of client's last applied command.
	//  DPrintf("\tbefore time: %d\n", kv.rs[clientId].TimeId)
	sm.rs[clientId] = r

	// Put result in corresponding client's channel.
	var ch chan Result
	if _, ok := sm.chs[clientId]; !ok {
		sm.chs[clientId] = make(chan Result)
	}
	ch = sm.chs[clientId]

	go func() {
		ch <- r
	}()
}

//
// Function to execute query on shardmaster.
//
func (sm *ShardMaster) query(op Op) {
	// Your code here.
	//  DPrintf("\tget before lock, key:%s, clientId:%d, timeId:%s\n", key, clientId, timeId)
	sm.mu.Lock()
	//  DPrintf("\tget after lock, key:%s, clientId:%d, timeId:%s\n", key, clientId, timeId)
	defer sm.mu.Unlock()

	clientId := op.ClientId
	timeId := op.TimeId

	// Check whether this command was executed before.
	if r, ok := sm.rs[clientId]; ok && r.TimeId >= timeId {
		// If this command was executed before, don't execute again.
		//    DPrintf("\tget run before: key:%s, clientId:%d, timeId:%s, now:%s\n", key, clientId, timeId, r.TimeId)
		return
	}

	// Execute command.
	var config Config
	if op.Num == -1 || op.Num >= len(sm.configs) {
		config = sm.configs[len(sm.configs)-1]
	} else {
		config = sm.configs[op.Num]
	}
	r := Result{QueryReply{false, OK, config}, timeId}

	// Update result of client's last applied command.
	//  DPrintf("\tbefore time: %d\n", kv.rs[clientId].TimeId)
	sm.rs[clientId] = r

	// Put result in corresponding client's channel.
	var ch chan Result
	if _, ok := sm.chs[clientId]; !ok {
		sm.chs[clientId] = make(chan Result)
	}
	ch = sm.chs[clientId]

	go func() {
		ch <- r
	}()
}

//
// Apply command from channel of Raft library.
//
func (sm *ShardMaster) applyCmd() {
	for {
		msg := <-sm.applyCh
		//    DPrintf("get command: %+v\n", msg)
		if msg.Command == nil {
			continue
		}
		op := msg.Command.(Op)
		//    DPrintf("server: %d After get command: %s\n", kv.me, op)
		switch op.CmdType {
		case JOIN:
			sm.join(op)
		case LEAVE:
			sm.leave(op)
		case MOVE:
			sm.move(op)
		case QUERY:
			sm.query(op)
		default:
			//DPrintf("Get wrong command: %s\n", msg)
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	Debug = 0
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.chs = make(map[int64]chan Result)
	sm.rs = make(map[int64]Result)

	// Use one goroutine to apply command per server.
	go sm.applyCmd()

	return sm
}
