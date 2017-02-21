package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId     int64 // unique identifier (a random generated number) for each client.
	lastServerId int   // index of last server from which client got result.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.lastServerId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	args.TimeId = time.Now().UnixNano()
	DPrintf("\tclient query: %+v\n", num)
	for {
		// try each known server.
		for i := ck.lastServerId; ; i = (i + 1) % len(ck.servers) {
			var reply QueryReply
			ok := ck.servers[i].Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastServerId = i
				return reply.Config
			} else {
				//DPrintf("\twrong server:%d\n", i)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	args.TimeId = time.Now().UnixNano()

	DPrintf("\tclient join: %+v\n", args.Servers)
	for {
		// try each known server.
		for i := ck.lastServerId; ; i = (i + 1) % len(ck.servers) {
			// DPrintf("\tclient join: %+v, server:%d\n", args.Servers, i)
			var reply JoinReply
			ok := ck.servers[i].Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastServerId = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.TimeId = time.Now().UnixNano()

	DPrintf("\tclient leave: %+v\n", args.GIDs)
	for {
		// try each known server.
		for i := ck.lastServerId; ; i = (i + 1) % len(ck.servers) {
			var reply LeaveReply
			ok := ck.servers[i].Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastServerId = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.TimeId = time.Now().UnixNano()

	DPrintf("\tclient move: %+v\n", args)
	for {
		// try each known server.
		for i := ck.lastServerId; ; i = (i + 1) % len(ck.servers) {
			var reply MoveReply
			ok := ck.servers[i].Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.lastServerId = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
