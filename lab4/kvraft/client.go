package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.lastServerId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	// Create args and reply.
	timeId := time.Now().UnixNano()
	args := GetArgs{key, ck.clientId, timeId}

	// Ask each server to execute command.
	for i := ck.lastServerId; ; i = (i + 1) % len(ck.servers) {
		reply := new(GetReply)
		DPrintf("client(%d) call server %d for get key: %s, time(%d)\n", ck.clientId, i, key, timeId)
		ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
		if ok && !reply.WrongLeader {
			// If command was executed successfully,
			// update lastServerId, and return result.
			DPrintf("\tclient(%d) Get finish: value: %s, time(%d)\n", ck.clientId, reply.Value, timeId)
			ck.lastServerId = i
			if reply.Err == ErrNoKey {
				return ""
			} else {
				return reply.Value
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// Create args and reply.
	timeId := time.Now().UnixNano()
	args := PutAppendArgs{key, value, op, ck.clientId, timeId}
	//var reply PutAppendReply

	// Ask each server to execute command.
	for i := ck.lastServerId; ; i = (i + 1) % len(ck.servers) {
		reply := new(PutAppendReply)
		DPrintf("client(%d) call server %d for %s key:%s, value:%s, time(%d)\n", ck.clientId, i, op, key, value, timeId)
		ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
		//    DPrintf("ok: %d, reply: %s reply wrongleader: %t, err: %s\n", ok, reply, reply.WrongLeader, reply.Err)
		if ok && !reply.WrongLeader {
			// If command was executed successfully,
			// update lastServerId, and return.
			DPrintf("\tclient(%d) %s finish, time(%d)\n", ck.clientId, op, timeId)
			ck.lastServerId = i
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
