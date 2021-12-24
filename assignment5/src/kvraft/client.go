package raftkv

import (
	"assign5/labrpc"
	"crypto/rand"
	"log"
	"math/big"
	"sync/atomic"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId  int32
	clientId  int64
	requestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 4)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.requestId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	reqId := atomic.AddInt64(&ck.requestId, 1)
	leader := atomic.LoadInt32(&ck.leaderId)

	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: reqId,
	}

	value := ""
	server := leader
	for ; ; server = (server + 1) % int32(len(ck.servers)) {
		log.Printf("Client %d calls Get on RaftKV %d\n", ck.clientId, server)

		reply := GetReply{}
		ok := ck.servers[server].Call("RaftKV.Get", &args, &reply)

		if ok && reply.Err != ErrWrongLeader {
			if reply.Err == OK {
				value = reply.Value
			}
			break
		}
	}

	atomic.StoreInt32(&ck.leaderId, server)

	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	reqId := atomic.AddInt64(&ck.requestId, 1)
	leader := atomic.LoadInt32(&ck.leaderId)

	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: reqId,
	}

	server := leader
	for ; ; server = (server + 1) % int32(len(ck.servers)) {
		log.Printf("Client %d calls PutAppend on RaftKV %d\n", ck.clientId, server)

		reply := PutAppendReply{}
		ok := ck.servers[server].Call("RaftKV.PutAppend", &args, &reply)

		if ok && reply.Err != ErrWrongLeader {
			break
		}
	}

	atomic.StoreInt32(&ck.leaderId, server)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
