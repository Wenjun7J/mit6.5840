package kvsrv

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	id    int64
	seqid int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.id = nrand()
	ck.seqid = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{Key: key, ClerkId: ck.id, Seqid: ck.seqid + 1}
	reply := GetReply{}
	for {
		if ok := ck.server.Call("KVServer.Get", &args, &reply); !ok {
			//fmt.Printf("call Get failed %v %v, %v %v\n", ck.id, ck.seqid, ok, reply.OK)
		} else {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, ClerkId: ck.id, Seqid: ck.seqid}
	reply := PutAppendReply{}
	for {
		if ok := ck.server.Call("KVServer."+op, &args, &reply); !ok {
			//fmt.Printf("call PutAppend failed %v %v, %v %v\n", ck.id, ck.seqid, ok, reply.OK)
		} else {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ck.seqid++
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
