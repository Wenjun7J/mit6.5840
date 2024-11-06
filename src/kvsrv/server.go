package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data        sync.Map
	seqid       sync.Map
	reply_cache sync.Map
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if seqId, ok := kv.seqid.Load(args.ClerkId); ok {
		if args.Seqid > seqId.(int) {
			kv.reply_cache.Delete(args.ClerkId)
		}
	}
	val, _ := kv.data.LoadOrStore(args.Key, "")
	reply.Value = val.(string)
}

// func bToMb(b uint64) uint64 {
// 	return b / 1024 / 1024
// }
// func printMemStats() {
// 	var m runtime.MemStats
// 	runtime.ReadMemStats(&m)

// 	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
// 	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
// 	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
// 	fmt.Printf("\tNumGC = %v\n", m.NumGC)
// }

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	seqId, _ := kv.seqid.LoadOrStore(args.ClerkId, 0)
	if args.Seqid == seqId.(int) {
		kv.data.Store(args.Key, args.Value)
		kv.seqid.Store(args.ClerkId, seqId.(int)+1)
		kv.reply_cache.Delete(args.ClerkId)
	} else if args.Seqid == seqId.(int)-1 {
		//do nothing
	} else {
		//something oops...
	}
	//fmt.Printf("put: %v %v %v (%v : %v)\n", args.ClerkId, args.Seqid, seqId, args.Key, args.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	seqId, _ := kv.seqid.LoadOrStore(args.ClerkId, 0)
	if args.Seqid == seqId.(int) {
		val, _ := kv.data.LoadOrStore(args.Key, "")
		reply.Value = val.(string)
		kv.reply_cache.Store(args.ClerkId, val)
		kv.seqid.Store(args.ClerkId, seqId.(int)+1)
		kv.data.Store(args.Key, reply.Value+args.Value)
	} else if args.Seqid == seqId.(int)-1 {
		val, _ := kv.reply_cache.Load(args.ClerkId)
		reply.Value = val.(string)
	} else {
		//something oops...
	}
	//fmt.Printf("append: %v %v %v (%v : %v)\n", args.ClerkId, args.Seqid, seqId, args.Key, args.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.

	return kv
}
