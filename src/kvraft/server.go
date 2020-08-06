package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Method    string
	ClientId  int64
	RequestId int
}

type Response struct {
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db                  map[string]string
	lastAppliedReqIdMap map[int64]int
	responseChanMap     map[int]chan Response
}

func (kv *KVServer) isDuplicateRequest(clientId int64, requestId int) bool {
	appliedReqId, ok := kv.lastAppliedReqIdMap[clientId]
	if !ok || appliedReqId < requestId {
		return false
	}
	return true
}

func (kv *KVServer) waitForRaft(op Op) (success bool) {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	if _, ok := kv.responseChanMap[index]; !ok {
		kv.responseChanMap[index] = make(chan Response, 1)
	}
	ch := kv.responseChanMap[index]
	kv.mu.Unlock()

	success = true
	select {
	case resp := <-ch:
		if resp.ClientId != op.ClientId || resp.RequestId != op.RequestId {
			success = false
		}
	case <-time.After(500 * time.Millisecond):
		kv.mu.Lock()
		if !kv.isDuplicateRequest(op.ClientId, op.RequestId) {
			success = false
		}
		kv.mu.Unlock()
	}
	kv.mu.Lock()
	delete(kv.responseChanMap, index)
	kv.mu.Unlock()
	return success
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key:       args.Key,
		Method:    "Get",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	success := kv.waitForRaft(op)
	if !success {
		reply.Err = ErrWrongLeader
	} else {
		kv.mu.Lock()
		value, ok := kv.db[args.Key]
		kv.mu.Unlock()
		if ok {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Method:    args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	success := kv.waitForRaft(op)
	if !success {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.lastAppliedReqIdMap = make(map[int64]int)
	kv.responseChanMap = make(map[int]chan Response)

	go func() {
		for applyMsg := range kv.applyCh {
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			if kv.isDuplicateRequest(op.ClientId, op.RequestId) {
				kv.mu.Unlock()
				continue
			}
			switch op.Method {
			case "Put":
				kv.db[op.Key] = op.Value
			case "Append":
				kv.db[op.Key] += op.Value
			case "Get":
				// do nothing
			}
			kv.lastAppliedReqIdMap[op.ClientId] = op.RequestId

			if ch, ok := kv.responseChanMap[applyMsg.CommandIndex]; ok {
				resp := Response{
					ClientId:  op.ClientId,
					RequestId: op.RequestId,
				}
				ch <- resp
			}
			kv.mu.Unlock()
			DPrintf("kvServer %d applied %s at index=%d, requestId=%d, clientId=%d",
				kv.me, op.Method, applyMsg.CommandIndex, op.RequestId, op.ClientId)
		}
	}()

	return kv
}
