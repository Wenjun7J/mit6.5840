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
  "sync"
  "sync/atomic"

  "6.5840/labgob"
  "6.5840/labrpc"

// rand
  "math/rand"
  "time"
  "fmt"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
  CommandValid bool
  Command      interface{}
  CommandIndex int

  // For 3D:
  SnapshotValid bool
  Snapshot      []byte
  SnapshotTerm  int
  SnapshotIndex int
}

type LogEntry struct {
  Term int
  Command interface{}
}

type SnapshotEntry struct {
  LastIncludedIndex int
  LastIncludedTerm int
  Snapshot  []byte
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
  mu        sync.Mutex          // Lock to protect shared access to this peer's state
  peers     []*labrpc.ClientEnd // RPC end points of all peers
  persister *Persister          // Object to hold this peer's persisted state
  me        int                 // this peer's index into peers[]
  dead      int32               // set by Kill()
  // Your data here (3A, 3B, 3C).
  // Look at the paper's Figure 2 for a description of what
  // state a Raft server must maintain.

  //3A begin:
  //need persist:
  term int
  voteFor int

  //violate on all
  commitIndex int
  applyIndex int

  //violate on leader, init after elect
  nextIndex []int
  matchIndex []int
  identity  int

  receivedRPCTime time.Time
  electTimeout  int64

  getVoted int
  getVotedMap map[int]bool

  entries []LogEntry

  cond *sync.Cond
  cond2 *sync.Cond

  snapshotEntry SnapshotEntry
}

func (rf *Raft) CommitToService() {
  rf.cond.Signal()
}

func (rf *Raft) SnapshotToService() {
  rf.cond2.Signal()
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

  var term int
  var isleader bool
  // Your code here (3A).
  rf.mu.Lock()
  term = rf.term
  isleader = (rf.identity == 0)
  rf.mu.Unlock()
  return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
  // Your code here (3C).
  // Example:
  // w := new(bytes.Buffer)
  // e := labgob.NewEncoder(w)
  // e.Encode(rf.xxx)
  // e.Encode(rf.yyy)
  // data := w.Bytes()
  // rf.persister.SaveRaftState(data)
  w := new(bytes.Buffer)
  e := labgob.NewEncoder(w)
  e.Encode(rf.term)
  e.Encode(rf.voteFor)
  e.Encode(rf.entries)
  e.Encode(rf.snapshotEntry.LastIncludedIndex)
  e.Encode(rf.snapshotEntry.LastIncludedTerm)
  data := w.Bytes()
  rf.persister.Save(data, rf.snapshotEntry.Snapshot)
}


func (rf *Raft) reset_timeout() {
  rf.receivedRPCTime = time.Now()
  rf.electTimeout = int64(300 + rand.Intn(300))
}

func (rf *Raft) get_last_entry_idx() int {
  return rf.snapshotEntry.LastIncludedIndex + len(rf.entries) - 1;
}

func (rf *Raft) get_last_entry_term() int {
  if (len(rf.entries) == 1) {
    return rf.snapshotEntry.LastIncludedTerm;
  } else {
    return rf.entries[len(rf.entries) - 1].Term;
  }
}

func (rf *Raft) get_entry_term_by_idx(n int) int {
  if (n == 0) {
    return 0
  } else if (n == rf.snapshotEntry.LastIncludedIndex) {
    return rf.snapshotEntry.LastIncludedTerm
  } else {
    k := n - rf.snapshotEntry.LastIncludedIndex
    if (k > 0) {
      return rf.entries[k].Term;
    } else {
      //if out of range, then return 0;
      return 0;
    }
  }
}
//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
  if data == nil || len(data) < 1 { // bootstrap without any state?
    return
  }
  // Your code here (3C).
  // Example:
  // r := bytes.NewBuffer(data)
  // d := labgob.NewDecoder(r)
  // var xxx
  // var yyy
  // if d.Decode(&xxx) != nil ||
  //    d.Decode(&yyy) != nil {
  //   error...
  // } else {
  //   rf.xxx = xxx
  //   rf.yyy = yyy
  // }

  r := bytes.NewBuffer(data)
  d := labgob.NewDecoder(r)
  var term int
  var voteFor int
  var entries []LogEntry
  var lastIncludedIndex int
  var lastIncludedTerm int
  if d.Decode(&term) != nil ||
     d.Decode(&voteFor) != nil ||
     d.Decode(&entries) != nil || 
     d.Decode(&lastIncludedIndex) != nil ||
     d.Decode(&lastIncludedTerm) != nil {
    //error
  } else {
    rf.term = term
    rf.voteFor = voteFor
    rf.entries = entries
    rf.snapshotEntry.LastIncludedIndex = lastIncludedIndex
    rf.snapshotEntry.LastIncludedTerm = lastIncludedTerm
  }
  rf.snapshotEntry.Snapshot = rf.persister.ReadSnapshot()
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

  // Your code here (3D).
  //no need to implement, just return true
  return true
}

type InstallSnapshotArgs struct {
  Term int
  LeaderId int
  LastIncludedIndex int
  LastIncludedTerm int 
  Snapshot []byte
}

type InstallSnapshotReply struct {
  Term int
  OK bool
  LastIncludedIndex int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
  fmt.Printf("[Node: %v] begin install snapshot\n", rf.me);
  rf.mu.Lock()
  if (args.Term > rf.term) {
    rf.identity = 2
    rf.voteFor = -1
    rf.getVoted = 0
    rf.term = args.Term
    rf.reset_timeout()
    reply.OK = false;
    rf.persist()
    //do not install.
    fmt.Printf("[Node: %v]  install snapshot failed because term not same\n", rf.me);
  } else if (args.Term == rf.term) {
    fmt.Printf("[Node: %v]  begin install snapshot  because term are the same\n", rf.me);
    rf.reset_timeout()
    if (rf.identity == 1) {
      rf.identity = 2
    }
    if (args.LastIncludedIndex <= rf.snapshotEntry.LastIncludedIndex) {
      reply.OK = false;
      fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] receive install snapshot msg and received snapshot index is low:  %v\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, args.LastIncludedIndex)
    } else {
      reply.OK = true;
      cut_num := args.LastIncludedIndex - rf.snapshotEntry.LastIncludedIndex
      if (cut_num >= len(rf.entries) - 1) {
        rf.entries = rf.entries[:1] // set empty
      } else {
        var tmp_entries []LogEntry
        tmp_entries = append(tmp_entries, rf.entries[0])
        for i:= cut_num + 1 + 1; i < len(rf.entries); i++ {
          tmp_entries = append(tmp_entries, tmp_entries[i])
        }
        rf.entries = tmp_entries
      }
      if (len(rf.entries) > 1) {
        if (rf.entries[1].Term != args.LastIncludedTerm) {
          rf.entries = rf.entries[:1]
        }
      }
      rf.snapshotEntry.LastIncludedIndex = args.LastIncludedIndex
      rf.snapshotEntry.LastIncludedTerm = args.LastIncludedTerm
      rf.snapshotEntry.Snapshot = args.Snapshot
      rf.persist()
      if (rf.commitIndex < args.LastIncludedIndex) {
        rf.commitIndex = args.LastIncludedIndex
      }

      if (rf.applyIndex < args.LastIncludedIndex) {
        rf.applyIndex = args.LastIncludedIndex
      }
      
      fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] receive install snapshot msg and install/update entries %v\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, len(rf.entries))
      rf.SnapshotToService()
    }
  } else { //args.Term < rf.term
    //invalid leader, do not reset timeout
    fmt.Printf("[Node: %v]  install snapshot failed2 because term not same\n", rf.me);
  }
  reply.Term = rf.term
  reply.LastIncludedIndex = rf.snapshotEntry.LastIncludedIndex
  rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
  var ok bool
  rf.mu.Lock()
  if (rf.identity != 0) {
    //do nothing
  } else {
    fmt.Printf("[Node: %v] send insall snapshot msg to %v\n", rf.me, server);
    rf.mu.Unlock()
    ok = rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
    rf.mu.Lock()
    if (rf.identity != 0) {
    } else if (reply.Term > rf.term) {
      rf.identity = 2
      rf.voteFor = -1
      rf.getVoted = 0
      rf.reset_timeout()
      rf.term = reply.Term
      rf.persist()
    } else if (reply.Term == rf.term) {
      if (reply.OK) {
        //append ok
        //{
        //The network request and reply  will be misorderred,
        //if append msg1 is earlier than append msg2
        //and reply1 if after msg2
        // msg1->msg2->reply1->reply2
        // reply1 update the nextIndex to nextIndex', and reply2 should not use the  nextIndex' to update nextIndex
        // rf.matchIndex[server] = rf.nextIndex[server] - 1 + len(args.Entries);
        // rf.nextIndex[server] = rf.matchIndex[server] + 1;
        //}
        rf.nextIndex[server] = rf.snapshotEntry.LastIncludedIndex + 1;
        rf.matchIndex[server] = rf.snapshotEntry.LastIncludedIndex
        fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] reply ok try install snapshot commit index:%v match index:%v next index:%v\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, rf.commitIndex, rf.matchIndex, rf.nextIndex);
      } else {
        if (rf.snapshotEntry.LastIncludedIndex <= reply.LastIncludedIndex) { //may be server applied Snapshot by service, and nextIndex is too front.
          rf.nextIndex[server] = reply.LastIncludedIndex + 1
        }
        fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] reply not ok install snapshot from server %v and update next index: %v\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, server, rf.nextIndex);
      }

      //2 logs on 3 servers
      //A:{3} {10}  commit_index = 1
      //B:{3} {10}  commit_index = 1
      //C:{3} {10}  commit_index = 0 , and should catch up, whatever
      //whatever append is ok, try catch match index

      //should only commit current term log
      // for newCommitIndex := rf.matchIndex[server]; newCommitIndex > rf.commitIndex && rf.entries[newCommitIndex - rf.snapshotEntry.LastIncludedIndex].Term == rf.term; newCommitIndex-- {
      //   count := 0
      //   for i := 0; i < len(rf.peers); i++ {
      //     if (rf.matchIndex[i] >= newCommitIndex) {
      //       count ++;
      //     }
      //   }
      //   if (count > len(rf.peers) / 2) {
      //     rf.commitIndex = newCommitIndex
      //     rf.CommitToService()
      //   }
      // }
    }
  }
  rf.mu.Unlock()
  return ok
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
  // Your code here (3D).
  fmt.Printf("[Node: %v] begin snapshot\n", rf.me);
  rf.mu.Lock()
  if (rf.snapshotEntry.LastIncludedIndex < index) {   
    if (index > rf.commitIndex) {
      index = rf.commitIndex
    }
    var tmp_entries []LogEntry
    tmp_entries = append(tmp_entries, rf.entries[0])
    for i := index + 1; i <= rf.get_last_entry_idx(); i++ {
      tmp_entries = append(tmp_entries, rf.entries[i -  rf.snapshotEntry.LastIncludedIndex])
    }

    rf.snapshotEntry.LastIncludedTerm = rf.entries[index - rf.snapshotEntry.LastIncludedIndex].Term
    rf.snapshotEntry.LastIncludedIndex = index;
    rf.snapshotEntry.Snapshot = snapshot

    //may be useless, never happened
    if (rf.applyIndex < index) {
      rf.applyIndex = index
    }

    rf.entries = tmp_entries
    rf.persist()
  }
  fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] do service snapshot %v, entries %v \n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, index, rf.entries);
  rf.mu.Unlock()
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
  // Your data here (3A, 3B).
  Term int
  Me int
  LastEntryId int
  LastEntryTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
  // Your data here (3A).
  VoteGranted bool
  Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
  // Your code here (3A, 3B).
  fmt.Printf("[Node: %v] get requst vote msg from %v\n", rf.me, args.Me);
  rf.mu.Lock()
  fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] begin vote for %v\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, args.Me)
  if (rf.term > args.Term) {
    reply.VoteGranted = false;
    reply.Term = rf.term;
  } else if (rf.term < args.Term) {
    reply.Term = rf.term;
    rf.identity = 2
    rf.voteFor = -1
    rf.getVoted = 0
    rf.term = args.Term


    //Suppose 3 server:
    //A is down;
    //C's elect timeout = 399; (max for [200,400]) and B elect timeout = 300;
    //B will become candidate first, requst vote from C, and C may always vote false
    //requestVote will reset C's receivedRPCTime. And then B will begin a new elect round, if B's new elect timeout < C's, C will never be candidate
    //The propobality that B's new elect timeout is larger than C's  is small, since C's too large. Thus no leader will be elected for a long time.
    //So entering a new term, update the elect timeout.

    // may be only enter a new term, the received time should be refresh; and update together with relectTimeout
    //rf.reset_timeout()
    //Maybe update the received time and the elect timeout at the same time is a more moderate way?

    if (rf.get_last_entry_term() > args.LastEntryTerm) {
      reply.VoteGranted = false;
    } else if (rf.get_last_entry_term() == args.LastEntryTerm && rf.get_last_entry_idx() > args.LastEntryId) {
      reply.VoteGranted = false;
    } else {
      rf.voteFor = args.Me
      reply.VoteGranted = true;
	  rf.reset_timeout()
    }
    rf.persist()
  } else if (rf.term == args.Term && rf.voteFor != -1) {
    reply.VoteGranted = false;
    reply.Term = rf.term;
  } else {
    reply.Term = rf.term;
    //rf.reset_timeout()
    //rf.identity = 2
    //rf.term = args.Term
    if (rf.get_last_entry_term() > args.LastEntryTerm) {
      reply.VoteGranted = false;
    } else if (rf.get_last_entry_term() == args.LastEntryTerm && rf. get_last_entry_idx() > args.LastEntryId) {
      reply.VoteGranted = false;
    } else {
      rf.voteFor = args.Me
      reply.VoteGranted = true;
      rf.reset_timeout() // give enough time for target, and flase vote node will become candidate faster.
    }
    rf.persist()
  }
  fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] vote to %v with %v\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, args.Me, reply.VoteGranted)
  rf.mu.Unlock()
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
  fmt.Printf("[Node: %v] send requst vote msg to %v\n", rf.me, server);
  ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  if (ok) {
    rf.mu.Lock()
    if (rf.identity == 1 && rf.term == args.Term) {
      if (reply.VoteGranted) {
        rf.getVoted ++
      } else if (reply.Term > rf.term) {
        rf.identity = 2
        rf.voteFor = -1
        rf.getVoted = 0
        rf.reset_timeout()
        rf.term = reply.Term
        rf.persist()
      }
    }
    rf.mu.Unlock()
  }
  return ok
}

type AppendEntriesArgs struct {
  Term int
  LeaderId int
  LastEntryId int
  LastEntryTerm int
  Entries []LogEntry
  LeaderCommit int
}

type AppendEntriesReply struct {
  Term int
  OK bool
  LastEntryId int
  LastEntryTerm int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
  rf.mu.Lock()
  if (args.Term > rf.term) {
    rf.identity = 2
    rf.voteFor = -1
    rf.getVoted = 0
    rf.term = args.Term
    rf.reset_timeout()
    reply.OK = false;
    rf.persist()

    //TODO :optimize
    if (args.LastEntryId > rf.get_last_entry_idx()) {
      reply.LastEntryId = rf.get_last_entry_idx();
      reply.LastEntryTerm = rf.get_last_entry_term();
    } else if (args.LastEntryId <= rf.snapshotEntry.LastIncludedIndex) {
      reply.LastEntryId = rf.snapshotEntry.LastIncludedIndex
      reply.LastEntryTerm = rf.snapshotEntry.LastIncludedTerm
    } else {
      tmpTerm := rf.get_entry_term_by_idx(args.LastEntryId)
      for i := args.LastEntryId ; i >= rf.snapshotEntry.LastIncludedIndex; i-- {
        if (rf.get_entry_term_by_idx(i) == tmpTerm) {
          //do nothing
        } else {
          reply.LastEntryId = i;
          reply.LastEntryTerm = rf.get_entry_term_by_idx(i);
          break;
        }
      }
    }
    fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] reject O %v, argTerm: %v, rf term: %v\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, args.Entries, args.Term, rf.term)
  } else if (args.Term == rf.term) {
    //the test need consider a misorderred scenario：
    //When L1 log: ... {5, 'Command1'}
    // Send Msg1 to F1(follower)
    //When L1 log: ... {5, 'Command1'} {5, 'Command2'}
    // Send Msg2 to F1
    //================> Network
    //F1 receive Msg2 first
    // then F1 update log to ... {5, 'Command1'} {5, 'Command2'}
    //F1 receive Msg1 after
    // F1 update log to ... {5, 'Command1'}
    // then F1 may lose committed entry.
    //normally tcp will make sure the order

    rf.reset_timeout()
    if (rf.identity == 1) {
      rf.identity = 2
    }

    if (args.LastEntryId > rf.get_last_entry_idx()) {
      reply.OK = false;
      reply.LastEntryId = rf.get_last_entry_idx();
      reply.LastEntryTerm = rf.get_last_entry_term()
      fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] reject A nextId:%v, entry_len:%v\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, args.LastEntryId, len(rf.entries))
    } else if (args.LastEntryId < rf.snapshotEntry.LastIncludedIndex) {
      reply.OK = false;
      reply.LastEntryId = rf.snapshotEntry.LastIncludedIndex
      reply.LastEntryTerm = rf.snapshotEntry.LastIncludedTerm
    } else if (args.LastEntryTerm == rf.get_entry_term_by_idx(args.LastEntryId)) {
      reply.OK = true;
      //appendEntries may be out of order
      // msg1: append {10,2}
      // msg2: append {10,2} {10,3}
      // msg1 and 2 are with the same last entry id
      // receive msg2 first
      // then receive mgs1 and msg1 should not cover msg2
      //check conflict
      i:=0
      for ; i < len(args.Entries) && i + args.LastEntryId + 1  <= rf.get_last_entry_idx(); i++ {
        if (args.Entries[i].Term != rf.get_entry_term_by_idx(i + args.LastEntryId + 1)) {
          rf.entries = rf.entries[:i + args.LastEntryId + 1 - rf.snapshotEntry.LastIncludedIndex]
          break; //conflict
        }
      }

      for ;i < len(args.Entries); i++ {
        rf.entries = append(rf.entries, args.Entries[i])
      }
      rf.persist()
      origCommitIndex := rf.commitIndex
      if (rf.commitIndex < args.LeaderCommit) {
        rf.commitIndex = args.LeaderCommit
        if (rf.commitIndex > i + args.LastEntryId) {
          rf.commitIndex = i + args.LastEntryId
        }
      }
      reply.LastEntryId = i + args.LastEntryId
      reply.LastEntryTerm = rf.get_entry_term_by_idx(reply.LastEntryId);
      if (rf.commitIndex != origCommitIndex) {
        rf.CommitToService()
      }
      fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] receive append msg and update entries %v\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, len(rf.entries))
    } else if (args.LastEntryTerm > rf.get_entry_term_by_idx(args.LastEntryId)) {
      reply.OK = false
      for i := args.LastEntryId; i >= 0; i-- {
        if (rf.get_entry_term_by_idx(i) == rf.get_entry_term_by_idx(args.LastEntryId)) {
          //do nothing
        } else {
          reply.LastEntryId = i;
          reply.LastEntryTerm = rf.get_entry_term_by_idx(i);
          break;
        }
      }
      fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] reject B %v\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, args.Entries)
    } else if (args.LastEntryTerm < rf.get_entry_term_by_idx(args.LastEntryId)) {
      reply.OK = false;
      for i := args.LastEntryId; i >= 0; i-- {
        if (rf.get_entry_term_by_idx(i) == args.LastEntryTerm) {
          reply.LastEntryId = i
          reply.LastEntryTerm = rf.get_entry_term_by_idx(i)
          break;
        }
      }
      fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] reject C %v\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, args.Entries)
    } else {
      fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] reject D %v\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, args.Entries)
    }

  } else { //args.Term < rf.term
    //invalid leader, do not reset timeout
  }
  reply.Term = rf.term
  rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
  ok := true
  rf.mu.Lock()
  if (rf.identity != 0) {
    //do nothing
  } else {
    fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] send term:%v AppendEntires(%v) to Node %v and LastEntryTerm: %v LastEntryId: %v\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, args.Term, len(args.Entries), server, args.LastEntryTerm, args.LastEntryId);
    rf.mu.Unlock()
    ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
    rf.mu.Lock()
    fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] get term:%v reply from %v, AppendEntires(%v) to Node %v and LastEntryTerm: %v LastEntryId: %v\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, args.Term, server, len(args.Entries), server, args.LastEntryTerm, args.LastEntryId);
    if (rf.identity != 0) {
      //do nothing
    } else if (reply.Term > rf.term) {
      rf.identity = 2
      rf.voteFor = -1
      rf.getVoted = 0
      rf.reset_timeout()
      rf.term = reply.Term
      rf.persist()
    } else if (reply.Term == rf.term) {
      if (reply.OK) {
        //append ok
        //{
        //The network request and reply  will be misorderred,
        //if append msg1 is earlier than append msg2
        //and reply1 if after msg2
        // msg1->msg2->reply1->reply2
        // reply1 update the nextIndex to nextIndex', and reply2 should not use the  nextIndex' to update nextIndex
        // rf.matchIndex[server] = rf.nextIndex[server] - 1 + len(args.Entries);
        // rf.nextIndex[server] = rf.matchIndex[server] + 1;
        //}
        rf.nextIndex[server] = reply.LastEntryId + 1
        rf.matchIndex[server] = reply.LastEntryId
        fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] reply ok try update commit index:%v match index:%v next index:%v\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, rf.commitIndex, rf.matchIndex, rf.nextIndex);
      } else {
        fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] reply not ok from server %v \n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, server);
        //append not nok
        rf.nextIndex[server] = reply.LastEntryId + 1

        //optimize:
        //can check if need to update matchIndex
      }

      //2 logs on 3 servers
      //A:{3} {10}  commit_index = 1
      //B:{3} {10}  commit_index = 1
      //C:{3} {10}  commit_index = 0 , and should catch up, whatever
      //whatever append is ok, try catch up match index

      //should only commit current term log
      for newCommitIndex := rf.matchIndex[server]; rf.get_entry_term_by_idx(newCommitIndex) == rf.term && newCommitIndex > rf.commitIndex; newCommitIndex-- {
        count := 0
        for i := 0; i < len(rf.peers); i++ {
          if (rf.matchIndex[i] >= newCommitIndex) {
            count ++;
          }
        }
        if (count > len(rf.peers) / 2) {
          rf.commitIndex = newCommitIndex
          rf.cond.Signal();
        }
      }
    }
  }
  fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] get reply OK:%v from %v, reply Last Entry ID: %v\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, reply.OK, server, reply.LastEntryId)
  rf.mu.Unlock()
  return ok
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
  index := -1
  term := -1
  isLeader := true
  // Your code here (3B).
  rf.mu.Lock()
  term = rf.term
  if (rf.identity != 0) {
    isLeader = false
  } else {
    index = rf.get_last_entry_idx() + 1;
    rf.entries = append(rf.entries, LogEntry{rf.term, command})
    rf.matchIndex[rf.me] = rf.get_last_entry_idx()
    rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
    rf.persist()
  }
  fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] Start(%v)\n", rf.me, isLeader, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, rf.entries);
  rf.mu.Unlock()
  return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
  atomic.StoreInt32(&rf.dead, 1)
  // Your code here, if desired.
}

func (rf *Raft) killed() bool {
  z := atomic.LoadInt32(&rf.dead)
  return z == 1
}


func (rf *Raft) BeginHeartBeat(term int) {
	rf.mu.Lock()
	for (!rf.killed() && rf.identity == 0 && term == rf.term) {
		for i:=0; i < len(rf.peers); i++ {
			if (i != rf.me) {
        if (rf.nextIndex[i] > rf.snapshotEntry.LastIncludedIndex) {
          var args AppendEntriesArgs
          var reply AppendEntriesReply
          args.Term = rf.term
          args.LeaderId = rf.me
          args.LastEntryId = rf.nextIndex[i] - 1;
          args.LastEntryTerm = rf.get_entry_term_by_idx(args.LastEntryId);
          // args.Entries = rf.entries[rf.nextIndex[i]:]
          l:=0
          if (len(rf.entries[rf.nextIndex[i] - rf.snapshotEntry.LastIncludedIndex:]) > 400) {
            l = 400;
          } else {
            l = len(rf.entries[rf.nextIndex[i] - rf.snapshotEntry.LastIncludedIndex:])
          }
          if (l != 0) {
            args.Entries = make([]LogEntry, l)
            copy(args.Entries, rf.entries[rf.nextIndex[i] - rf.snapshotEntry.LastIncludedIndex: rf.nextIndex[i] + l - rf.snapshotEntry.LastIncludedIndex]);
          }
          args.LeaderCommit = rf.commitIndex
          go rf.sendAppendEntries(i, &args, &reply)
        } else {
          var args InstallSnapshotArgs
          var reply InstallSnapshotReply
          args.Term = rf.term
          args.LeaderId = rf.me
          args.LastIncludedIndex = rf.snapshotEntry.LastIncludedIndex
          args.LastIncludedTerm = rf.snapshotEntry.LastIncludedTerm
          args.Snapshot = rf.persister.ReadSnapshot()
          go rf.sendInstallSnapshot(i, &args, &reply)
        }
			}
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
}

func (rf *Raft) BeginRequestVote(term int) {
	rf.mu.Lock()
	for (!rf.killed() && rf.identity == 1 && term == rf.term) {
		for i:=0; i < len(rf.peers); i++ {
			if (rf.me != i) {
			var args RequestVoteArgs
			var reply RequestVoteReply
			args.Term = rf.term
			args.Me = rf.me
			args.LastEntryId = rf.get_last_entry_idx();
			args.LastEntryTerm = rf.get_last_entry_term();
			go rf.sendRequestVote(i, &args, &reply)
			}
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
  // Your code here to check if a leader election should
  // be started and to randomize sleeping time using
  // time.Sleep().
  for (rf.killed() == false) {
    size := len(rf.peers)
    rf.mu.Lock()
    if (rf.identity == 0) {
		//do nothing
    } else if (rf.identity == 2) {
      if (time.Since(rf.receivedRPCTime).Milliseconds() > rf.electTimeout) {
        fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] Follower become candidate cause timeout\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm);
        rf.identity = 1
        rf.term ++
        rf.voteFor = rf.me
        rf.getVoted = 1;
        rf.reset_timeout()

        rf.persist()
        //begintime = time.Now()
        go rf.BeginRequestVote(rf.term)
      }
    } else if (rf.identity == 1) {
	  if (rf.getVoted > (size / 2)) {
			rf.identity = 0
			fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v] Become a new Leader\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex);
			for i:=0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.get_last_entry_idx() + 1
			rf.matchIndex[i] = 0;
			}
			rf.matchIndex[rf.me] = rf.get_last_entry_idx();
			rf.persist()

			go rf.BeginHeartBeat(rf.term)
	  } else if (time.Since(rf.receivedRPCTime).Milliseconds() > rf.electTimeout) {
        fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] Candidate begin a new election\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm);
        rf.identity = 1
        rf.term ++
        rf.voteFor = rf.me
        rf.getVoted = 1;
        rf.reset_timeout()

        rf.persist()

        go rf.BeginRequestVote(rf.term)
        //begintime = time.Now()
      } else {
      }
    } else {
    }
    rf.mu.Unlock()
	time.Sleep(30 * time.Millisecond)
  }
}


func (rf *Raft) sendCommitMsg(applyCh chan ApplyMsg) {
  for rf.killed() == false {
    rf.mu.Lock()
    for rf.applyIndex == rf.commitIndex {
      rf.cond.Wait()
    }
    fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] send msg to channel: %v %v, entries:%v\n", rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm,  rf.applyIndex, rf.commitIndex, rf.entries)
    for rf.applyIndex < rf.commitIndex {
      if (rf.applyIndex < rf.snapshotEntry.LastIncludedIndex) {
        rf.applyIndex = rf.snapshotEntry.LastIncludedIndex
      }
      var applyMsg ApplyMsg
      applyMsg.CommandValid = true;
      fmt.Printf("show: %v , %v\n", rf.applyIndex + 1, rf.snapshotEntry.LastIncludedIndex)
      applyMsg.Command = rf.entries[rf.applyIndex + 1 - rf.snapshotEntry.LastIncludedIndex].Command
      applyMsg.CommandIndex = rf.applyIndex + 1;
      target_applyIndex := rf.applyIndex + 1;
      rf.mu.Unlock()
      applyCh <- applyMsg //applyChan buffer is one, when cosuming, may call snapshot, then cause dead lock.
      rf.mu.Lock()
      //in this gap, a snapshot() may by called. then applyIndex may be updated already.
      //and may be updated to LastIncludedIndex
      //adjust applyIndex here carefully
      if (target_applyIndex > rf.applyIndex) {
        rf.applyIndex = target_applyIndex
      }
    }
    rf.mu.Unlock()
  }
}

func (rf *Raft) sendSnapshotMsg(applyCh chan ApplyMsg) {
  for rf.killed() == false {
    rf.mu.Lock()
    rf.cond2.Wait()
    var applyMsg ApplyMsg
    applyMsg.CommandValid = false;
    applyMsg.SnapshotValid = true;
    applyMsg.Snapshot = rf.snapshotEntry.Snapshot
    applyMsg.SnapshotTerm = rf.snapshotEntry.LastIncludedTerm
    applyMsg.SnapshotIndex = rf.snapshotEntry.LastIncludedIndex
    rf.mu.Unlock()
    applyCh <- applyMsg
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

  // Your initialization code here (3A, 3B, 3C).
  rf.identity = 2
  rf.getVoted = 0
  rf.applyIndex = 0;
  rf.commitIndex = 0;
  if (persister.RaftStateSize() < 1 || persister.ReadRaftState() == nil) {
    rf.voteFor = -1
    rf.term = 0;
    rf.entries = append(rf.entries, LogEntry{0, 0});
    rf.snapshotEntry.LastIncludedIndex = 0
    rf.snapshotEntry.LastIncludedTerm = 0
    rf.snapshotEntry.Snapshot = make([]byte, 0, 0)
    rf.persist()
    fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] bootstrap\n",  rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm)
  } else {
    rf.readPersist(persister.ReadRaftState())
    fmt.Printf("[Node: %v, IsLeader: %v, Term: %v, commitIndex:%v, applyIndex:%v, snapshot:(%v, %v)] read from persister, entries(%v), term(%v), voteFor(%v)\n",  rf.me, rf.identity == 0, rf.term, rf.commitIndex, rf.applyIndex, rf.snapshotEntry.LastIncludedIndex, rf.snapshotEntry.LastIncludedTerm, len(rf.entries), rf.term, rf.voteFor)
  }

  for i:=0; i < len(peers); i++ {
    rf.nextIndex = append(rf.nextIndex, rf.get_last_entry_idx() + 1)
    rf.matchIndex = append(rf.matchIndex, 0)
  }
  // initialize from state persisted before a crash

  rf.cond = sync.NewCond(&rf.mu)
  rf.cond2 = sync.NewCond(&rf.mu)
  rf.applyIndex = rf.snapshotEntry.LastIncludedIndex
  rf.commitIndex = rf.snapshotEntry.LastIncludedIndex
  go rf.sendCommitMsg(applyCh)
  go rf.sendSnapshotMsg(applyCh)

  rf.reset_timeout()

  // start ticker goroutine to start elections
  go rf.ticker()
  return rf
}