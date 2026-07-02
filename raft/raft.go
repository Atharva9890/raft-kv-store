package raft

import (
	"math/rand"
	"sync"
	"time"
)

// one cluster member. everything mutable lives behind mu - RPC
// handlers, the background loops, all of it takes the lock before
// touching state and drops it before making any network call. I
// learned the hard way (well, in theory - read enough Raft postmortems)
// that holding a lock across an RPC is the easiest way to deadlock a
// whole cluster, so nothing in here does that.
type Node struct {
	mu sync.Mutex

	id        PeerID
	peers     []PeerID
	transport Transport
	persister Persister
	applyCh   chan ApplyMsg

	// persistent state, Figure 2
	currentTerm uint64
	votedFor    PeerID
	log         *log

	// volatile, every node
	role        Role
	commitIndex uint64
	lastApplied uint64

	// volatile, leader only - reset every time I win an election
	nextIndex  map[PeerID]uint64
	matchIndex map[PeerID]uint64

	// last leader I heard a real heartbeat from, so clients hitting
	// the wrong node can be pointed somewhere useful instead of
	// guessing (see client/client.go)
	leaderHint PeerID

	electionReset chan struct{}
	stopCh        chan struct{}
	stopOnce      sync.Once
}

func NewNode(cfg Config, transport Transport, persister Persister, applyCh chan ApplyMsg) *Node {
	n := &Node{
		id:            cfg.Self,
		peers:         cfg.OtherPeers(),
		transport:     transport,
		persister:     persister,
		applyCh:       applyCh,
		log:           newLog(),
		role:          Follower,
		nextIndex:     make(map[PeerID]uint64),
		matchIndex:    make(map[PeerID]uint64),
		electionReset: make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
	}

	if term, votedFor, entries, err := persister.LoadState(); err == nil {
		n.currentTerm = term
		n.votedFor = votedFor
		n.log.entries = entries
	}
	if data, lastIncludedIndex, lastIncludedTerm, err := persister.LoadSnapshot(); err == nil {
		n.log.lastIncludedIndex = lastIncludedIndex
		n.log.lastIncludedTerm = lastIncludedTerm
		if n.commitIndex < lastIncludedIndex {
			n.commitIndex = lastIncludedIndex
		}
		if n.lastApplied < lastIncludedIndex {
			n.lastApplied = lastIncludedIndex
		}
		// the state machine itself picks this up off applyCh once Run
		// starts - queuing it here so it's the very first thing that
		// goes out
		go func() {
			n.applyCh <- ApplyMsg{SnapshotValid: true, Snapshot: data, SnapshotIndex: lastIncludedIndex, SnapshotTerm: lastIncludedTerm}
		}()
	}

	return n
}

// starts the three background loops. call it once, in its own
// goroutine (or just `go node.Run()`).
func (n *Node) Run() {
	go n.electionTimerLoop()
	go n.heartbeatLoop()
	go n.applyLoop()
}

// safe to call more than once - a test killing a node it already
// killed shouldn't panic on a double close.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
}

func (n *Node) electionTimerLoop() {
	for {
		timeout := randomElectionTimeout()
		select {
		case <-time.After(timeout):
			n.mu.Lock()
			role := n.role
			n.mu.Unlock()
			if role != Leader {
				n.startElection()
			}
		case <-n.electionReset:
			continue
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.mu.Lock()
			isLeader := n.role == Leader
			n.mu.Unlock()
			if isLeader {
				n.broadcastAppendEntries()
			}
		case <-n.stopCh:
			return
		}
	}
}

// pushes anything between lastApplied and commitIndex onto applyCh,
// in order. I poll instead of using a condition variable here - it's
// simpler and 10ms of latency on applying a committed entry has never
// mattered for anything I've tested against.
func (n *Node) applyLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.mu.Lock()
			var msgs []ApplyMsg
			for n.lastApplied < n.commitIndex {
				n.lastApplied++
				if entry, ok := n.log.at(n.lastApplied); ok {
					msgs = append(msgs, ApplyMsg{
						CommandValid: true,
						Command:      entry.Command,
						CommandIndex: entry.Index,
						CommandTerm:  entry.Term,
					})
				}
			}
			n.mu.Unlock()
			for _, m := range msgs {
				n.applyCh <- m
			}
		case <-n.stopCh:
			return
		}
	}
}

// appends cmd to my log if I'm currently the leader. returns right
// after the local append - the caller (server/server.go) has to wait
// for the entry to actually reach commitIndex before telling a client
// it succeeded. appending locally is cheap and reversible; committed
// is the only thing Raft actually promises will stick.
func (n *Node) Propose(cmd []byte) (index uint64, term uint64, isLeader bool) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, 0, false
	}

	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   n.log.lastIndex() + 1,
		Command: cmd,
	}
	n.log.append(entry)
	n.persistLocked()
	n.mu.Unlock()

	// don't wait for the next heartbeat tick to let everyone know -
	// kick replication off right away so a client isn't sitting
	// around for up to HeartbeatInterval for no reason
	go n.broadcastAppendEntries()

	return entry.Index, entry.Term, true
}

func (n *Node) State() (term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.role == Leader
}

func (n *Node) LeaderHint() PeerID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderHint
}

func (n *Node) persistLocked() {
	_ = n.persister.SaveState(n.currentTerm, n.votedFor, n.log.entries)
}

func randomElectionTimeout() time.Duration {
	span := ElectionTimeoutMax - ElectionTimeoutMin
	return ElectionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
}

func (n *Node) resetElectionTimer() {
	select {
	case n.electionReset <- struct{}{}:
	default:
	}
}
