package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Node is a single Raft cluster member. It owns the persistent and
// volatile state described in Raft Figure 2, drives the
// election/heartbeat timers, and exposes Propose for the KV server to
// submit new commands.
//
// All mutable fields are guarded by mu. RPC handlers (election.go,
// log.go, snapshot.go) and the background loop (run) all take the
// lock before touching state, and release it before making any
// network call - holding a lock across an RPC is how you deadlock a
// Raft cluster.
type Node struct {
	mu sync.Mutex

	id        PeerID
	peers     []PeerID
	transport Transport
	persister Persister
	applyCh   chan ApplyMsg

	// --- persistent state (Figure 2) ---
	currentTerm uint64
	votedFor    PeerID // "" if none
	log         *log

	// --- volatile state, all nodes ---
	role        Role
	commitIndex uint64
	lastApplied uint64

	// --- volatile state, leaders only (reinitialized on election) ---
	nextIndex  map[PeerID]uint64
	matchIndex map[PeerID]uint64

	electionReset chan struct{} // signals "we heard from a valid leader/candidate, reset the timer"
	stopCh        chan struct{}
}

// NewNode constructs a Node for cfg.Self. It does not start any
// goroutines; call Run in its own goroutine to begin participating in
// the cluster.
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

	return n
}

// Run drives the node's timers until Stop is called. It must be
// started in its own goroutine.
func (n *Node) Run() {
	go n.electionTimerLoop()
	go n.heartbeatLoop()
	go n.applyLoop()
}

func (n *Node) Stop() {
	close(n.stopCh)
}

// electionTimerLoop fires startElection whenever ElectionTimeout
// elapses without electionReset being pinged (i.e. without hearing a
// valid heartbeat/vote request from a legitimate leader/candidate).
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

// heartbeatLoop sends AppendEntries (possibly empty, i.e. a
// heartbeat) to every peer at a fixed interval whenever this node
// believes it is the leader.
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

// applyLoop pushes committed-but-not-yet-applied entries onto applyCh
// in order. See log.go advanceCommitIndex for how commitIndex moves
// forward.
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

// Propose appends cmd to the log if this node is currently the
// leader. It returns immediately after appending locally; the caller
// (server/server.go) must wait for the entry to reach commitIndex
// (delivered via applyCh) before responding to the client - Raft only
// guarantees an entry survives once it is committed, not once it is
// merely appended by a leader that may lose the next election.
func (n *Node) Propose(cmd []byte) (index uint64, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return 0, 0, false
	}

	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   n.log.lastIndex() + 1,
		Command: cmd,
	}
	n.log.append(entry)
	n.persistLocked()

	// TODO(replication): appending locally isn't enough to make
	// progress before the next heartbeat tick - kick an immediate
	// replication round here instead of waiting up to
	// HeartbeatInterval for the periodic broadcast in log.go.

	return entry.Index, entry.Term, true
}

func (n *Node) State() (term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.role == Leader
}

// LeaderHint returns the id of the peer this node most recently
// believed to be leader, so a client hitting the wrong node can be
// redirected without guessing. Returns "" if unknown.
//
// TODO: track this explicitly (e.g. set from the leaderId field on
// every AppendEntries this node accepts) - currently unimplemented,
// which means clients fall back to round-robin retry against the
// full peer list. See client/client.go.
func (n *Node) LeaderHint() PeerID {
	return ""
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
