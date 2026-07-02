package raft

import "time"

// Role is a node's position in the Raft state machine: every node
// starts a Follower, becomes a Candidate when it stops hearing from a
// leader, and becomes Leader if it wins an election.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// Tuning knobs. The election timeout is randomized per node within
// [ElectionTimeoutMin, ElectionTimeoutMax) to avoid split votes -
// see election.go.
const (
	ElectionTimeoutMin = 300 * time.Millisecond
	ElectionTimeoutMax = 600 * time.Millisecond
	HeartbeatInterval  = 100 * time.Millisecond
)

// LogEntry is one entry in the replicated log. Command is an opaque
// payload interpreted by the StateMachine (server/kv_store.go encodes
// KV operations into it).
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// ApplyMsg is delivered on the node's apply channel once an entry has
// been committed by a majority of the cluster and is safe to apply to
// the local state machine. CommandValid distinguishes a normal log
// entry from a snapshot install.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
	CommandTerm  uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// PeerID identifies a cluster member. It must match the id used in
// the cluster Config and in gRPC dial targets (config/config.go).
type PeerID string

// Config describes the fixed membership of the cluster this node
// participates in.
//
// TODO(membership-changes): this is a static config loaded at startup.
// The resume claim is "membership changes" - implementing that means
// replacing this with joint consensus (Raft §6): a ConfigEntry type
// committed through the log itself, with old+new config both voting
// during the transition. Start here once basic election + replication
// pass their tests.
type Config struct {
	Self  PeerID
	Peers map[PeerID]string // id -> "host:port"
}

func (c Config) OtherPeers() []PeerID {
	ids := make([]PeerID, 0, len(c.Peers)-1)
	for id := range c.Peers {
		if id != c.Self {
			ids = append(ids, id)
		}
	}
	return ids
}
