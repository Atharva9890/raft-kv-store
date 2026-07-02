package raft

import "time"

// the three states a node can be in. everyone starts as a follower,
// and only moves to candidate/leader through an election.
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

// randomizing the election timeout per node is what stops every
// follower from timing out at the same instant and splitting the
// vote every single round. heartbeat needs to be comfortably shorter
// than the timeout window or leaders will get randomly deposed.
const (
	ElectionTimeoutMin = 300 * time.Millisecond
	ElectionTimeoutMax = 600 * time.Millisecond
	HeartbeatInterval  = 100 * time.Millisecond
)

// one entry in the replicated log. Command is whatever the KV layer
// encoded (see server/kv_store.go) - raft itself doesn't care what's
// inside, it just needs to replicate it in order.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// what gets pushed to the state machine once something's actually
// committed. CommandValid vs SnapshotValid tells the receiver which
// half of this struct to look at.
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

// id of a cluster member - has to match what's in Config.Peers and
// what config/config.go hands out from the PEERS env var.
type PeerID string

// static membership for the cluster this node belongs to, loaded once
// at startup. I didn't implement live membership changes (adding or
// removing a node from a running cluster) - that needs joint
// consensus per §6 of the paper and felt like a separate project on
// top of getting basic replication right.
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
