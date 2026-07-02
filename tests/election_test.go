package tests

import (
	"testing"
	"time"

	"github.com/Atharva9890/raft-kv-store/raft"
)

// TestSingleLeaderElected is the most basic possible Raft correctness
// property: give a healthy 3-node cluster enough time, and exactly
// one node should become leader.
//
// Fails on the unmodified scaffold because startElection() in
// raft/election.go never actually counts votes or calls
// becomeLeaderLocked.
func TestSingleLeaderElected(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()

	c.waitForLeader(2 * time.Second)
}

// TestReElectionAfterLeaderCrash checks that once a leader is elected
// and then partitioned away from the rest of the cluster (simulating
// a crash - it can no longer send heartbeats or receive votes), the
// remaining majority elects a new leader on its own.
func TestReElectionAfterLeaderCrash(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()

	first := c.waitForLeader(2 * time.Second)

	var rest []raft.PeerID
	for id := range c.nodes {
		if id != first {
			rest = append(rest, id)
		}
	}
	c.net.Partition([]raft.PeerID{first}, rest)

	second := c.waitForLeader(2 * time.Second)
	if second == first {
		t.Fatalf("expected a new leader after partitioning away %s, got the same leader back", first)
	}
}
