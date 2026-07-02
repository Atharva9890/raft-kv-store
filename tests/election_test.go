package tests

import (
	"testing"
	"time"

	"github.com/Atharva9890/raft-kv-store/raft"
)

// most basic thing I could ask of this: give a healthy 3-node cluster
// a couple seconds and exactly one of them should end up leader.
func TestSingleLeaderElected(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()

	c.waitForLeader(2 * time.Second)
}

// partition the leader away from everyone else (no network access at
// all, in either direction) and check the remaining 4 nodes elect
// someone new on their own. I'm using waitForLeaderAmong scoped to
// "rest" here instead of the plain global waitForLeader, because the
// old leader doesn't actually know it's been cut off - it just keeps
// blasting heartbeats into the void and, as far as its own state
// goes, still thinks it's in charge. that's correct Raft behavior,
// not a bug, so the test has to account for it instead of asserting
// cluster-wide uniqueness.
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

	second := c.waitForLeaderAmong(rest, 2*time.Second)
	if second == first {
		t.Fatalf("expected a new leader after partitioning away %s, got the same leader back", first)
	}
}
