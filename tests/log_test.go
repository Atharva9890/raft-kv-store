package tests

import (
	"testing"
	"time"
)

// TestProposedEntryReplicatesToAllNodes checks the core log
// replication promise: once the leader commits an entry, every node
// in the cluster eventually applies it - not just the leader.
//
// Fails on the unmodified scaffold because replicateToPeer() in
// raft/log.go never advances nextIndex/matchIndex or calls
// advanceCommitIndexLocked, so commitIndex never moves and applyLoop
// has nothing to deliver.
func TestProposedEntryReplicatesToAllNodes(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()

	leaderID := c.waitForLeader(2 * time.Second)
	leader := c.nodes[leaderID]

	index, _, isLeader := leader.Propose([]byte("set x=1"))
	if !isLeader {
		t.Fatalf("node %s reported as leader by waitForLeader but rejected Propose", leaderID)
	}
	if index == 0 {
		t.Fatalf("expected a positive log index from Propose, got 0")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		allCaughtUp := true
		for id := range c.nodes {
			if c.appliedCount(id) < 1 {
				allCaughtUp = false
			}
		}
		if allCaughtUp {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("not all nodes applied the proposed entry within the deadline")
}

// TestFollowerRejectsProposals checks that only the leader accepts
// writes - a correctness property that's actually already
// implemented (see raft.Node.Propose), so this test should pass even
// on the unmodified scaffold. If it doesn't, something more basic
// than the TODOs is broken.
func TestFollowerRejectsProposals(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()

	leaderID := c.waitForLeader(2 * time.Second)
	for id, n := range c.nodes {
		if id == leaderID {
			continue
		}
		if _, _, isLeader := n.Propose([]byte("nope")); isLeader {
			t.Fatalf("follower %s accepted a Propose call - only the leader should", id)
		}
	}
}
