package tests

import (
	"testing"
	"time"

	"github.com/Atharva9890/raft-kv-store/raft"
)

// crash simulates a hard node failure: stop its timers/goroutines and
// cut it off from every other node, so it can neither send nor
// receive anything - indistinguishable, from the rest of the
// cluster's point of view, from the process having been kill -9'd.
func crash(c *cluster, id raft.PeerID) {
	c.nodes[id].Stop()
	var others []raft.PeerID
	for other := range c.nodes {
		if other != id {
			others = append(others, other)
		}
	}
	c.net.Partition([]raft.PeerID{id}, others)
}

// TestClusterSurvivesLeaderCrash is the headline demo of this whole
// project: commit an entry, kill the leader, and confirm both that a
// new leader takes over AND that the entry committed before the
// crash is still there afterwards (durability of committed state is
// the entire point of a consensus protocol - if a crash could lose
// committed data, you'd just be building an eventually-consistent
// cache with extra steps).
func TestClusterSurvivesLeaderCrash(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()

	leaderID := c.waitForLeader(2 * time.Second)
	leader := c.nodes[leaderID]

	if _, _, isLeader := leader.Propose([]byte("set x=1")); !isLeader {
		t.Fatalf("expected %s to still be leader when proposing", leaderID)
	}

	// Give the entry a chance to commit before we pull the plug.
	time.Sleep(300 * time.Millisecond)

	crash(c, leaderID)

	newLeaderID := c.waitForLeader(2 * time.Second)
	if newLeaderID == leaderID {
		t.Fatalf("waitForLeader returned the crashed node %s as leader", leaderID)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.appliedCount(newLeaderID) >= 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("new leader %s never applied the entry committed before the old leader crashed", newLeaderID)
}

// TestMinorityCannotMakeProgressDuringOutage combines a crash with the
// quorum requirement: killing 3 of 5 nodes leaves only a minority
// alive, which must NOT be able to elect a leader or commit anything,
// since it can't rule out that a majority-side leader with more
// recent entries exists.
func TestMinorityCannotMakeProgressDuringOutage(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()

	c.waitForLeader(2 * time.Second)

	var all []raft.PeerID
	for id := range c.nodes {
		all = append(all, id)
	}
	for _, id := range all[:3] {
		crash(c, id)
	}

	survivors := all[3:]
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range survivors {
			if _, isLeader := c.nodes[id].State(); isLeader {
				t.Fatalf("survivor %s became leader with only %d/%d nodes alive - should be impossible without a majority", id, len(survivors), len(all))
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}
