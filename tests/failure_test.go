package tests

import (
	"testing"
	"time"

	"github.com/Atharva9890/raft-kv-store/raft"
)

// simulates a hard crash: stop the node's goroutines and cut it off
// from everyone else, so from the rest of the cluster's perspective
// it's indistinguishable from someone pulling the power cord. I also
// mark it dead in the harness so leaders()/waitForLeader() stop
// asking its Node object for a status it can no longer meaningfully
// give - see the comment on markDead in harness.go for why that's
// needed.
func crash(c *cluster, id raft.PeerID) {
	c.nodes[id].Stop()
	var others []raft.PeerID
	for other := range c.nodes {
		if other != id {
			others = append(others, other)
		}
	}
	c.net.Partition([]raft.PeerID{id}, others)
	c.markDead(id)
}

// this is basically the whole point of the project: commit something,
// kill the leader, and check both that (a) someone else takes over
// and (b) the entry that was already committed is still there
// afterwards. if a crash could lose committed data I'd just be
// building a fancy cache, not a consensus system.
func TestClusterSurvivesLeaderCrash(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()

	leaderID := c.waitForLeader(2 * time.Second)
	leader := c.nodes[leaderID]

	if _, _, isLeader := leader.Propose([]byte("set x=1")); !isLeader {
		t.Fatalf("expected %s to still be leader when proposing", leaderID)
	}

	time.Sleep(300 * time.Millisecond) // give it a moment to actually commit before I pull the plug

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

// kill 3 of 5 nodes and the 2 survivors should never be able to
// commit anything new - they can't rule out that a majority-side
// leader with newer entries exists somewhere they can't see.
//
// note this does NOT assert "no survivor has role==Leader" - if the
// node that happened to already be leader before the crash is one of
// the 2 survivors, it's still going to think it's leader (nothing
// told it otherwise) and that's fine. the actual guarantee Raft makes
// is that it can never get a new entry committed with only 2/5 nodes
// reachable, so that's what I check instead.
func TestMinorityCannotMakeProgressDuringOutage(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()

	c.waitForLeader(2 * time.Second)
	time.Sleep(200 * time.Millisecond) // let the post-election no-op finish landing on every node first

	var all []raft.PeerID
	for id := range c.nodes {
		all = append(all, id)
	}

	// baseline before crashing anything - without this I'd sometimes
	// catch a survivor applying an entry that was legitimately
	// committed by the full healthy cluster moments earlier and just
	// hadn't been picked up by its applyLoop yet, which isn't the bug
	// this test is supposed to catch.
	baseline := make(map[raft.PeerID]int)
	for _, id := range all {
		baseline[id] = c.appliedCount(id)
	}

	for _, id := range all[:3] {
		crash(c, id)
	}
	survivors := all[3:]

	for _, id := range survivors {
		if _, _, isLeader := c.nodes[id].Propose([]byte("should never commit")); isLeader {
			break // found whichever survivor (if any) still thinks it's leader
		}
	}

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range survivors {
			if c.appliedCount(id) > baseline[id] {
				t.Fatalf("survivor %s applied a new entry with only %d/%d nodes alive - should be impossible without a majority", id, len(survivors), len(all))
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}
