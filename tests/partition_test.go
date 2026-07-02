package tests

import (
	"testing"
	"time"

	"github.com/Atharva9890/raft-kv-store/raft"
)

// split a 5-node cluster into a 2-node minority and a 3-node
// majority. the majority should still be able to elect a leader and
// commit new writes; the minority should not be able to commit
// anything at all.
//
// worth calling out: I'm NOT asserting "no minority node has
// role==Leader". if the pre-partition leader happens to land in the
// minority group, it keeps believing it's leader - nothing tells it
// otherwise until it reconnects. that's expected, not a split brain,
// because it can never actually get a write committed without a
// majority. the property that actually matters (and the one Raft
// promises) is "can it commit," so that's what I check.
func TestMinorityPartitionCannotElectLeader(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()

	c.waitForLeader(2 * time.Second)
	time.Sleep(200 * time.Millisecond) // let the post-election no-op finish landing on every node before I split anything

	var all []raft.PeerID
	for id := range c.nodes {
		all = append(all, id)
	}
	minority := all[:2]
	majority := all[2:]

	baseline := make(map[raft.PeerID]int)
	for _, id := range minority {
		baseline[id] = c.appliedCount(id)
	}

	c.net.Partition(minority, majority)

	majorityLeader := c.waitForLeaderAmong(majority, 2*time.Second)
	if _, _, isLeader := c.nodes[majorityLeader].Propose([]byte("majority-only write")); !isLeader {
		t.Fatalf("%s was just confirmed as the majority-side leader but rejected Propose", majorityLeader)
	}

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range minority {
			if c.appliedCount(id) > baseline[id] {
				t.Fatalf("minority node %s applied a new entry while partitioned - should be impossible without a majority", id)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// once a partition heals, the cluster should converge back to a
// single leader - no stuck stale terms, no minority left confused
// forever.
func TestClusterRecoversAfterHeal(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()

	c.waitForLeader(2 * time.Second)

	var all []raft.PeerID
	for id := range c.nodes {
		all = append(all, id)
	}
	c.net.Partition(all[:2], all[2:])
	time.Sleep(500 * time.Millisecond)
	c.net.Heal()

	c.waitForLeader(2 * time.Second)
}
