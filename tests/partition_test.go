package tests

import (
	"testing"
	"time"

	"github.com/Atharva9890/raft-kv-store/raft"
)

// TestMinorityPartitionCannotElectLeader is the flip side of majority
// quorum: if you split a 5-node cluster into a 2-node minority and a
// 3-node majority, only the majority side should ever be able to
// elect a leader. A minority that could still elect its own leader
// would let both sides accept writes independently - a split brain.
//
// Fails on the unmodified scaffold for the same reason
// TestSingleLeaderElected does (no vote counting yet), but is also
// the test that will catch a *broken* vote-counting implementation
// that forgets to require a strict majority.
func TestMinorityPartitionCannotElectLeader(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stop()

	c.waitForLeader(2 * time.Second)

	var all []raft.PeerID
	for id := range c.nodes {
		all = append(all, id)
	}
	minority := all[:2]
	majority := all[2:]
	c.net.Partition(minority, majority)

	// The majority side must still be able to elect (possibly a new)
	// leader on its own.
	deadline := time.Now().Add(2 * time.Second)
	var majorityLeader raft.PeerID
	for time.Now().Before(deadline) {
		for _, id := range majority {
			if _, isLeader := c.nodes[id].State(); isLeader {
				majorityLeader = id
			}
		}
		if majorityLeader != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if majorityLeader == "" {
		t.Fatalf("majority partition never elected a leader")
	}

	// The minority side must NOT be able to elect a leader of its own -
	// that would be two leaders active at once.
	for _, id := range minority {
		if _, isLeader := c.nodes[id].State(); isLeader {
			t.Fatalf("minority node %s incorrectly became leader while partitioned", id)
		}
	}
}

// TestClusterRecoversAfterHeal checks that once a partition heals, the
// cluster converges back to a single leader instead of getting stuck
// with stale terms or a permanently confused minority.
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
