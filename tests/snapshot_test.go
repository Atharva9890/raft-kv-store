package tests

import (
	"testing"
	"time"

	"github.com/Atharva9890/raft-kv-store/raft"
)

// disconnect a follower right away, pile up a bunch of entries on the
// leader, compact the log past everything that follower ever saw,
// then reconnect it. the only way it can catch up at that point is
// InstallSnapshot - plain AppendEntries can't help it once the leader
// no longer has the entries it's missing.
func TestFollowerCatchesUpViaSnapshot(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stop()

	leaderID := c.waitForLeader(2 * time.Second)
	leader := c.nodes[leaderID]

	var lagging raft.PeerID
	for id := range c.nodes {
		if id != leaderID {
			lagging = id
			break
		}
	}

	var others []raft.PeerID
	for id := range c.nodes {
		if id != lagging {
			others = append(others, id)
		}
	}
	c.net.Partition([]raft.PeerID{lagging}, others)

	var lastIndex uint64
	for i := 0; i < 10; i++ {
		idx, _, isLeader := leader.Propose([]byte("while-disconnected"))
		if !isLeader {
			t.Fatalf("%s stopped being leader mid-test", leaderID)
		}
		lastIndex = idx
	}
	waitForCondition(t, 2*time.Second, func() bool { return c.appliedCount(leaderID) >= 10 })

	leader.TakeSnapshot(lastIndex, []byte("fake-snapshot-data"))

	c.net.Heal()

	waitForCondition(t, 3*time.Second, func() bool { return c.hasSnapshot(lagging) })
}
