// I exercise raft.Node in-process here, over an in-memory fake
// network instead of real gRPC. That's what lets me simulate a
// partition or a node crash in a couple lines and run the whole suite
// in a few seconds instead of needing docker-compose running.
//
// these tests are what I actually trust to tell me if the Raft
// implementation is correct - go test ./tests/... is my ground truth,
// more than "it worked when I tried it manually." if I ever rewrite
// election.go/log.go/snapshot.go from scratch, this is the suite that
// has to pass again before I believe it.
package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Atharva9890/raft-kv-store/raft"
)

var (
	errPartitioned = errors.New("peer unreachable (partitioned)")
	errUnknownPeer = errors.New("unknown peer")
)

// fakeNetwork routes RPCs directly between in-process raft.Node
// instances, with the ability to cut connectivity between arbitrary
// pairs of nodes to simulate a partition.
type fakeNetwork struct {
	mu    sync.RWMutex
	nodes map[raft.PeerID]*raft.Node
	cut   map[raft.PeerID]map[raft.PeerID]bool // cut[a][b] == true means a cannot reach b
}

func newFakeNetwork() *fakeNetwork {
	return &fakeNetwork{
		nodes: make(map[raft.PeerID]*raft.Node),
		cut:   make(map[raft.PeerID]map[raft.PeerID]bool),
	}
}

func (f *fakeNetwork) register(id raft.PeerID, n *raft.Node) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes[id] = n
}

func (f *fakeNetwork) setCut(a, b raft.PeerID, cut bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cut[a] == nil {
		f.cut[a] = make(map[raft.PeerID]bool)
	}
	f.cut[a][b] = cut
}

// Partition splits the cluster into two groups that cannot reach each
// other. Nodes within the same group can still talk freely.
func (f *fakeNetwork) Partition(groupA, groupB []raft.PeerID) {
	for _, a := range groupA {
		for _, b := range groupB {
			f.setCut(a, b, true)
			f.setCut(b, a, true)
		}
	}
}

// Heal reconnects every pair of nodes.
func (f *fakeNetwork) Heal() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cut = make(map[raft.PeerID]map[raft.PeerID]bool)
}

func (f *fakeNetwork) reachable(a, b raft.PeerID) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return !f.cut[a][b]
}

func (f *fakeNetwork) transportFor(self raft.PeerID) raft.Transport {
	return &fakeTransport{net: f, self: self}
}

type fakeTransport struct {
	net  *fakeNetwork
	self raft.PeerID
}

func (t *fakeTransport) resolve(peer raft.PeerID) (*raft.Node, error) {
	if !t.net.reachable(t.self, peer) {
		return nil, errPartitioned
	}
	t.net.mu.RLock()
	n, ok := t.net.nodes[peer]
	t.net.mu.RUnlock()
	if !ok {
		return nil, errUnknownPeer
	}
	return n, nil
}

func (t *fakeTransport) SendRequestVote(_ context.Context, peer raft.PeerID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	n, err := t.resolve(peer)
	if err != nil {
		return nil, err
	}
	return n.HandleRequestVote(args), nil
}

func (t *fakeTransport) SendAppendEntries(_ context.Context, peer raft.PeerID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	n, err := t.resolve(peer)
	if err != nil {
		return nil, err
	}
	return n.HandleAppendEntries(args), nil
}

func (t *fakeTransport) SendInstallSnapshot(_ context.Context, peer raft.PeerID, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	n, err := t.resolve(peer)
	if err != nil {
		return nil, err
	}
	return n.HandleInstallSnapshot(args), nil
}

// cluster is a set of in-process nodes wired together over a
// fakeNetwork, plus enough bookkeeping to apply committed commands
// and inspect them from test code.
type cluster struct {
	t     *testing.T
	net   *fakeNetwork
	nodes map[raft.PeerID]*raft.Node

	mu      sync.Mutex
	applied map[raft.PeerID][]raft.ApplyMsg
	dead    map[raft.PeerID]bool
}

// newCluster builds n nodes named "node1".."nodeN", fully connected,
// and starts them all running.
func newCluster(t *testing.T, n int) *cluster {
	t.Helper()

	peers := make(map[raft.PeerID]string)
	for i := 1; i <= n; i++ {
		id := raft.PeerID(fmt.Sprintf("node%d", i))
		peers[id] = "" // address is unused by fakeTransport
	}

	c := &cluster{
		t:       t,
		net:     newFakeNetwork(),
		nodes:   make(map[raft.PeerID]*raft.Node),
		applied: make(map[raft.PeerID][]raft.ApplyMsg),
		dead:    make(map[raft.PeerID]bool),
	}

	for id := range peers {
		cfg := raft.Config{Self: id, Peers: peers}
		applyCh := make(chan raft.ApplyMsg, 256)
		node := raft.NewNode(cfg, c.net.transportFor(id), raft.NewMemoryPersister(), applyCh)
		c.net.register(id, node)
		c.nodes[id] = node
		go c.drainApplyCh(id, applyCh)
	}

	for _, n := range c.nodes {
		n.Run()
	}

	return c
}

func (c *cluster) drainApplyCh(id raft.PeerID, ch chan raft.ApplyMsg) {
	for msg := range ch {
		c.mu.Lock()
		c.applied[id] = append(c.applied[id], msg)
		c.mu.Unlock()
	}
}

func (c *cluster) stop() {
	for _, n := range c.nodes {
		n.Stop()
	}
}

// markDead excludes id from leaders()/waitForLeader() from here on.
// I need this because "crashing" a node in the harness (see crash()
// in failure_test.go) only stops its goroutines and cuts its network
// - the in-process Node object is still sitting right there and its
// last-known role field never gets updated again, so it'll answer
// State() with whatever it believed right before it died. A real
// crashed process can't answer at all, so I don't let the fake one
// either once it's marked dead.
func (c *cluster) markDead(id raft.PeerID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dead[id] = true
}

// every node that currently believes it's the leader, excluding dead
// ones. worth remembering this can legitimately return more than one
// id during a partition: a leader stuck on the minority side has no
// way of knowing it's been deposed until it either reconnects or
// tries to commit something and fails to reach a majority. that's not
// a bug, it's why waitForLeaderAmong exists below.
func (c *cluster) leaders() []raft.PeerID {
	c.mu.Lock()
	dead := make(map[raft.PeerID]bool, len(c.dead))
	for id := range c.dead {
		dead[id] = true
	}
	c.mu.Unlock()

	var ids []raft.PeerID
	for id, n := range c.nodes {
		if dead[id] {
			continue
		}
		if _, isLeader := n.State(); isLeader {
			ids = append(ids, id)
		}
	}
	return ids
}

// waitForLeader polls until exactly one leader emerges cluster-wide.
// only meaningful when the cluster isn't currently split by a
// partition - see waitForLeaderAmong for that case.
func (c *cluster) waitForLeader(timeout time.Duration) raft.PeerID {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ls := c.leaders(); len(ls) == 1 {
			return ls[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no single leader elected within %s (leaders seen: %v)", timeout, c.leaders())
	return ""
}

// same idea, but scoped to a subset of nodes - what I actually want
// when checking "did the majority side elect someone" while a
// partitioned-off former leader is still out there insisting it's in
// charge.
func (c *cluster) waitForLeaderAmong(ids []raft.PeerID, timeout time.Duration) raft.PeerID {
	c.t.Helper()
	want := make(map[raft.PeerID]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var found []raft.PeerID
		for _, id := range c.leaders() {
			if want[id] {
				found = append(found, id)
			}
		}
		if len(found) == 1 {
			return found[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no single leader elected among %v within %s", ids, timeout)
	return ""
}

// appliedCount returns how many entries node id has applied so far.
func (c *cluster) appliedCount(id raft.PeerID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.applied[id])
}

// hasSnapshot reports whether id has ever received a SnapshotValid
// ApplyMsg - i.e. it caught up via InstallSnapshot rather than plain
// AppendEntries.
func (c *cluster) hasSnapshot(id raft.PeerID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, msg := range c.applied[id] {
		if msg.SnapshotValid {
			return true
		}
	}
	return false
}

// generic poll-until helper for the handful of things above that
// don't fit waitForLeader/waitForLeaderAmong.
func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
