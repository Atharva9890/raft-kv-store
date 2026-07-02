// Package tests exercises raft.Node in-process, over an in-memory
// fake network instead of gRPC. That's what makes it possible to
// simulate a network partition or a node crash in a few lines and
// run the whole suite in milliseconds - no docker-compose required.
//
// Every test in this package is a target, not a given: against the
// unmodified scaffold (election.go/log.go/snapshot.go TODOs still
// unimplemented) these tests are expected to FAIL or time out. That's
// intentional - they define what "done" looks like.
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

// leaders returns the ids of every node that currently believes it is
// the leader. In a correct implementation this should never have more
// than one member for a given term.
func (c *cluster) leaders() []raft.PeerID {
	var ids []raft.PeerID
	for id, n := range c.nodes {
		if _, isLeader := n.State(); isLeader {
			ids = append(ids, id)
		}
	}
	return ids
}

// waitForLeader polls until exactly one leader emerges or timeout
// elapses, failing the test on timeout.
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

// appliedCount returns how many entries node id has applied so far.
func (c *cluster) appliedCount(id raft.PeerID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.applied[id])
}
