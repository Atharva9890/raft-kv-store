package server

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Atharva9890/raft-kv-store/proto/kvpb"
	"github.com/Atharva9890/raft-kv-store/raft"
)

var (
	ErrTimeout   = errors.New("timed out waiting for command to commit")
	ErrNotLeader = errors.New("this node is not the leader")
)

const proposeTimeout = 5 * time.Second

// how often (in applied entries) I ask Raft to compact the log. low
// enough that it's easy to actually see happen in a demo, not so low
// that I'm snapshotting on every single write.
const snapshotEvery = 50

type applyResult struct {
	msg   raft.ApplyMsg
	value string
	found bool
}

// wraps a raft.Node with the client-facing KV gRPC service. this is
// the piece that turns "append this to a replicated log" into "here's
// your answer, or go ask node X instead."
type Server struct {
	kvpb.UnimplementedKVServer

	node        *raft.Node
	sm          *KVStore
	publicAddrs map[raft.PeerID]string // for translating LeaderHint() into something a client can actually dial

	mu      sync.Mutex
	waiters map[uint64]chan applyResult // log index -> whoever's waiting on it
}

func NewServer(node *raft.Node, sm *KVStore, publicAddrs map[raft.PeerID]string, applyCh chan raft.ApplyMsg) *Server {
	s := &Server{
		node:        node,
		sm:          sm,
		publicAddrs: publicAddrs,
		waiters:     make(map[uint64]chan applyResult),
	}
	go s.applyLoop(applyCh)
	return s
}

func (s *Server) applyLoop(applyCh chan raft.ApplyMsg) {
	appliedSinceSnapshot := 0
	for msg := range applyCh {
		if msg.CommandValid {
			op, err := DecodeOp(msg.Command)
			var value string
			var found bool
			if err == nil {
				value, found = s.sm.Apply(op)
			}
			s.mu.Lock()
			if ch, ok := s.waiters[msg.CommandIndex]; ok {
				ch <- applyResult{msg: msg, value: value, found: found}
				delete(s.waiters, msg.CommandIndex)
			}
			s.mu.Unlock()

			appliedSinceSnapshot++
			if appliedSinceSnapshot >= snapshotEvery {
				appliedSinceSnapshot = 0
				if data, err := s.sm.Snapshot(); err == nil {
					s.node.TakeSnapshot(msg.CommandIndex, data)
				}
			}
		}

		if msg.SnapshotValid {
			_ = s.sm.Restore(msg.Snapshot)
		}
	}
}

// submits op to Raft and blocks until it's actually committed and
// applied - not just appended by whoever I currently think is the
// leader, since that's not a promise Raft actually makes.
func (s *Server) propose(ctx context.Context, op Op) (applyResult, error) {
	encoded, err := EncodeOp(op)
	if err != nil {
		return applyResult{}, err
	}

	index, term, isLeader := s.node.Propose(encoded)
	if !isLeader {
		return applyResult{}, ErrNotLeader
	}

	ch := make(chan applyResult, 1)
	s.mu.Lock()
	s.waiters[index] = ch
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, proposeTimeout)
	defer cancel()

	select {
	case res := <-ch:
		if res.msg.CommandTerm != term {
			// whatever landed at this index came from a different
			// term than the one I proposed under - a new leader must
			// have overwritten it before it committed. my command
			// never made it in, so the caller needs to retry.
			return applyResult{}, ErrNotLeader
		}
		return res, nil
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.waiters, index)
		s.mu.Unlock()
		return applyResult{}, ErrTimeout
	}
}

func (s *Server) notLeaderHint() string {
	id := s.node.LeaderHint()
	if id == "" {
		return ""
	}
	if addr, ok := s.publicAddrs[id]; ok {
		return addr
	}
	return string(id) // best effort - at least tells you which node, even if not how to dial it
}
