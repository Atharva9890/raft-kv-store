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

type applyResult struct {
	msg   raft.ApplyMsg
	value string
	found bool
}

// Server implements the client-facing KV gRPC service on top of a
// raft.Node. It is the piece that turns "append this to a replicated
// log" into "here is your answer, or go ask node X instead."
type Server struct {
	kvpb.UnimplementedKVServer

	node *raft.Node
	sm   *KVStore

	mu      sync.Mutex
	waiters map[uint64]chan applyResult // log index -> notify channel
}

func NewServer(node *raft.Node, sm *KVStore, applyCh chan raft.ApplyMsg) *Server {
	s := &Server{
		node:    node,
		sm:      sm,
		waiters: make(map[uint64]chan applyResult),
	}
	go s.applyLoop(applyCh)
	return s
}

func (s *Server) applyLoop(applyCh chan raft.ApplyMsg) {
	for msg := range applyCh {
		switch {
		case msg.CommandValid:
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

		case msg.SnapshotValid:
			_ = s.sm.Restore(msg.Snapshot)
		}
	}
}

// propose submits op to raft and blocks until the entry Propose
// appended has actually been committed and applied - or until this
// node turns out not to be the leader after all, or a timeout elapses.
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
			// The entry at this index belongs to a different term
			// than the one we proposed under, which means a new
			// leader overwrote it before it committed. Our command
			// was never actually applied - the caller should retry.
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
	return string(s.node.LeaderHint())
}
