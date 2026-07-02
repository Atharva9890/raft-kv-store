package server

import (
	"context"
	"errors"

	"github.com/Atharva9890/raft-kv-store/proto/kvpb"
)

func (s *Server) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	res, err := s.propose(ctx, Op{Type: OpGet, Key: req.Key})
	if errors.Is(err, ErrNotLeader) {
		return &kvpb.GetResponse{NotLeader: true, LeaderHint: s.notLeaderHint()}, nil
	}
	if err != nil {
		return nil, err
	}
	return &kvpb.GetResponse{Found: res.found, Value: res.value}, nil
}

func (s *Server) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	_, err := s.propose(ctx, Op{Type: OpPut, Key: req.Key, Value: req.Value})
	if errors.Is(err, ErrNotLeader) {
		return &kvpb.PutResponse{NotLeader: true, LeaderHint: s.notLeaderHint()}, nil
	}
	if err != nil {
		return nil, err
	}
	return &kvpb.PutResponse{Ok: true}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	_, err := s.propose(ctx, Op{Type: OpDelete, Key: req.Key})
	if errors.Is(err, ErrNotLeader) {
		return &kvpb.DeleteResponse{NotLeader: true, LeaderHint: s.notLeaderHint()}, nil
	}
	if err != nil {
		return nil, err
	}
	return &kvpb.DeleteResponse{Ok: true}, nil
}
