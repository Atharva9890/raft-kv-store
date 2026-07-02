package server

import (
	"context"

	"github.com/Atharva9890/raft-kv-store/proto/kvpb"
	"github.com/Atharva9890/raft-kv-store/raft"
)

// adapts the internal peer-to-peer gRPC service (the Raft service in
// proto/kv.proto) onto a raft.Node's Handle* methods. I kept this
// separate from Server (the client-facing KV service) because even
// though they share a listener, they're serving completely different
// audiences - clients vs. other cluster members.
type RaftService struct {
	kvpb.UnimplementedRaftServer
	node *raft.Node
}

func NewRaftService(node *raft.Node) *RaftService {
	return &RaftService{node: node}
}

func (r *RaftService) RequestVote(ctx context.Context, req *kvpb.RequestVoteRequest) (*kvpb.RequestVoteResponse, error) {
	reply := r.node.HandleRequestVote(&raft.RequestVoteArgs{
		Term:         req.Term,
		CandidateID:  raft.PeerID(req.CandidateId),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	return &kvpb.RequestVoteResponse{
		Term:        reply.Term,
		VoteGranted: reply.VoteGranted,
	}, nil
}

func (r *RaftService) AppendEntries(ctx context.Context, req *kvpb.AppendEntriesRequest) (*kvpb.AppendEntriesResponse, error) {
	entries := make([]raft.LogEntry, 0, len(req.Entries))
	for _, e := range req.Entries {
		entries = append(entries, raft.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command})
	}
	reply := r.node.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term:         req.Term,
		LeaderID:     raft.PeerID(req.LeaderId),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	})
	return &kvpb.AppendEntriesResponse{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictIndex: reply.ConflictIndex,
		ConflictTerm:  reply.ConflictTerm,
	}, nil
}

func (r *RaftService) InstallSnapshot(ctx context.Context, req *kvpb.InstallSnapshotRequest) (*kvpb.InstallSnapshotResponse, error) {
	reply := r.node.HandleInstallSnapshot(&raft.InstallSnapshotArgs{
		Term:              req.Term,
		LeaderID:          raft.PeerID(req.LeaderId),
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
	})
	return &kvpb.InstallSnapshotResponse{Term: reply.Term}, nil
}
