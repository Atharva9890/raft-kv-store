package server

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Atharva9890/raft-kv-store/proto/kvpb"
	"github.com/Atharva9890/raft-kv-store/raft"
)

// the real raft.Transport, over gRPC - dials each peer lazily and
// caches the connection. this is the only spot in the whole project
// where raft/ actually touches a network, since everything in there
// only ever talks to the Transport interface. that's also what lets
// tests/ swap in an in-memory fake and run the whole suite without
// gRPC in the loop at all.
type GRPCTransport struct {
	addrs map[raft.PeerID]string

	mu    sync.Mutex
	conns map[raft.PeerID]kvpb.RaftClient
}

func NewGRPCTransport(addrs map[raft.PeerID]string) *GRPCTransport {
	return &GRPCTransport{
		addrs: addrs,
		conns: make(map[raft.PeerID]kvpb.RaftClient),
	}
}

func (t *GRPCTransport) client(peer raft.PeerID) (kvpb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if c, ok := t.conns[peer]; ok {
		return c, nil
	}
	addr, ok := t.addrs[peer]
	if !ok {
		return nil, fmt.Errorf("unknown peer %q", peer)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := kvpb.NewRaftClient(conn)
	t.conns[peer] = client
	return client, nil
}

func (t *GRPCTransport) SendRequestVote(ctx context.Context, peer raft.PeerID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	resp, err := c.RequestVote(ctx, &kvpb.RequestVoteRequest{
		Term:         args.Term,
		CandidateId:  string(args.CandidateID),
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return nil, err
	}
	return &raft.RequestVoteReply{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (t *GRPCTransport) SendAppendEntries(ctx context.Context, peer raft.PeerID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	entries := make([]*kvpb.LogEntry, 0, len(args.Entries))
	for _, e := range args.Entries {
		entries = append(entries, &kvpb.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command})
	}
	resp, err := c.AppendEntries(ctx, &kvpb.AppendEntriesRequest{
		Term:         args.Term,
		LeaderId:     string(args.LeaderID),
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}
	return &raft.AppendEntriesReply{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictIndex: resp.ConflictIndex,
		ConflictTerm:  resp.ConflictTerm,
	}, nil
}

func (t *GRPCTransport) SendInstallSnapshot(ctx context.Context, peer raft.PeerID, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	resp, err := c.InstallSnapshot(ctx, &kvpb.InstallSnapshotRequest{
		Term:              args.Term,
		LeaderId:          string(args.LeaderID),
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	})
	if err != nil {
		return nil, err
	}
	return &raft.InstallSnapshotReply{Term: resp.Term}, nil
}
