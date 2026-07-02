package raft

import "context"

// RequestVoteArgs/Reply, AppendEntriesArgs/Reply and
// InstallSnapshotArgs/Reply mirror the generated protobuf messages
// (proto/kvpb) but stay transport-agnostic so raft/ has no gRPC
// import and can be unit tested with an in-memory Transport.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  PeerID
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     PeerID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64
	ConflictTerm  uint64
}

type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          PeerID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotReply struct {
	Term uint64
}

// Transport sends RPCs to a named peer. server/ provides a gRPC
// implementation (server/transport_grpc.go); tests/ provides an
// in-memory implementation that can drop/delay/partition messages
// without spinning up real network sockets.
type Transport interface {
	SendRequestVote(ctx context.Context, peer PeerID, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, peer PeerID, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	SendInstallSnapshot(ctx context.Context, peer PeerID, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}
