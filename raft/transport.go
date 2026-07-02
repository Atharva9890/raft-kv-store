package raft

import "context"

// these mirror the protobuf messages in proto/kvpb almost field for
// field, but I kept them as plain structs so raft/ doesn't need to
// import gRPC at all. that's what lets tests/ swap in an in-memory
// fake transport and run the whole suite without a real network.
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
	Term    uint64
	Success bool
	// ConflictIndex/ConflictTerm let the leader skip straight to where
	// my log actually diverges instead of retrying one index at a
	// time - see the backoff logic in log.go replicateToPeer.
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

// anything that can carry these three RPCs to a named peer. server/
// has the real gRPC implementation, tests/ has an in-memory one that
// can drop/partition messages on command.
type Transport interface {
	SendRequestVote(ctx context.Context, peer PeerID, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, peer PeerID, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	SendInstallSnapshot(ctx context.Context, peer PeerID, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}
