package raft

import "context"

// TakeSnapshot is called by the KV server once it decides the log has
// grown large enough to compact (e.g. every N applied entries - see
// server/server.go). snapshotData is an opaque, fully-serialized copy
// of the state machine as of lastIncludedIndex.
//
// TODO(core): unimplemented. You need to:
//  1. Discard log entries up to and including lastIncludedIndex,
//     recording lastIncludedIndex/lastIncludedTerm on the log struct
//     so toArrayIndex/lastIndex/lastTerm keep working (see state.go).
//  2. Persist the snapshot + the trimmed log via n.persister so a
//     restart doesn't need to replay the whole history.
func (n *Node) TakeSnapshot(lastIncludedIndex uint64, snapshotData []byte) {
}

// sendInstallSnapshot is used by a leader when nextIndex[peer] points
// at an entry the leader has already compacted away - the only way to
// bring that follower up to date is to ship it the whole snapshot.
//
// TODO(core): unimplemented. Mirrors replicateToPeer but for the
// InstallSnapshot RPC: on success, advance both nextIndex[peer] and
// matchIndex[peer] to lastIncludedIndex+1 / lastIncludedIndex.
func (n *Node) sendInstallSnapshot(peer PeerID) {
	_, _ = context.Background(), peer
}

// HandleInstallSnapshot implements the InstallSnapshot RPC
// (Raft §7), called on a follower that has fallen far enough behind
// that the leader no longer has the entries it needs in its log.
//
// TODO(core): unimplemented. Must:
//  1. Reply immediately with the current term if args.Term is stale.
//  2. Save the snapshot data via n.persister.
//  3. If the snapshot covers a prefix of this node's own log,
//     discard just that prefix and keep any newer entries; if it
//     covers more than the whole log, discard everything.
//  4. Deliver the snapshot to the state machine on applyCh
//     (ApplyMsg.SnapshotValid = true) so the KV store can load it.
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &InstallSnapshotReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
		reply.Term = n.currentTerm
	}
	return reply
}
