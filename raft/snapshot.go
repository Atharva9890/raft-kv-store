package raft

import "context"

// called by the KV layer once it decides the log's grown big enough
// to compact (server/server.go does this every N applied entries).
// snapshotData is the state machine fully serialized as of
// lastIncludedIndex - I don't know or care what's inside it.
func (n *Node) TakeSnapshot(lastIncludedIndex uint64, snapshotData []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if lastIncludedIndex <= n.log.lastIncludedIndex {
		return // already snapshotted at least this far, nothing to do
	}

	var lastIncludedTerm uint64
	if entry, ok := n.log.at(lastIncludedIndex); ok {
		lastIncludedTerm = entry.Term
	} else if lastIncludedIndex == n.log.lastIndex() {
		lastIncludedTerm = n.log.lastTerm()
	} else {
		return // don't actually have that entry, can't safely snapshot there
	}

	if i, ok := n.log.toArrayIndex(lastIncludedIndex); ok {
		n.log.entries = append([]LogEntry(nil), n.log.entries[i+1:]...)
	} else {
		n.log.entries = nil
	}
	n.log.lastIncludedIndex = lastIncludedIndex
	n.log.lastIncludedTerm = lastIncludedTerm

	_ = n.persister.SaveSnapshot(snapshotData, lastIncludedIndex, lastIncludedTerm)
	n.persistLocked()
}

// used when a follower's nextIndex points at something I've already
// compacted away - the log can't help it catch up anymore, so it
// needs the whole snapshot instead.
func (n *Node) sendInstallSnapshot(peer PeerID) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	n.mu.Unlock()

	data, lastIncludedIndex, lastIncludedTerm, err := n.persister.LoadSnapshot()
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), HeartbeatInterval*4)
	defer cancel()
	reply, err := n.transport.SendInstallSnapshot(ctx, peer, &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          n.id,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	})
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	if n.role != Leader || n.currentTerm != term {
		return
	}
	if lastIncludedIndex+1 > n.nextIndex[peer] {
		n.nextIndex[peer] = lastIncludedIndex + 1
	}
	if lastIncludedIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = lastIncludedIndex
	}
}

// the InstallSnapshot handler (§7), for a follower that's fallen far
// enough behind that the leader no longer has the entries it needs.
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()

	reply := &InstallSnapshotReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		n.mu.Unlock()
		return reply
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
		reply.Term = n.currentTerm
	}
	n.role = Follower
	n.leaderHint = args.LeaderID
	n.resetElectionTimer()

	if args.LastIncludedIndex <= n.log.lastIncludedIndex {
		n.mu.Unlock()
		return reply // I've already got a snapshot at least this recent
	}

	// keep anything newer than the snapshot that I happen to already
	// have, ditch the rest
	if entry, ok := n.log.at(args.LastIncludedIndex); ok && entry.Term == args.LastIncludedTerm {
		i, _ := n.log.toArrayIndex(args.LastIncludedIndex)
		n.log.entries = append([]LogEntry(nil), n.log.entries[i+1:]...)
	} else {
		n.log.entries = nil
	}
	n.log.lastIncludedIndex = args.LastIncludedIndex
	n.log.lastIncludedTerm = args.LastIncludedTerm

	if n.commitIndex < args.LastIncludedIndex {
		n.commitIndex = args.LastIncludedIndex
	}
	if n.lastApplied < args.LastIncludedIndex {
		n.lastApplied = args.LastIncludedIndex
	}

	_ = n.persister.SaveSnapshot(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)
	n.persistLocked()
	n.mu.Unlock()

	// applyLoop only ever hands out entries above lastApplied, so this
	// won't race with it re-delivering something from before the snapshot
	n.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}

	return reply
}
