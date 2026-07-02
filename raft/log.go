package raft

import "context"

// broadcastAppendEntries sends AppendEntries (heartbeat or with new
// entries) to every peer in parallel. Called on every heartbeat tick
// by heartbeatLoop, and should also be triggered immediately after
// Propose appends a new entry (see the TODO in raft.go Propose).
func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	leaderCommit := n.commitIndex
	peers := append([]PeerID(nil), n.peers...)
	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicateToPeer(peer, term, leaderCommit)
	}
}

// replicateToPeer sends this leader's view of the log starting at
// nextIndex[peer] to a single follower, and reconciles the reply.
//
// TODO(core): this is the heart of log replication and is currently
// unimplemented beyond the RPC plumbing. You need to:
//  1. Read nextIndex[peer] (locked) to compute prevLogIndex/prevLogTerm
//     and the slice of entries to send (everything from nextIndex
//     onward - may be empty, i.e. a pure heartbeat).
//  2. On success: advance matchIndex[peer] and nextIndex[peer] to
//     reflect what was just replicated, then call
//     advanceCommitIndexLocked() since a majority match might have
//     just been reached.
//  3. On failure due to log inconsistency (reply.Success == false but
//     same term): decrement nextIndex[peer] and retry - or better,
//     use reply.ConflictIndex/ConflictTerm to jump back directly to
//     the follower's actual divergence point (Raft §5.3, "fast
//     backtracking") instead of one entry at a time.
//  4. On a reply with a higher term: step down via
//     becomeFollowerLocked and stop - you're not the leader anymore.
func (n *Node) replicateToPeer(peer PeerID, term uint64, leaderCommit uint64) {
	n.mu.Lock()
	prevLogIndex := n.nextIndex[peer] - 1
	prevLogTerm := uint64(0)
	if e, ok := n.log.at(prevLogIndex); ok {
		prevLogTerm = e.Term
	}
	var entries []LogEntry
	for idx := n.nextIndex[peer]; idx <= n.log.lastIndex(); idx++ {
		if e, ok := n.log.at(idx); ok {
			entries = append(entries, e)
		}
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), HeartbeatInterval*2)
	defer cancel()

	_, err := n.transport.SendAppendEntries(ctx, peer, &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	})
	if err != nil {
		return
	}

	// TODO(core): see doc comment above - the reply is currently
	// discarded, so nextIndex/matchIndex never advance and
	// commitIndex never moves past whatever a majority already had
	// when the leader was elected.
}

// HandleAppendEntries implements the AppendEntries RPC (Raft §5.3,
// Figure 2), called by a follower when its leader sends a heartbeat
// or new entries.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &AppendEntriesReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		reply.Success = false
		return reply
	}

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	// A valid AppendEntries from the current term's leader means this
	// node should not start its own election.
	n.role = Follower
	n.resetElectionTimer()
	reply.Term = n.currentTerm

	// TODO(core): implement the log consistency check and merge,
	// Figure 2 steps 2-5:
	//  1. Reply false if log doesn't contain an entry at
	//     PrevLogIndex whose term matches PrevLogTerm (set
	//     ConflictIndex/ConflictTerm to help the leader backtrack
	//     fast - see log.go replicateToPeer TODO #3).
	//  2. If an existing entry conflicts with a new one (same index,
	//     different term), delete it and everything after it
	//     (log.truncateFrom).
	//  3. Append any new entries not already in the log.
	//  4. If LeaderCommit > commitIndex, set commitIndex =
	//     min(LeaderCommit, index of last new entry) - this is what
	//     lets applyLoop start delivering entries to the state
	//     machine.
	reply.Success = false
	return reply
}

// advanceCommitIndexLocked recomputes commitIndex from matchIndex
// once replication progress changes. Must be called with n.mu held.
//
// TODO(core): find the highest N such that N > commitIndex, a
// majority of matchIndex[peer] >= N (plus the leader's own log, which
// implicitly "matches" itself up to lastIndex), AND log[N].Term ==
// currentTerm (the term-matching requirement is what the no-op-entry
// TODO in election.go exists to satisfy - see Raft §5.4.2). If found,
// set n.commitIndex = N.
func (n *Node) advanceCommitIndexLocked() {
}
