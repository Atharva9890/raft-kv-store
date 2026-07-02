package raft

import "context"

// fires off AppendEntries to every peer in parallel - either a real
// batch of new entries or an empty heartbeat. called on every
// heartbeat tick, and also right after Propose so a new write doesn't
// have to wait around for the next tick.
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

// sends whatever a single follower needs to catch up, starting at
// nextIndex[peer], and reconciles the reply. this is where most of
// the actual "make replication converge" logic lives.
func (n *Node) replicateToPeer(peer PeerID, term uint64, leaderCommit uint64) {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}

	// if I've already compacted away the entries this follower needs,
	// a snapshot is the only way to get it caught up
	if n.nextIndex[peer] <= n.log.lastIncludedIndex {
		n.mu.Unlock()
		n.sendInstallSnapshot(peer)
		return
	}

	prevLogIndex := n.nextIndex[peer] - 1
	var prevLogTerm uint64
	if prevLogIndex == n.log.lastIncludedIndex {
		prevLogTerm = n.log.lastIncludedTerm
	} else if e, ok := n.log.at(prevLogIndex); ok {
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

	reply, err := n.transport.SendAppendEntries(ctx, peer, &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	})
	if err != nil {
		return // couldn't reach it this round, next heartbeat tick will retry
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader || n.currentTerm != term {
		return // I'm not in charge of this term anymore, none of this matters
	}

	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}

	if reply.Success {
		matched := prevLogIndex + uint64(len(entries))
		if matched > n.matchIndex[peer] {
			n.matchIndex[peer] = matched
			n.nextIndex[peer] = matched + 1
			n.advanceCommitIndexLocked()
		}
		return
	}

	// follower rejected me - use its conflict hint to jump back to
	// roughly the right spot instead of decrementing nextIndex one
	// entry at a time, which gets painfully slow on a long divergence
	if reply.ConflictTerm == 0 {
		// its log just doesn't reach PrevLogIndex yet
		n.nextIndex[peer] = reply.ConflictIndex
	} else {
		// see if I've got anything from ConflictTerm myself - if so,
		// retry right after the last entry I have in that term
		next := reply.ConflictIndex
		for idx := n.log.lastIndex(); idx > n.log.lastIncludedIndex; idx-- {
			if e, ok := n.log.at(idx); ok && e.Term == reply.ConflictTerm {
				next = idx + 1
				break
			}
		}
		n.nextIndex[peer] = next
	}
	if n.nextIndex[peer] < 1 {
		n.nextIndex[peer] = 1
	}
}

// the AppendEntries handler, called on a follower when its leader
// sends a heartbeat or new entries. this is Figure 2's consistency
// check plus the merge.
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
	// a legit AppendEntries from the current leader means I shouldn't
	// be trying to start my own election right now
	n.role = Follower
	n.leaderHint = args.LeaderID
	n.resetElectionTimer()
	reply.Term = n.currentTerm

	// my log doesn't even reach PrevLogIndex yet
	if args.PrevLogIndex > n.log.lastIndex() {
		reply.Success = false
		reply.ConflictIndex = n.log.lastIndex() + 1
		reply.ConflictTerm = 0
		return reply
	}

	// I've got PrevLogIndex, but from a different term - I'm on a
	// branch of history that diverges from the leader's
	if args.PrevLogIndex > n.log.lastIncludedIndex {
		if e, ok := n.log.at(args.PrevLogIndex); ok && e.Term != args.PrevLogTerm {
			conflictTerm := e.Term
			conflictIndex := args.PrevLogIndex
			for idx := args.PrevLogIndex; idx > n.log.lastIncludedIndex; idx-- {
				entry, ok := n.log.at(idx)
				if !ok || entry.Term != conflictTerm {
					break
				}
				conflictIndex = idx
			}
			reply.Success = false
			reply.ConflictIndex = conflictIndex
			reply.ConflictTerm = conflictTerm
			return reply
		}
	} else if args.PrevLogIndex == n.log.lastIncludedIndex && n.log.lastIncludedIndex > 0 && args.PrevLogTerm != n.log.lastIncludedTerm {
		// PrevLogIndex lands right on my snapshot boundary and the
		// terms don't line up - shouldn't come up in practice but I'd
		// rather reject than silently accept something wrong
		reply.Success = false
		reply.ConflictIndex = args.PrevLogIndex
		reply.ConflictTerm = args.PrevLogTerm
		return reply
	}

	// merge: keep what already matches, overwrite what doesn't,
	// append what's new
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + uint64(i) + 1
		if existing, ok := n.log.at(idx); ok {
			if existing.Term != entry.Term {
				n.log.truncateFrom(idx)
				n.log.append(entry)
			}
		} else {
			n.log.append(entry)
		}
	}
	n.persistLocked()

	if args.LeaderCommit > n.commitIndex {
		lastNew := args.PrevLogIndex + uint64(len(args.Entries))
		if args.LeaderCommit < lastNew {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNew
		}
	}

	reply.Success = true
	return reply
}

// recomputes commitIndex from matchIndex after a successful
// replication. must be called with n.mu already held. walking down
// from the top means the first index I find that qualifies is the
// highest one, so I can stop right there.
func (n *Node) advanceCommitIndexLocked() {
	for idx := n.log.lastIndex(); idx > n.commitIndex; idx-- {
		entry, ok := n.log.at(idx)
		if !ok || entry.Term != n.currentTerm {
			// §5.4.2 / Figure 8: I can only commit an entry directly
			// if it's from my own term. older entries only get
			// committed as a side effect of a same-term entry above
			// them getting a majority.
			continue
		}

		count := 1 // I've got it, I'm the leader
		for _, p := range n.peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count*2 > len(n.peers)+1 {
			n.commitIndex = idx
			return
		}
	}
}
