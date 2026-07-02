package raft

import (
	"context"
)

// step down to follower for newTerm. gets called from any RPC handler
// the moment I see a term bigger than mine - whoever's on that term
// knows something I don't.
func (n *Node) becomeFollowerLocked(newTerm uint64) {
	n.currentTerm = newTerm
	n.votedFor = ""
	n.role = Follower
	n.persistLocked()
}

func (n *Node) becomeCandidateLocked() {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.persistLocked()
}

func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderHint = n.id
	lastIndex := n.log.lastIndex()
	for _, p := range n.peers {
		n.nextIndex[p] = lastIndex + 1
		n.matchIndex[p] = 0
	}

	// committing a blank entry the second I win an election is what
	// lets commitIndex move at all if nobody sends a write for a
	// while. Raft won't let a leader count entries from an older term
	// as committed just because a majority already has them (Figure
	// 8) - it needs a majority on something from its OWN term first.
	// this no-op is the cheapest way to get that.
	noop := LogEntry{Term: n.currentTerm, Index: n.log.lastIndex() + 1, Command: nil}
	n.log.append(noop)
	n.persistLocked()
}

// kicks off an election: become a candidate, vote for myself, ask
// everyone else in parallel. votes is closed over by every goroutine
// below but only ever touched while holding n.mu, so it doesn't need
// its own lock.
func (n *Node) startElection() {
	n.mu.Lock()
	n.becomeCandidateLocked()
	term := n.currentTerm
	lastIndex := n.log.lastIndex()
	lastTerm := n.log.lastTerm()
	peers := append([]PeerID(nil), n.peers...)
	n.mu.Unlock()

	n.resetElectionTimer()

	votes := 1 // I vote for myself the instant I become a candidate

	for _, peer := range peers {
		go func(peer PeerID) {
			ctx, cancel := context.WithTimeout(context.Background(), ElectionTimeoutMin/2)
			defer cancel()
			reply, err := n.transport.SendRequestVote(ctx, peer, &RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// this election might be over by the time the reply gets
			// back - a newer term or a lost race means I should just
			// ignore it
			if n.role != Candidate || n.currentTerm != term {
				return
			}

			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
				return
			}

			if !reply.VoteGranted {
				return
			}

			votes++
			if votes*2 > len(peers)+1 && n.role != Leader {
				n.becomeLeaderLocked()
				go n.broadcastAppendEntries() // let everyone know right away instead of waiting for the next heartbeat tick
			}
		}(peer)
	}
}

// the RequestVote handler - called by the gRPC layer whenever a peer
// wants my vote.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}

	reply := &RequestVoteReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		reply.VoteGranted = false
		return reply
	}

	alreadyVotedForSomeoneElse := n.votedFor != "" && n.votedFor != args.CandidateID

	// the "election restriction" from §5.4.1: I only vote for a
	// candidate whose log is at least as up to date as mine. higher
	// last-log term wins outright; if the terms tie, whoever has the
	// longer log wins. this is the whole mechanism that guarantees a
	// candidate holding a committed entry can never lose an election
	// to one that's missing it.
	candidateUpToDate := args.LastLogTerm > n.log.lastTerm() ||
		(args.LastLogTerm == n.log.lastTerm() && args.LastLogIndex >= n.log.lastIndex())

	if alreadyVotedForSomeoneElse || !candidateUpToDate {
		reply.VoteGranted = false
		return reply
	}

	n.votedFor = args.CandidateID
	n.persistLocked()
	n.resetElectionTimer() // just voted for someone, no reason to also run against them
	reply.VoteGranted = true
	return reply
}
