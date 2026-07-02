package raft

import (
	"context"
	"time"
)

// --- role transitions ---

// becomeFollower steps down to Follower for newTerm. Called whenever
// this node sees a term higher than its own, from any RPC handler.
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
	lastIndex := n.log.lastIndex()
	for _, p := range n.peers {
		n.nextIndex[p] = lastIndex + 1
		n.matchIndex[p] = 0
	}
	// TODO(no-op entry): many Raft implementations commit a no-op
	// entry immediately on election so the new leader can safely
	// advance commitIndex past entries from prior terms (Raft §5.4.2 -
	// a leader can only conclude an entry is committed by counting
	// replicas of an entry from its OWN term). Without this, a fresh
	// leader with no new client writes yet cannot commit anything
	// left over from the previous leader.
}

// --- starting and running an election ---

// startElection is called by the election timer when this node has
// gone too long without hearing from a leader. It transitions to
// Candidate, votes for itself, and asks every peer for their vote in
// parallel.
//
// TODO(core): this function currently only handles the mechanical
// parts (role transition, fan-out, term bookkeeping). You still need
// to:
//  1. Count granted votes as replies arrive and call
//     becomeLeaderLocked once a majority (including your own vote)
//     is reached.
//  2. Abort the election if any reply carries a higher term (step
//     down via becomeFollowerLocked and stop counting).
//  3. Make sure a stale election (from a term that has since moved
//     on) can't win - capture the term you started the election in
//     and check it's still current before acting on each reply.
func (n *Node) startElection() {
	n.mu.Lock()
	n.becomeCandidateLocked()
	term := n.currentTerm
	lastIndex := n.log.lastIndex()
	lastTerm := n.log.lastTerm()
	peers := append([]PeerID(nil), n.peers...)
	n.mu.Unlock()

	n.resetElectionTimer()

	votesCh := make(chan *RequestVoteReply, len(peers))
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
				votesCh <- nil
				return
			}
			votesCh <- reply
		}(peer)
	}

	// TODO(core): replace this stub with real vote counting per the
	// doc comment above. As written this goroutine drains replies but
	// never acts on them, so a candidate will never actually become
	// leader.
	go func() {
		for range peers {
			select {
			case <-votesCh:
			case <-time.After(ElectionTimeoutMax):
				return
			}
		}
	}()
}

// HandleRequestVote implements the RequestVote RPC (Raft §5.2,
// Figure 2). It is called by the gRPC server layer
// (server/raft_service.go) when a peer asks for our vote.
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

	// TODO(core): grant the vote only if BOTH are true:
	//   (a) votedFor is empty or already equal to args.CandidateID
	//       for this term, AND
	//   (b) the candidate's log is at least as up-to-date as ours:
	//       compare (lastLogTerm, lastLogIndex) pairs - higher term
	//       wins outright; equal term means longer log wins. This is
	//       the "election restriction" (Raft §5.4.1) that guarantees
	//       a candidate with a committed entry can never lose an
	//       election to a candidate missing it.
	//
	// Remember to call n.resetElectionTimer() and persistLocked()
	// whenever you actually grant a vote - granting a vote is a
	// promise that must survive a crash, and it should suppress this
	// node's own election timeout so it doesn't immediately compete
	// against the candidate it just voted for.
	reply.VoteGranted = false
	return reply
}
