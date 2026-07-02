# Architecture

## Role state machine

Every node is in exactly one of three states. Transitions come from election timeouts, votes, and RPCs carrying a term higher than what a node has seen.

```
                    times out, starts election
        ┌───────────────────────────────────────────┐
        │                                             ▼
   ┌─────────┐   times out /                    ┌───────────┐
   │Follower │──starts election──────────────▶  │ Candidate │
   └─────────┘                                   └─────┬─────┘
        ▲                                               │
        │           sees higher term / valid            │ wins majority
        │           AppendEntries from a leader          │ of votes
        │                                               ▼
        │                                         ┌───────────┐
        └─────────────────────────────────────────│  Leader   │
              sees higher term (steps down)        └───────────┘
```

`raft/election.go` has the transitions: `becomeFollowerLocked`, `becomeCandidateLocked`, `becomeLeaderLocked`.

## Election sequence

```
node1 (follower)   node2 (follower)   node3 (follower)
      │                    │                   │
      │  election timeout  │                   │
      ├───────────────────▶│ becomes candidate │
      │  RequestVote(term=2)                   │
      ├────────────────────┼──────────────────▶│
      │  RequestVote(term=2)                   │
      │◀───────────────────┤                   │
      │  vote granted       │  vote granted     │
      ├────────────────────▶│◀─────────────────┤
      │                    │  majority (2/3)   │
      │                    │  → becomes LEADER │
      │                    │  → commits a no-op│
      │                    │    right away     │
      │◀───── AppendEntries (heartbeat) ───────┤
      │◀───────────────────┼───────────────────┤
```

That no-op commit the instant a leader wins is easy to skip and I did, the first time - see the note below on why it matters.

## Log replication sequence

```
client          leader (node2)       node1            node3
  │  Put(k,v)        │                 │                │
  ├─────────────────▶│                 │                │
  │                   │ append to log  │                │
  │                   │ (uncommitted)  │                │
  │                   ├───────────────▶│ AppendEntries   │
  │                   ├─────────────────────────────────▶│
  │                   │◀───────────────┤ success=true    │
  │                   │◀─────────────────────────────────┤ success=true
  │                   │ majority (3/3) replicated         │
  │                   │ → advance commitIndex             │
  │                   │ → apply to state machine          │
  │◀──────────────────┤ Ok                                │
  │                   ├───────────────▶│ leaderCommit in   │
  │                   ├─────────────────────────────────▶│ next heartbeat
  │                   │                 │ applies too      │ applies too
```

The client only gets an answer once the entry is *committed* (replicated to a majority), not just appended locally by the leader - `server.Server.propose` in `server/server.go` blocks on exactly that. Otherwise a leader could say "OK" and then lose the write in the next election.

## Why the no-op-on-election thing matters

This is the one part of the paper that bit me. Raft's commit rule (§5.4.2, Figure 8) says a leader can only conclude an entry is committed by counting replicas of an entry **from its own current term** - not by directly counting replicas of older entries, even if a majority already has them. A freshly elected leader with no new client writes yet has nothing from its own term to point at, so it's stuck: it can see a majority already has some old entry, but it's not allowed to call that committed on its own authority. Committing an empty no-op the moment it wins gives it something from its own term to anchor to, and from there `advanceCommitIndexLocked` can walk everything below it forward too. Skip this and you get a Raft implementation that looks correct in casual testing (single leader, replicates fine) but silently stalls under exactly the kind of partition/crash scenario the tests in `tests/` are built to catch.

## "Leader" isn't the same as "can make progress"

The other thing that tripped me up, in the tests rather than the implementation itself: a leader that's partitioned into the minority doesn't know it's been cut off. It has no way to find out short of reconnecting or trying to commit something and failing to reach a majority - so it just keeps calling itself leader. My first draft of the partition tests asserted "no minority node should ever report role==Leader," which fails constantly and looks like a Raft bug. It isn't - it's the test asserting something Raft never promised. What Raft actually guarantees is that the minority can never get a *new* entry committed, so that's what `tests/partition_test.go` and `tests/failure_test.go` check now: applied-entry counts before and after, not the role field.

## Data flow: a `Get` end to end

1. `kvctl get foo` dials a node from its address list.
2. If that node isn't the leader, it replies `NotLeader` with a `LeaderHint`; the client redirects and retries (`client/client.go`).
3. The leader encodes `Op{Type: Get, Key: "foo"}` and calls `raft.Node.Propose` - yes, for reads too.
4. Once a majority has replicated it and `commitIndex` moves past it, `applyLoop` in `raft/raft.go` delivers it on `applyCh`.
5. `server.Server.applyLoop` applies the op to `KVStore` and wakes up whoever's blocked in `propose()`, which returns the value read at that point in the log's total order.

## Snapshotting

`server/server.go` calls `raft.Node.TakeSnapshot` every 50 applied entries (see `snapshotEvery`). That compacts the leader's in-memory log up to that point and writes the snapshot to disk via `Persister.SaveSnapshot`. If a follower's `nextIndex` ever points at something the leader has already compacted away, `replicateToPeer` in `raft/log.go` falls back to `sendInstallSnapshot` instead of a normal `AppendEntries` - the log alone can't help that follower catch up anymore, so it needs the whole state machine snapshot shipped over instead.

## What "correct" means here

Exactly one leader can ever get a majority to commit an entry in a given term. Every committed entry ends up in the same order on every node that hasn't crashed. A minority partition can never commit anything new. Killing the leader mid-write either leaves the write visible on every surviving node or tells the client it failed - never silently drops an acknowledged write. `tests/` checks all of these directly, and it's the thing I actually run - not "I clicked around and it seemed fine."
