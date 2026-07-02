# Architecture

## Role state machine

Every node is always in exactly one of three roles. Transitions are driven by election timeouts, votes, and RPCs carrying a higher term than the node has seen.

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

Implemented in [`raft/election.go`](../raft/election.go): `becomeFollowerLocked`, `becomeCandidateLocked`, `becomeLeaderLocked`. The transitions themselves are wired up; what's still a TODO is the vote-counting and majority-detection logic that actually *triggers* the Candidate → Leader edge.

## Election sequence (target behavior once TODOs are implemented)

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
      │                    │                   │
      │◀───── AppendEntries (heartbeat) ───────┤
      │◀───────────────────┼───────────────────┤
```

## Log replication sequence (target behavior)

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

The client only gets its answer after the entry is *committed* (replicated to a majority), not merely appended locally by the leader — see `server.Server.propose` in [`server/server.go`](../server/server.go). This is what prevents a client from being told "OK" for a write that a leader-crash-before-replication could then lose.

## Why a no-op entry on election win matters

Raft's commit rule (§5.4.2) is subtle: a leader can only conclude an entry is committed by counting replicas of an entry **from its own current term** — not by directly counting replicas of older entries, even if a majority already has them. A freshly-elected leader with no new writes yet has nothing from its own term to count, so it's stuck unable to advance `commitIndex` past whatever the previous leader left committed, until either a real client write arrives or (more robustly) it commits an empty no-op entry immediately upon election. This is flagged as a TODO in `becomeLeaderLocked` — easy to miss, and the kind of thing that makes a naive implementation *look* correct in casual testing while failing the harder partition/crash tests in `tests/`.

## Data flow: a `Get` end to end

1. `kvctl get foo` → `client.Client.Get` dials a node from its address list.
2. If that node isn't the leader, it replies `NotLeader` with a `LeaderHint`; the client redirects and retries (`client/client.go`).
3. The leader encodes `Op{Type: Get, Key: "foo"}` as JSON and calls `raft.Node.Propose` — yes, even for reads (see "Design decisions" in the README for why).
4. Once a majority of nodes have replicated and the leader's `commitIndex` advances past this entry, `applyLoop` (`raft/raft.go`) delivers it on `applyCh`.
5. `server.Server.applyLoop` applies the op to `KVStore` and wakes up the goroutine blocked in `propose()`, which returns the value read at that point in the log's total order.

## What "done" looks like

A cluster where: exactly one leader exists per term; every committed entry is present, in the same order, on every node that hasn't crashed; a minority partition can never elect a leader or accept writes; killing the leader mid-write results in either the write being visible on every surviving node or the client being told it failed — never a silent loss of an acknowledged write. The tests in [`tests/`](../tests/) assert exactly these properties.
