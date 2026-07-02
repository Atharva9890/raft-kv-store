# raft-kv-store

A distributed key-value store built on Raft consensus, implemented from scratch in Go — leader election, log replication, and snapshotting over gRPC, with a Docker Compose cluster for local testing.

## Status: scaffold, not yet a working consensus algorithm

This repo is fully wired — proto definitions, gRPC transport, a 5-node Docker cluster, an in-memory test harness that can simulate partitions and crashes — but the actual Raft decision logic (who wins an election, how the log gets replicated, how commits advance) is deliberately left as `TODO`-marked stubs in [`raft/election.go`](raft/election.go), [`raft/log.go`](raft/log.go), and [`raft/snapshot.go`](raft/snapshot.go). Every stub links back to the relevant section of the [Raft paper](https://raft.github.io/raft.pdf) (Figure 2 mostly).

Why ship it like this instead of finishing it first: implementing the core algorithm is the whole point of a project like this, both as a learning exercise and as something to speak to in an interview convincingly. A scaffold you understand cold beats a finished repo you copy-pasted the hard part of. See [`docs/architecture.md`](docs/architecture.md) for what "done" looks like and the TODO checklist below for the implementation order.

Confirmed working today: the gRPC plumbing, service wiring, and Docker cluster are real — five containers on a real Docker network exchange real `RequestVote` RPCs and correctly step down on higher terms (see the "what's already verified" section below). What's missing is the decision logic layered on top.

## Architecture

```
                    ┌─────────────────────────────────────────────┐
                    │                 5-node cluster                │
                    │                                                │
   client ──gRPC──▶ │  ┌────────┐   gRPC (Raft RPCs)   ┌────────┐  │
   (kvctl)          │  │ node1  │◀────────────────────▶│ node2  │  │
                    │  │        │◀──────────┐  ┌───────▶│        │  │
                    │  └───┬────┘           │  │        └───┬────┘  │
                    │      │        ┌───────▼──▼───┐        │       │
                    │      └───────▶│    node3     │◀───────┘       │
                    │               └───────┬──────┘                │
                    │                       │                       │
                    │               ┌───────▼──────┐                │
                    │               │  node4/node5 │                │
                    │               └──────────────┘                │
                    └─────────────────────────────────────────────┘

Inside each node:

  gRPC listener (:9001)
   ├── KV service     ──▶ server.Server   ──▶ propose(op) ──▶ raft.Node.Propose
   │                                                              │
   └── Raft service   ──▶ server.RaftService ──▶ raft.Node.Handle{RequestVote,AppendEntries,InstallSnapshot}
                                                                  │
                                                        ┌─────────▼─────────┐
                                                        │     raft.Node      │
                                                        │  role, term, log   │
                                                        │  election timer    │
                                                        │  heartbeat loop    │
                                                        └─────────┬─────────┘
                                                                  │ applyCh (committed entries)
                                                                  ▼
                                                        server.KVStore (map[string]string)
```

Every client request (`Get`/`Put`/`Delete`) is encoded as an `Op` and proposed to Raft — including reads. That's what makes reads linearizable: a `Get` can't return stale data just because it landed on a node that hasn't caught up, because it has to go through the same log ordering as every write. See [`server/kv_store.go`](server/kv_store.go).

## Quickstart

```bash
docker compose up --build
```

That starts 5 nodes (`node1`..`node5`) on ports `9101`-`9105`. In another terminal, once the election/log TODOs below are implemented:

```bash
go run ./cmd/kvctl put foo bar
go run ./cmd/kvctl get foo
```

Watch leader election happen live:

```bash
docker compose logs -f | grep role=
```

Kill the current leader and watch the rest of the cluster re-elect (also depends on the TODOs being implemented — see [`scripts/kill_leader.sh`](scripts/kill_leader.sh)):

```bash
./scripts/kill_leader.sh
```

## What's already verified

Run `go test ./tests/...` — every test currently fails, and they're supposed to: they assert on the exact behavior the TODOs below produce once implemented (single leader elected, entries replicated to all nodes, minority partitions can't make progress, cluster survives a leader crash without losing committed data). They're the acceptance criteria, not a bug.

What *is* confirmed working, independent of the TODOs:
- `go build ./...` and `go vet ./...` are clean.
- A 5-node Docker Compose cluster builds and starts.
- Nodes exchange real `RequestVote` RPCs over the Docker network and correctly step down when they see a higher term (visible in `docker compose logs` as terms climbing in lockstep across all 5 containers).
- The KV gRPC service, the internal Raft gRPC service, the client CLI with leader-redirect retry logic, and the in-memory test harness (which can simulate partitions and crashes without touching Docker) all compile and run correctly against the stubbed `raft.Node`.

## Implementation checklist (do these in order)

1. **Leader election** — [`raft/election.go`](raft/election.go)
   - `startElection`: count granted votes, become leader on a majority, step down on a higher term.
   - `HandleRequestVote`: grant a vote only if the candidate's log is at least as up-to-date as yours (§5.4.1) and you haven't already voted this term.
   - Run `go test ./tests/ -run TestSingleLeaderElected -v` until it passes.
2. **Log replication** — [`raft/log.go`](raft/log.go)
   - `replicateToPeer`: advance `nextIndex`/`matchIndex` on success, back off on conflict, step down on a higher term.
   - `HandleAppendEntries`: the consistency check + merge (Figure 2 steps 2-5).
   - `advanceCommitIndexLocked`: majority-match commit advancement, respecting the current-term restriction (§5.4.2).
   - Also add the no-op-entry-on-election-win TODO in `becomeLeaderLocked` (`raft/election.go`) — needed for `advanceCommitIndexLocked` to ever move past a prior term.
   - Run `go test ./tests/ -run TestProposedEntryReplicatesToAllNodes -v`.
3. **Partition tolerance** — should fall out of 1+2 automatically if the majority-quorum logic is correct. Verify with `go test ./tests/ -run Partition -v`.
4. **Crash recovery / durability** — [`raft/state.go`](raft/state.go)'s `Persister` interface currently has only an in-memory, non-durable implementation (`raft/persister_memory.go`). Replace it with something that survives a process restart (an append-only log file with `fsync`, or embed BoltDB/SQLite) before treating "kill a node, it comes back correctly" as proven rather than assumed.
5. **Snapshotting** — [`raft/snapshot.go`](raft/snapshot.go). Lowest priority; only matters once the log has grown large enough that replaying it from scratch is impractical.
6. **(Stretch) Membership changes** — see the TODO on `raft.Config` in [`raft/types.go`](raft/types.go). Static membership loaded from `PEERS` today; joint consensus (§6) would let nodes join/leave a live cluster.

## Design decisions

- **Reads go through the log too.** Simpler to reason about and test than a separate read-index/lease-read fast path, at the cost of every `Get` paying full consensus latency. A production system chasing lower read latency would implement the ReadIndex optimization from the Raft paper instead.
- **One gRPC listener per node, two services.** `KV` (client-facing) and `Raft` (internal peer-to-peer) share a port because that's one less thing to configure per container in Compose. A real deployment would likely put mTLS + a separate internal network in front of the `Raft` service instead of trusting anything that can reach the port.
- **`raft.Transport` is an interface, not a concrete gRPC type.** This is what lets [`tests/harness.go`](tests/harness.go) simulate a partitioned network in-process, in milliseconds, instead of every test needing a live Docker cluster.
- **JSON encoding for log commands**, not protobuf or gob. Slower and bigger on the wire than either, but you can `cat` a log entry and read it, which matters more for a project meant to be understood than for one meant to be fast.
- **What I'd do differently at scale:** batch multiple client requests into a single `AppendEntries` round instead of one RPC per proposal, add the ReadIndex optimization for reads, and replace the static `PEERS` config with real membership changes so the cluster can be resized without a full restart.

## Repo layout

```
raft/           core consensus algorithm (transport-agnostic, unit-testable without gRPC)
server/         gRPC services (KV + internal Raft) and the KV state machine
client/         Go client library with leader-redirect retry logic
cmd/node/       node binary entrypoint
cmd/kvctl/      CLI client binary
config/         cluster membership loaded from environment variables
proto/          gRPC/protobuf schema + generated code
tests/          in-memory cluster harness + partition/failure/election/log tests
docs/           architecture notes
scripts/        kill_leader.sh for the re-election demo
```
