# raft-kv-store

[![CI](https://github.com/Atharva9890/raft-kv-store/actions/workflows/ci.yml/badge.svg)](https://github.com/Atharva9890/raft-kv-store/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A distributed key-value store I built from scratch on top of Raft - leader election, log replication, and snapshotting, talking over gRPC, with a 5-node Docker Compose cluster to actually poke at.

I wanted something that would hold up if someone asked me to walk through it in an interview, not a repo where I could wave my hands past the hard part. So this isn't wrapping an existing Raft library - `raft/` is my own implementation of the algorithm, and `tests/` is how I convinced myself it's actually correct rather than "seemed to work when I tried it a couple times."

## What's actually in here

- **Real leader election** - randomized timeouts, proper vote counting, the up-to-date log check from §5.4.1 so a candidate missing a committed entry can't win.
- **Real log replication** - the AppendEntries consistency check and merge, fast conflict backtracking instead of decrementing nextIndex one entry at a time, and the no-op-on-election commit that Raft needs to move commitIndex forward under a fresh leader (§5.4.2 / Figure 8 - this one's easy to miss and I did, the first time).
- **Real snapshotting** - compaction on the leader, InstallSnapshot for a follower that's fallen behind further than the log can fix.
- **Actual durability** - a file-based persister (JSON on disk, atomic writes) so a killed and restarted node comes back with its term/vote/log intact instead of amnesia.
- **A test suite I trust** - an in-memory fake network that can partition or crash nodes in a couple lines, no Docker needed. `go test ./tests/...` is genuinely how I catch bugs here, not just for show.

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

inside each node:

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

Every client request - `Get`, `Put`, `Delete`, all of them - gets encoded and proposed to Raft, even reads. That's what makes reads linearizable: a `Get` can't come back stale just because it hit a node that hasn't caught up, since it has to go through the same log ordering as every write. See `server/kv_store.go`.

## Running it

```bash
docker compose up --build
```

Brings up 5 nodes on ports 9101-9105. Watch the election happen:

```bash
docker compose logs -f | grep role=
```

Once a leader shows up:

```bash
go run ./cmd/kvctl put foo bar
go run ./cmd/kvctl get foo
```

Kill the leader and watch someone else take over, then bring it back and watch it rejoin with its state intact:

```bash
./scripts/kill_leader.sh
docker compose logs -f
docker compose start <the node the script killed>
```

![put/get, kill the leader, watch re-election, read the value back](docs/demo.gif)

(that's a real recording, not staged - `docs/demo.tape` is the exact script if you want to reproduce it with [vhs](https://github.com/charmbracelet/vhs))

Run the actual test suite (this is the thing I run after touching anything in `raft/`):

```bash
go test ./tests/... -race -count=5
```

## Benchmarks

I used to have a throughput number here with nothing behind it. Not anymore - `bench/main.go` spins up a real 5-node cluster in-process (real gRPC, real `FilePersister` writing JSON to disk on every entry, no batching, one RPC per op) and hammers the leader directly with persistent connections:

```bash
go run ./bench
```

What I actually get, 5 runs on my M1 MacBook Air (8GB RAM), 50 concurrent clients, 5 seconds each:

```
PUT: ~950-1,400 ops/sec
GET: ~950-1,400 ops/sec  (goes through the same consensus path as writes - see above)
```

So, roughly **1,000-1,400 ops/sec**, not 100K. I ran the same benchmark against `MemoryPersister` (no disk I/O at all) to see where the time actually goes, and it roughly doubled to ~2,300-3,600 ops/sec - so the JSON-on-every-entry persister is a real, measurable chunk of the cost, not a rounding error. The other big one is architectural: one RPC per write with no batching, which is exactly the thing I called out under "at scale" below. A production system doing both of those - a real WAL instead of rewriting JSON, and batching proposals - would post a meaningfully bigger number. This one doesn't, and I'd rather say that than make something up.

Run it yourself before quoting any of this - it'll depend on your disk and CPU, and a laptop isn't the hardware anyone would actually run this on.

## How I built this

I went `raft/` first, with an in-memory fake transport in `tests/` before touching gRPC or Docker at all - way faster to iterate on election/replication logic when a whole 5-node test run takes half a second instead of needing containers up. Only once `go test ./tests/...` was passing did I wire up the real gRPC transport and the Compose cluster.

Bugs worth remembering, because I hit both of these myself while writing the tests:

- **The no-op-on-election thing.** A freshly elected leader can't advance commitIndex just because a majority already has some old entry - Raft only lets you count entries from your *own* current term directly (Figure 8). Without committing something in your own term right away, a quiet leader with no new writes just sits there unable to move commitIndex at all. Cost me a confusing hour before I found the right paragraph in the paper.
- **"Believes it's leader" isn't the same as "can make progress."** A leader that gets partitioned into the minority doesn't magically know it's been cut off - it keeps thinking it's leader until it either reconnects or tries to commit something and can't reach a majority. My first pass at the partition/crash tests asserted "no minority node should ever have role==Leader," which is just wrong - I had to rewrite those to check the actual guarantee, which is "the minority can never get a *new* entry committed." Left the corrected reasoning in the test file comments since it's the kind of thing that looks like a bug in my Raft code the first time you see it fail, but is actually a bug in the test's assumption.

## Design decisions / what I'd do differently at scale

- **Reads go through the log too.** Simpler to reason about and to test than a separate read-index/lease-read fast path, at the cost of every `Get` paying full consensus latency. If I cared about read latency I'd implement the ReadIndex optimization from the paper instead.
- **One gRPC listener, two services per node.** `KV` (client-facing) and `Raft` (internal) share a port - one less thing to configure per container. A real deployment would put the `Raft` service behind mTLS and a separate internal network instead of trusting anything that can reach the port.
- **`raft.Transport` is an interface, not a concrete gRPC type.** This is the whole reason `tests/harness.go` can simulate a partitioned network in-process in milliseconds instead of every test needing a live cluster.
- **JSON for both the persister and the log commands.** Slower and bigger on the wire than protobuf or gob, but I can `cat state.json` mid-debugging and actually read it, which mattered more to me while building this than shaving bytes.
- **At scale:** batch multiple proposals into a single AppendEntries round instead of one RPC per write, add ReadIndex for reads, and replace the static `PEERS` config with real membership changes (joint consensus, §6) so the cluster can be resized without a full restart. Didn't tackle that last one here - felt like its own project on top of getting basic replication right.

## Layout

```
raft/           the actual consensus algorithm - no gRPC import, unit-testable on its own
server/         gRPC services (KV + internal Raft) and the KV state machine
client/         Go client with leader-redirect retry logic
cmd/node/       node binary
cmd/kvctl/      CLI client
config/         cluster membership from env vars
proto/          gRPC schema + generated code
tests/          in-memory cluster harness + election/log/partition/failure/snapshot tests
bench/          throughput benchmark against a real cluster - see Benchmarks below
docs/           architecture notes, sequence diagrams
scripts/        kill_leader.sh for the re-election demo
```
