# mini-etcd

A distributed, strongly-consistent key-value store written from scratch in Go — the core engine behind etcd, rebuilt to learn how it works.

Three nodes, one replicated log, MVCC versioning with time-travel reads, and crash recovery from a write-ahead log. No consensus library: the log replication is hand-written.

---

## What it does

| Feature | Notes |
|---|---|
| **Replicated log** | Hand-written `AppendEntries` RPC. Writes commit only after a **majority** of nodes have them on disk. |
| **MVCC + revisions** | Every write bumps a global revision. Old versions are kept — `?rev=N` reads the past. |
| **Write-ahead log** | Length-prefixed binary records with CRC32. Fsynced before acknowledging. Tolerates a torn tail after a crash. |
| **Crash recovery** | State is rebuilt by replaying the log at startup. |
| **Tombstone deletes** | Deleting keeps history, so time-travel reads still work. |
| **Quorum enforcement** | Lose 2 of 3 nodes and writes correctly **fail** rather than silently diverging. |

**Not implemented:** leader elections. Node 1 is the leader by configuration — if it dies, reads keep working but writes stop until it returns. See [Roadmap](#roadmap).

---

## Architecture

```
          ┌──────────── Client (curl) ────────────┐
          │                                       │
      POST /cmd                              GET /kv/{key}
          │                                       │
   ┌──────▼───────┐  AppendEntries RPC    ┌───────▼──────┐
   │   node1      │◄─────────────────────►│   node2/3    │
   │  (leader)    │   POST /raft/...      │ (followers)  │
   └──────┬───────┘                       └───────┬──────┘
          │                                       │
   ┌──────▼───────┐                       ┌───────▼──────┐
   │ Raft log     │  wal.log on disk      │ Raft log     │
   │ (truth)      │  append-only + CRC    │ (truth)      │
   └──────┬───────┘                       └───────┬──────┘
          │ apply committed entries only           │
   ┌──────▼───────┐                       ┌───────▼──────┐
   │ MVCC store   │  in RAM, derived      │ MVCC store   │
   └──────────────┘                       └──────────────┘
```

**The log is the database; the in-memory map is a derived view** rebuilt from it at boot. A write is durable on a majority of disks *before* the client sees `200 OK`.

| Package | Role |
|---|---|
| `Wal/` | Binary record format — encode, decode, CRC32, fsync |
| `Raft/log.go` | Indexed log over the WAL: random access + truncation |
| `Raft/raft.go` | Replication engine — AppendEntries, commit rule, apply loop |
| `Raft/http.go` | RPC transport (JSON over HTTP, so it's `curl`-debuggable) |
| `Store/` | MVCC state machine — the **only** thing that holds data |
| `main.go` | HTTP API and wiring |

---

## Run it

```bash
podman compose up --build -d     # or: docker compose
```

| Node | Role | URL |
|---|---|---|
| node1 | leader | `localhost:8080` |
| node2 | follower | `localhost:8081` |
| node3 | follower | `localhost:8082` |

---

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/cmd` | Write. Body: `{"op":"PUT","key":"/name","value":"piyush"}` or `{"op":"DELETE","key":"/name"}`. **Leader only.** |
| `GET` | `/kv/{key}` | Current value |
| `GET` | `/kv/{key}?rev=N` | Value as of revision N (time travel) |
| `GET` | `/kv?prefix=/cfg/` | Range scan |
| `GET` | `/status` | Node id, term, log index, commit index, revision |
| `POST` | `/raft/append-entries` | Peer RPC (internal) |

---

## Demo

**Replication** — write to the leader, read from a follower that was never written to:

```bash
curl -X POST localhost:8080/cmd -d '{"op":"PUT","key":"/name","value":"piyush"}'
curl localhost:8081/kv/name      # piyush
curl localhost:8082/kv/name      # piyush
```

**MVCC** — history survives updates and deletes:

```bash
curl -X POST localhost:8080/cmd -d '{"op":"PUT","key":"/name","value":"bhoyar"}'
curl localhost:8080/kv/name           # bhoyar, version 2, create_revision unchanged
curl "localhost:8080/kv/name?rev=1"   # piyush
```

**Fault tolerance** — survives one node down:

```bash
podman stop node3
curl -X POST localhost:8080/cmd -d '{"op":"PUT","key":"/during-outage","value":"yes"}'
# succeeds: 2 of 3 is a majority

podman start node3 && sleep 2
curl localhost:8082/kv/during-outage  # yes — caught up automatically
```

node3 catches up through the normal AppendEntries path: the leader's consistency check fails, it walks back to the last index both logs agree on, then streams forward. There is no separate resync code.

**Quorum loss** — the test that proves it's consistent, not just replicated:

```bash
podman stop node2 node3
curl -i -X POST localhost:8080/cmd -d '{"op":"PUT","key":"/x","value":"1"}'
# 503 no quorum — refuses the write rather than accepting data it can't guarantee
curl localhost:8080/kv/name   # reads still work
```

**Check they agree:**

```bash
for p in 8080 8081 8082; do curl -s localhost:$p/status; echo; done
# commit_index and revision should match across all three
```

---

## How a write travels

```
1. Client → leader: POST /cmd
2. Leader appends to its own log, fsync.      ← on disk, NOT yet visible
3. Leader → followers: AppendEntries RPC
4. Followers append + fsync, reply success
5. Majority (2/3) confirmed → entry is COMMITTED
6. Apply loop runs it through the MVCC store → 200 OK
7. Next heartbeat tells followers to apply it too
```

Followers lag the leader by up to one heartbeat (100 ms), so reads from a follower are **eventually consistent**. That's correct behaviour, not a bug — etcd solves it with linearizable reads, which this doesn't implement yet.

---

## Design notes

**Revision is a logical clock for the whole cluster**, not a per-key counter. Like a git commit number: `create_revision` is when a key was born, `mod_revision` is when it last changed, `version` counts that key's own updates. One global ordering is what makes consistent snapshots and gapless watches possible.

**`store.Apply` must be deterministic.** No `time.Now()`, no `rand`, no map-iteration order. All three nodes run it over the same log and must land on identical state. `Store/store_test.go:TestDeterminism` guards this — if it fails, the cluster will diverge silently.

**Only the apply loop writes to the store.** Not the HTTP handler, not `Propose`. Applying in both places makes the leader's revision advance twice per write while followers advance once.

---

## Roadmap

- [ ] **Leader elections** — `RequestVote`, randomized timeouts, persisted `currentTerm`/`votedFor`, the election restriction. Turns "leader dies, writes stop" into a ~1s blip.
- [ ] Write forwarding from followers (etcd does this; here you get a 307)
- [ ] Snapshots + log compaction
- [ ] Watch API (streaming changes from a revision — MVCC already makes this possible)
- [ ] Leases / TTL keys
- [ ] gRPC instead of JSON

---

## Test

```bash
go test ./Store/ -race -v
go build ./...
```

---

## Differences from real etcd

| | mini-etcd | etcd |
|---|---|---|
| Consensus | log replication, fixed leader | full Raft with elections |
| Storage | everything in RAM | RAM index + bbolt B+tree on disk |
| Transport | JSON over HTTP | gRPC / protobuf |
| Sharding | none (full replica per node) | none — etcd replicates too |
| Reads | eventually consistent | linearizable available |
