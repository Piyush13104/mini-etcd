0. Startup (context for everything else)

```
main()                                               main.go:160
  ├─ flag.Parse()                       --id --http-addr --data-dir --peers --leader-id
  ├─ parsePeers("1@node1:8080,...")                  main.go:139
  │     → []raft.Peer (excluding self) + map[id]addr
  │
  ├─ raft.OpenLog("/data/wal.log")                   log.go:36
  │     ├─ os.OpenFile(O_RDWR|O_CREATE|O_APPEND)
  │     └─ loop until EOF:
  │          ├─ f.Seek(0, Current)        ← remember byte offset
  │          ├─ wal.ReadNextEntry(f)                 Wal/main.go:129
  │          │     ├─ read 4B length, 4B CRC
  │          │     ├─ verify CRC
  │          │     └─ decodeEntry()                  Wal/main.go:100
  │          └─ append to l.entries[] and l.offsets[]
  │       (torn tail → f.Truncate(pos), warn, stop)
  │
  ├─ store.New()                                     Store/main.go:103
  ├─ applyFn := func(index, data)                    main.go:196
  │       └─ THE state-machine boundary. Nothing else may call st.Apply.
  │
  ├─ raft.NewNode(id, isLeader, peers, rlog, applyFn)  raft.go:98
  │       └─ nextIndex[p]=last+1, matchIndex[p]=0, creates all channels
  │
  ├─ rn.ReplayAll()                                  raft.go:420
  │       └─ for i := 1..LastIndex: applyFn(i, entry.Data)
  │             → rebuilds the RAM map from disk
  │
  ├─ rn.Start()                                      raft.go:122
  │       ├─ go applyLoop()                 (every node)
  │       └─ go replicateLoop(p) per peer   (leader only)
  │
  └─ ListenAndServe with 5 routes
  ```

Two background goroutines are now running forever. Every request below interacts with them.

---

1. POST /cmd — write (on the leader)

This one crosses three goroutines. Follow the *** *** markers.

HTTP goroutine
──────────────
Server.handleCommand                                 main.go:28
  ├─ if !s.raft.IsLeader → 307                        main.go:34
  ├─ io.ReadAll(body)
  ├─ store.DecodeCommand(body)                        Store/main.go:31
  │     └─ validates op + key  ← reject BEFORE it touches the log
  ├─ json.Marshal(cmd)          ← canonical bytes, all nodes apply the same
  ├─ context.WithTimeout(5s)
  │
  └─ raft.Propose(ctx, data)                          raft.go:141
        ├─ n.mu.Lock()
        ├─ index = log.LastIndex()+1                  log.go:82
        ├─ log.Append(entry)                          log.go:127
        │     └─ wal.WriteWAL(file, e)                Wal/main.go:61
        │          ├─ encodeEntry()                   Wal/main.go:25
        │          ├─ frame = [len][crc][payload]
        │          └─ file.Write + file.Sync()   ← FSYNC, now durable
        ├─ waiters[index] = &waiter{done: chan}
        ├─ n.mu.Unlock()
        ├─ triggerAll()  ──► trigger["2"]←{}  trigger["3"]←{}   raft.go:396
        ├─ advanceCommit()   (commits instantly if single-node)  raft.go:325
        └─ select { <-w.done | <-ctx.Done() }   *** BLOCKS HERE ***

replicateLoop("2") goroutine — woken by trigger
───────────────────────────────────────────────
replicateLoop(p)                                      raft.go:252
  └─ sendTo(p)                                        raft.go:266
       ├─ lock: read nextIndex[p], build request
       │    PrevLogIndex = next-1, PrevLogTerm = TermAt(prev)   log.go:93
       │    Entries = log.From(next)                            log.go:113
       │    LeaderCommit = commitIndex
       ├─ unlock          ← never hold the lock over the network
       ├─ sendAppendEntries(addr, req)                http.go:23
       │     └─ json.Marshal → HTTP POST → json.Decode
       └─ on success:
            ├─ matchIndex[p] = prevIndex + len(entries)
            ├─ nextIndex[p]  = matchIndex[p] + 1
            └─ advanceCommit()                        raft.go:325
                 ├─ count nodes with matchIndex >= idx
                 ├─ count > total/2  → commitIndex = idx
                 └─ signalApply()  ──► applyCh←{}     raft.go:389

applyLoop goroutine — woken by applyCh
──────────────────────────────────────
applyLoop()                                           raft.go:351
  └─ while lastApplied < commitIndex:
       ├─ lastApplied++
       ├─ log.At(idx)                                 log.go:102
       ├─ n.apply(idx, data)  → applyFn               main.go:196
       │     ├─ store.DecodeCommand(data)             Store/main.go:31
       │     └─ store.Apply(cmd)                      Store/main.go:115
       │          ├─ rev++
       │          ├─ append VersionedValue to kv[key]
       │          └─ return Result
       └─ if waiters[idx]:
            ├─ w.result = Result      ← set the answer FIRST
            └─ close(w.done)          ──► unblocks Propose

back in HTTP goroutine
──────────────────────
  Propose returns Result
  → res.Applied? → writeJSON 200                      main.go:76
  → else 404 (deleted a missing key)

On timeout instead: ctx.Done() fires → 503 no quorum (main.go:67). The entry stays in the leader's log but is never committed and never applied.

---

2. POST /cmd — on a follower

Server.handleCommand                                  main.go:28
  └─ !s.raft.IsLeader → http.Error(307, "not leader, write to node1:8080")

Two lines. Propose is never reached. (Propose also guards itself at raft.go:142, in case anything calls it directly.)

---

3. POST /raft/append-entries — the peer RPC

The other half of flow #1, running on the follower.

AppendEntriesHandler                                  http.go:51
  ├─ json.Decode → AppendEntriesRequest
  └─ HandleAppendEntries(req)                         raft.go:182
       ├─ n.mu.Lock()
       │
       │  STEP 1  stale leader?
       ├─ req.Term < currentTerm → reject             raft.go:191
       │
       │  STEP 2  THE CONSISTENCY CHECK
       ├─ log.TermAt(req.PrevLogIndex)                log.go:93
       │     ├─ missing or wrong term → reject with LastIndex
       │     │     (leader steps back and retries = catch-up)
       │     └─ matches → prefix is proven identical, continue
       │
       │  STEP 3  append entries
       ├─ for each entry:                             raft.go:213
       │     ├─ already have same (index, term)? → skip  (retransmit)
       │     ├─ conflict? → log.TruncateFrom(idx)     log.go:158
       │     │                 └─ file.Truncate(offsets[idx-1]) + Sync
       │     └─ log.Append(e)                         log.go:127
       │            └─ wal.WriteWAL → fsync
       │
       │  STEP 4  learn what is safe to apply
       ├─ commitIndex = min(LeaderCommit, LastIndex)  raft.go:241
       ├─ signalApply()  ──► applyCh←{}
       ├─ n.mu.Unlock()
       └─ return {success: true, last_index: N}
            │
            └─ json.Encode → back over HTTP to the leader

(meanwhile this node's applyLoop wakes and applies, same as flow #1)

Empty Entries = heartbeat. Same path, steps 3 does nothing, step 4 still delivers the new commit index.

---

4. GET /kv/{key} — read

Server.handleGet                                      main.go:88
  ├─ key = "/" + r.PathValue("key")     /kv/cfg/db → "/cfg/db"
  ├─ no ?rev → store.Get(key)                         Store/main.go:179
  │     ├─ s.mu.RLock()
  │     ├─ live(s.kv[key])                            Store/main.go:260
  │     │     ├─ history empty → not found
  │     │     ├─ newest is a tombstone → not found
  │     │     └─ else return it
  │     └─ RUnlock
  ├─ err → 404
  └─ writeJSON 200 {key, value, create_revision, mod_revision, version}

No log, no disk, no Raft. Reads hit the RAM map directly — which is why a follower can be up to one heartbeat stale.

---

5. GET /kv/{key}?rev=N — time travel

Same handler, different branch:

Server.handleGet                                      main.go:88
  ├─ strconv.ParseInt(rev)   → bad → 400
  └─ store.GetAtRevision(key, rev)                    Store/main.go:191
        ├─ RLock
        └─ walk history BACKWARDS:
             ├─ first version with ModRevision <= rev wins
             ├─ if it's a tombstone → 404 (was deleted by then)
             └─ none found → 404 (didn't exist yet)

---

6. GET /kv?prefix=/cfg/ — range

Server.handleRange                                    main.go:112
  └─ store.Range(prefix)                              Store/main.go:211
        ├─ RLock
        ├─ iterate the whole map
        │    ├─ strings.HasPrefix(k, prefix)?
        │    └─ live(history)?  ← tombstones excluded
        ├─ sort.Slice by key   ← Go map order is random; sort makes
        │                        responses comparable across nodes
        └─ return []KeyValue
  → writeJSON {revision, count, kvs}

Note: GET /kv and GET /kv/{key...} are separate routes — Go's mux picks by whether a path segment follows.

---

7. GET /status

Server.handleStatus                                   main.go:121
  ├─ raft.Status()                                    raft.go:406
  │     └─ lock → {id, is_leader, term, last_index,
  │                commit_index, last_applied, match_index}
  ├─ st["revision"]    = store.Revision()             Store/main.go:108
  ├─ st["leader_addr"] = s.leaderAddr
  └─ writeJSON

The debugging endpoint. last_index = entries in the log. commit_index = agreed by a majority. last_applied = fed to the store. revision = the store's own counter.

Healthy cluster: all three nodes show the same commit_index and revision.
- last_index differs → a follower is behind on replication
- last_index matches but revision differs → Apply isn't deterministic (run TestDeterminism)

---

Background loops (no HTTP involved)

replicateLoop(p)   LEADER ONLY, one per follower      raft.go:252
  select {
    <-stopCh        → exit
    <-ticker.C      → every 100ms, heartbeat
    <-trigger[p.ID] → a new write, send now
  }
  → sendTo(p)

applyLoop()        EVERY node, exactly one            raft.go:351
  select {
    <-stopCh  → exit
    <-applyCh → drain lastApplied up to commitIndex
  }

---

The three layers, summarized

┌───────────────────────────────────────┬──────────────────────────────┬────────────────────────────────────────┐
│                 Layer                 │          Touched by          │                Durable?                │
├───────────────────────────────────────┼──────────────────────────────┼────────────────────────────────────────┤
│ Log (Raft/log.go + Wal/)              │ Propose, HandleAppendEntries │ Yes — fsynced file. This is the truth. │
├───────────────────────────────────────┼──────────────────────────────┼────────────────────────────────────────┤
│ Raft state (commitIndex, matchIndex…) │ replicate + apply loops      │ No — rebuilt at startup                │
├───────────────────────────────────────┼──────────────────────────────┼────────────────────────────────────────┤
│ Store (Store/)                        │ only applyFn via applyLoop   │ No — RAM, replayed from the log        │
└───────────────────────────────────────┴──────────────────────────────┴────────────────────────────────────────┘

Two invariants hold the whole thing together:

1. Writes only enter through Propose. HTTP handlers never call store.Apply.
2. applyLoop is the only caller of store.Apply. If Propose also applied directly, the leader's revision would advance by 2 per write and followers by 1 — silent divergence.