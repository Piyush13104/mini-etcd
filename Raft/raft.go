package raft

import (
	"context"
	"log"
	"sync"
	"time"

	wal "github.com/Piyush22070/mini-etcd/Wal"
)

const heartbeatInterval = 100 * time.Millisecond

// All the hosts
type Peer struct {
	ID   string
	Addr string // host:port, e.g. "node2:8080"
}

// ---- the wire protocol ----

// AppendEntriesRequest is the ONE message that does replication, catch-up,
// heartbeating, and commit notification. Raft reuses a single RPC for all
// four jobs, which is a big part of why it is simpler than Paxos.
type AppendEntriesRequest struct {
	Term     int64  `json:"term"` // leader's term
	LeaderID string `json:"leader_id"`

	// PrevLogIndex/PrevLogTerm are THE consistency check.
	// They describe the entry immediately before Entries[0].
	// If the follower doesn't have exactly that entry, it rejects --
	// which proves, by induction, that the whole prefix matches when it
	// accepts.
	PrevLogIndex int64 `json:"prev_log_index"`
	PrevLogTerm  int64 `json:"prev_log_term"`

	Entries []wal.Entry `json:"entries"` // empty = pure heartbeat

	// LeaderCommit tells the follower how far it is safe to APPLY.
	// Having an entry on disk is not permission to act on it.
	LeaderCommit int64 `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term    int64 `json:"term"`
	Success bool  `json:"success"`

	// LastIndex is an optimisation: on rejection, it tells the leader where
	// this follower actually is, so it can jump straight there instead of
	// stepping back one index at a time.
	LastIndex int64 `json:"last_index"`
}

// ApplyFunc hands a committed command to the state machine.
// It returns whatever the state machine produced, so the client that
// proposed it can be told the result.
type ApplyFunc func(index int64, data []byte) any

type waiter struct {
	done   chan struct{}
	result any
}

type Node struct {
	ID       string
	IsLeader bool
	Peers    []Peer

	log   *Log
	apply ApplyFunc

	mu          sync.Mutex
	currentTerm int64

	// commitIndex: replicated to a MAJORITY of disks. Safe to apply.
	// lastApplied: actually handed to the state machine.
	//
	// WHY two counters: entries exist in the log before they are safe.
	// Applying an uncommitted entry means acting on data that might still
	// be truncated away. These two must never be collapsed into one.
	commitIndex int64
	lastApplied int64

	// Leader-only, per follower:
	//   nextIndex  = what to send next  (optimistic guess, starts at last+1)
	//   matchIndex = confirmed on their disk (pessimistic, starts at 0)
	nextIndex  map[string]int64
	matchIndex map[string]int64

	waiters map[int64]*waiter // clients blocked until their entry commits

	applyCh  chan struct{}            // nudges the apply loop
	trigger  map[string]chan struct{} // nudges a peer's replication loop
	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewNode(id string, isLeader bool, peers []Peer, lg *Log, apply ApplyFunc) *Node {
	n := &Node{
		ID: id, IsLeader: isLeader, Peers: peers,
		log: lg, apply: apply,
		currentTerm: 1, // no elections yet, so the term never changes
		nextIndex:   make(map[string]int64),
		matchIndex:  make(map[string]int64),
		waiters:     make(map[int64]*waiter),
		applyCh:     make(chan struct{}, 1),
		trigger:     make(map[string]chan struct{}),
		stopCh:      make(chan struct{}),
	}
	last := lg.LastIndex()
	for _, p := range peers {
		// Optimistic: assume the follower is fully caught up. If it isn't,
		// the first AppendEntries fails and we walk backwards to the truth.
		n.nextIndex[p.ID] = last + 1
		n.matchIndex[p.ID] = 0
		n.trigger[p.ID] = make(chan struct{}, 1)
	}
	return n
}

func (n *Node) Start() {
	go n.applyLoop()
	if n.IsLeader {
		for _, p := range n.Peers {
			go n.replicateLoop(p) // one goroutine per follower
		}
	}
}

func (n *Node) Stop() { n.stopOnce.Do(func() { close(n.stopCh) }) }

// ---- the write path ----

// Propose appends a command and blocks until a MAJORITY has it on disk.
//
// WHY block: returning 200 the moment the leader writes locally would be a
// lie -- if the leader died a millisecond later, the write would vanish.
// "200 OK means a majority has it" is the guarantee worth having, and it is
// what makes the quorum test meaningful.
func (n *Node) Propose(ctx context.Context, data []byte) (any, error) {
	if !n.IsLeader {
		return nil, ErrNotLeader
	}

	n.mu.Lock()
	index := n.log.LastIndex() + 1
	entry := wal.Entry{Index: index, Term: n.currentTerm, Type: 1, Data: data}
	if err := n.log.Append(entry); err != nil {
		n.mu.Unlock()
		return nil, err
	}
	w := &waiter{done: make(chan struct{})}
	n.waiters[index] = w
	n.mu.Unlock()

	// Don't wait for the next 100ms heartbeat -- push immediately.
	n.triggerAll()

	// Single-node case: majority of 1 is 1, so this commits right away.
	n.mu.Lock()
	n.advanceCommit()
	n.mu.Unlock()

	select {
	case <-w.done:
		return w.result, nil
	case <-ctx.Done():
		// Timed out = no majority. The entry stays in our log but is NOT
		// committed and NOT applied. A future leader may keep or discard
		// it. We must not tell the client it succeeded.
		n.mu.Lock()
		delete(n.waiters, index)
		n.mu.Unlock()
		return nil, ctx.Err()
	}
}

// ---- follower side ----

// HandleAppendEntries is the follower's half of replication.
func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := AppendEntriesResponse{Term: n.currentTerm, LastIndex: n.log.LastIndex()}

	// STEP 1: ignore a leader from an older term (a deposed leader that
	// hasn't noticed yet). Matters only once elections exist, but wiring it
	// now means the message format never has to change.
	if req.Term < n.currentTerm {
		return resp // Success stays false
	}
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
	}

	// STEP 2: THE CONSISTENCY CHECK.
	// "Do I have exactly the entry the leader thinks precedes this batch?"
	// PrevLogIndex 0 means "this is the very start of the log", always OK.
	if req.PrevLogIndex > 0 {
		term, ok := n.log.TermAt(req.PrevLogIndex)
		if !ok || term != req.PrevLogTerm {
			// Reject. The leader will step back and try an earlier index.
			// This loop is the ENTIRE catch-up mechanism -- there is no
			// separate "resync" code path anywhere in Raft.
			resp.LastIndex = n.log.LastIndex()
			return resp
		}
	}

	// STEP 3: append the new entries.
	for i, e := range req.Entries {
		idx := req.PrevLogIndex + int64(i) + 1
		e.Index = idx // trust our own arithmetic, not the sender's field

		if existingTerm, ok := n.log.TermAt(idx); ok {
			if existingTerm == e.Term {
				// We already have this exact entry. This happens whenever a
				// response got lost and the leader retried. Appending again
				// would DUPLICATE it, so skip.
				continue
			}
			// Conflict: same slot, different term. Delete it and everything
			// after it, then fall through and append the leader's version.
			if err := n.log.TruncateFrom(idx); err != nil {
				log.Printf("truncate failed: %v", err)
				return resp
			}
		}
		if err := n.log.Append(e); err != nil {
			log.Printf("append failed: %v", err)
			return resp
		}
	}

	// STEP 4: learn what is now safe to apply.
	// min() because the leader may have committed entries we haven't
	// received yet -- we can only apply what we actually hold.
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, n.log.LastIndex())
		n.signalApply()
	}

	resp.Success = true
	resp.LastIndex = n.log.LastIndex()
	return resp
}

// ---- leader side ----

func (n *Node) replicateLoop(p Peer) {
	t := time.NewTicker(heartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C: // periodic heartbeat, even with nothing new to send
		case <-n.trigger[p.ID]: // a new proposal, send immediately
		}
		n.sendTo(p)
	}
}

func (n *Node) sendTo(p Peer) {
	n.mu.Lock()
	next := n.nextIndex[p.ID]
	prevIndex := next - 1
	prevTerm := int64(0)
	if prevIndex > 0 {
		if t, ok := n.log.TermAt(prevIndex); ok {
			prevTerm = t
		}
	}
	req := AppendEntriesRequest{
		Term: n.currentTerm, LeaderID: n.ID,
		PrevLogIndex: prevIndex, PrevLogTerm: prevTerm,
		Entries:      n.log.From(next), // nil when caught up = heartbeat
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock() // never hold the lock across a network call

	resp, err := sendAppendEntries(p.Addr, req)
	if err != nil {
		return // peer down; the next tick retries. No special handling.
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Success {
		sent := req.PrevLogIndex + int64(len(req.Entries))
		if sent > n.matchIndex[p.ID] {
			n.matchIndex[p.ID] = sent
			n.nextIndex[p.ID] = sent + 1
		}
		n.advanceCommit()
		return
	}

	// Rejected: this follower's log doesn't match where we thought.
	// Back up and retry on the next tick. Using their reported LastIndex
	// jumps straight there instead of decrementing one at a time.
	back := resp.LastIndex + 1
	if back < 1 {
		back = 1
	}
	if back >= n.nextIndex[p.ID] {
		back = n.nextIndex[p.ID] - 1 // always make progress backwards
	}
	if back < 1 {
		back = 1
	}
	n.nextIndex[p.ID] = back
}

// advanceCommit finds the highest index stored on a MAJORITY of disks.
// Caller must hold n.mu.
//
// WHY a majority and not "all": requiring all means one slow node stops the
// cluster. WHY not "any two specific nodes": any two majorities of the same
// cluster must overlap in at least one node, so a committed entry can never
// be lost -- whoever leads next is guaranteed to have seen it.
func (n *Node) advanceCommit() {
	total := len(n.Peers) + 1 // peers plus ourselves
	for idx := n.log.LastIndex(); idx > n.commitIndex; idx-- {
		count := 1 // the leader has it by definition
		for _, p := range n.Peers {
			if n.matchIndex[p.ID] >= idx {
				count++
			}
		}
		if count > total/2 { // 3 nodes -> need 2; 5 nodes -> need 3
			n.commitIndex = idx
			n.signalApply()
			return
		}
	}
}

// ---- applying ----

// applyLoop is the ONLY place the state machine is touched.
//
// WHY one loop on every node, leader included: the leader must NOT apply in
// Propose(). If it did, it would apply once there and once here -- the
// revision would advance by 2 on the leader and 1 on followers, and the
// cluster would diverge silently. One writer, driven by commitIndex. This
// is the single most common bug when wiring consensus to a store.
func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyCh:
		}

		for {
			n.mu.Lock()
			if n.lastApplied >= n.commitIndex {
				n.mu.Unlock()
				break
			}
			n.lastApplied++
			idx := n.lastApplied
			n.mu.Unlock()

			e, ok := n.log.At(idx)
			if !ok {
				continue
			}
			result := n.apply(idx, e.Data) // strictly in index order

			n.mu.Lock()
			if w, waiting := n.waiters[idx]; waiting {
				w.result = result
				close(w.done) // unblocks the client's Propose
				delete(n.waiters, idx)
			}
			n.mu.Unlock()
		}
	}
}

// signalApply is a non-blocking nudge. Buffered size 1: if a wakeup is
// already pending, we don't need a second one -- the loop drains everything
// up to commitIndex either way.
func (n *Node) signalApply() {
	select {
	case n.applyCh <- struct{}{}:
	default:
	}
}

func (n *Node) triggerAll() {
	for _, p := range n.Peers {
		select {
		case n.trigger[p.ID] <- struct{}{}:
		default:
		}
	}
}

// Status is for /status and for eyeballing whether nodes agree.
func (n *Node) Status() map[string]any {
	n.mu.Lock()
	defer n.mu.Unlock()
	return map[string]any{
		"id":           n.ID,
		"is_leader":    n.IsLeader,
		"term":         n.currentTerm,
		"last_index":   n.log.LastIndex(),
		"commit_index": n.commitIndex,
		"last_applied": n.lastApplied,
		"match_index":  n.matchIndex,
	}
}

func (n *Node) ReplayAll() {
	last := n.log.LastIndex()
	for i := int64(1); i <= last; i++ {
		if e, ok := n.log.At(i); ok {
			n.apply(i, e.Data)
		}
	}
	n.commitIndex = last
	n.lastApplied = last
	log.Printf("replayed %d entries from disk", last)
}
