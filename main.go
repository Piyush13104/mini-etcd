package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	raft "github.com/Piyush22070/mini-etcd/Raft"
	store "github.com/Piyush22070/mini-etcd/Store"
)

type Server struct {
	id         string
	store      *store.Store
	raft       *raft.Node
	leaderAddr string
}

func (s *Server) handleCommand(w http.ResponseWriter, r *http.Request) {
	// Only the leader may accept writes.
	//
	// WHY: if two nodes both appended to their own logs, there would be two
	// different "entry 47" and no way to reconcile them. A single writer is
	// what makes a single ordering possible.
	if !s.raft.IsLeader {
		http.Error(w, "not leader, write to "+s.leaderAddr, http.StatusTemporaryRedirect)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil || len(body) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	// Validate BEFORE it reaches the log. An invalid command in the log
	// would be replayed by every node, forever, on every restart.
	cmd, err := store.DecodeCommand(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Re-marshal to canonical form so all three nodes apply identical bytes.
	data, err := json.Marshal(cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 5s: long enough for a slow peer, short enough that a client learns
	// quorum is lost instead of hanging.
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	result, err := s.raft.Propose(ctx, data)
	if err != nil {
		// Timeout here means NO MAJORITY. Reporting success would be a lie.
		http.Error(w, "no quorum: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	res, _ := result.(store.Result)
	if !res.Applied {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// ---- read path: local memory, no consensus ----
//
// WHY reads skip Raft: the entries are already applied locally. The cost is
// that a follower can be up to one heartbeat behind the leader, so a read
// immediately after a write to a DIFFERENT node may return the old value.
// That is EVENTUAL CONSISTENCY, and it is correct behaviour, not a bug.
// Real etcd offers linearizable reads by asking the leader for its commit
// index first; that's a later upgrade.

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := "/" + r.PathValue("key")

	var (
		v   store.VersionedValue
		err error
	)
	if q := r.URL.Query().Get("rev"); q != "" {
		rev, perr := strconv.ParseInt(q, 10, 64)
		if perr != nil {
			http.Error(w, "bad rev", http.StatusBadRequest)
			return
		}
		v, err = s.store.GetAtRevision(key, rev)
	} else {
		v, err = s.store.Get(key)
	}
	if err != nil {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, store.KeyValue{Key: key, VersionedValue: v})
}

func (s *Server) handleRange(w http.ResponseWriter, r *http.Request) {
	kvs := s.store.Range(r.URL.Query().Get("prefix"))
	writeJSON(w, http.StatusOK, map[string]any{
		"revision": s.store.Revision(), "count": len(kvs), "kvs": kvs,
	})
}

// handleStatus is your main debugging tool. Compare it across all three
// nodes: equal commit_index and revision means they are in sync.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st := s.raft.Status()
	st["revision"] = s.store.Revision() // state machine's view
	st["leader_addr"] = s.leaderAddr
	writeJSON(w, http.StatusOK, st)
}

// Response That we send to client
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// parsePeers turns "1@node1:8080,2@node2:8080,3@node3:8080" into the peer
// list, excluding ourselves
// WHY every node gets the SAME string: one value in the compose file, no
// chance of node2 and node3 disagreeing about who is in the cluster.
func parsePeers(spec, selfID string) ([]raft.Peer, map[string]string) {
	var peers []raft.Peer
	all := make(map[string]string)
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// extract the id addr seperated by
		id, addr, ok := strings.Cut(part, "@")
		if !ok {
			log.Fatalf("bad peer %q, want id@host:port", part)
		}
		all[id] = addr
		if id != selfID {
			peers = append(peers, raft.Peer{ID: id, Addr: addr})
		}
	}
	return peers, all
}

func main() {

	// These are pointers of flags
	//   when your code hits flag.Parse(), Go looks at your terminal, sees you typed -id 2,
	//   walks over to that exact memory address, and overwrites the "1" with "2"

	// To let you pass settings (like port numbers or node IDs)
	// from your terminal into your Go app without editing code.

	//So that flag.Parse() can modify the value after your program has already started running.
	id := flag.String("id", "1", "node id")
	addr := flag.String("http-addr", ":8080", "listen address")
	dataDir := flag.String("data-dir", "Wal/Logs", "where wal.log lives")
	peerSpec := flag.String("peers", "", "id@host:port,... including self")
	leaderID := flag.String("leader-id", "1", "who is leader (no elections yet)")

	// overwrite the command writeen
	flag.Parse()

	//  if no dir present in Code
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatal(err)
	}

	peers, all := parsePeers(*peerSpec, *id)

	rlog, err := raft.OpenLog(filepath.Join(*dataDir, "wal.log"))
	if err != nil {
		log.Fatalf("open log: %v", err)
	}
	defer rlog.Close()

	st := store.New()

	// The apply callback IS the state machine boundary.
	//
	// Nothing else in the program is allowed to call st.Apply. Every
	// mutation on every node arrives here, in log order, only after it is
	// committed. That is what keeps three independent processes identical.
	applyFn := func(index int64, data []byte) any {
		cmd, err := store.DecodeCommand(data)
		if err != nil {
			log.Printf("WARNING: bad entry %d: %v", index, err)
			return store.Result{}
		}
		return st.Apply(cmd)
	}

	rn := raft.NewNode(*id, *id == *leaderID, peers, rlog, applyFn)

	// Replay what's already on disk BEFORE serving.
	//
	// Entries in our log were committed in a previous life, so re-apply them
	// to rebuild the map. (A strict Raft would persist commitIndex and only
	// replay up to it. With a fixed leader and no elections, replaying the
	// whole log is safe and much simpler.)
	rn.ReplayAll()

	rn.Start()
	defer rn.Stop()

	srv := &Server{id: *id, store: st, raft: rn, leaderAddr: all[*leaderID]}

	mux := http.NewServeMux()
	// Peer traffic and client traffic share one port for simplicity.
	// Real etcd splits them: 2380 for peers, 2379 for clients.
	mux.HandleFunc("POST /raft/append-entries", rn.AppendEntriesHandler)
	mux.HandleFunc("POST /cmd", srv.handleCommand)
	mux.HandleFunc("GET /status", srv.handleStatus)
	mux.HandleFunc("GET /kv", srv.handleRange)
	mux.HandleFunc("GET /kv/{key...}", srv.handleGet)

	log.Printf("node %s listening on %s (leader=%v, peers=%v)",
		*id, *addr, *id == *leaderID, peers)
	if err := http.ListenAndServe(*addr, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
