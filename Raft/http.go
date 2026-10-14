package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

var ErrNotLeader = errors.New("not leader")

// rpcClient is shared and has a timeout.
//
// WHY the timeout matters so much here: a dead peer must fail FAST. With
// http.DefaultClient (no timeout) a crashed follower would hang the
// replication goroutine forever, and the leader would stop heartbeating
// the healthy nodes too. The timeout must be well under the time you are
// willing to wait for a write.
var rpcClient = &http.Client{Timeout: 2 * time.Second}

func sendAppendEntries(addr string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	var out AppendEntriesResponse

	body, err := json.Marshal(req)
	if err != nil {
		return out, err
	}
	resp, err := rpcClient.Post(
		"http://"+addr+"/raft/append-entries",
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		return out, err // peer down, DNS failure, timeout -- all the same to us
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("peer returned %s", resp.Status)
	}
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

// AppendEntriesHandler is the server side. Mount it at
// POST /raft/append-entries.
//
// Note entry Data is []byte, which Go's json package encodes as base64
// automatically -- so binary values travel safely without extra work.
func (n *Node) AppendEntriesHandler(w http.ResponseWriter, r *http.Request) {
	var req AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	resp := n.HandleAppendEntries(req)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
