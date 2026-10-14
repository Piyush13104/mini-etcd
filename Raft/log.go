package raft

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	wal "github.com/Piyush22070/mini-etcd/Wal"
)

// Log is your append-only WAL file plus an in-memory index over it.
//
// WHY the index: a flat file can only be read front to back, but the leader
// constantly needs "entries from 47 onward" for a specific follower, and a
// follower sometimes needs to delete a bad suffix. Keeping the entries and
// their byte offsets in RAM makes both O(1). This is exactly the problem
// real databases solve with a B+tree (etcd/bbolt) or an LSM tree
// (CockroachDB/Pebble) -- we solve it with a slice because our data is small.
type Log struct {
	mu   sync.RWMutex
	file *os.File

	// entries[i] holds log index i+1. Log indexes start at 1, because
	// index 0 means "no entry" in the AppendEntries protocol.
	entries []wal.Entry

	// offsets[i] = byte position of entry i+1 in the file.
	// Needed only for truncation: to delete entry N onward we just chop
	// the file at offsets[N-1].
	offsets []int64
}

// OpenLog loads an existing log file into memory, or creates a new one.
func OpenLog(path string) (*Log, error) {

	// code seeks the file -- the OS enforces it, we can't get it wrong.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	// file
	l := &Log{file: f}

	// Read the whole file, remembering where each entry starts.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	for {
		// Current position = where THIS entry begins.
		pos, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		e, err := wal.ReadNextEntry(f)
		if err == io.EOF {
			break // clean end of file
		}
		if err != nil {
			// A half-written final record is the NORMAL result of a crash --
			// it is the exact thing a WAL exists to survive. Chop it off and
			// carry on. Refusing to boot here would mean one bad shutdown
			// bricks the node forever.
			log.Printf("WARNING: torn record at offset %d, truncating: %v", pos, err)
			if terr := f.Truncate(pos); terr != nil {
				return nil, terr
			}
			break
		}

		l.entries = append(l.entries, *e)
		l.offsets = append(l.offsets, pos)
	}

	return l, nil
}

// last Index of Logs
func (l *Log) LastIndex() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return int64(len(l.entries))
}

// TermAt reports the term of entry `index`, and whether it exists.
//
// WHY it returns two values: "I don't have that entry" and "I have it with
// a different term" are different situations for the leader, and both mean
// "step back and retry". Squashing them into a 0 would be ambiguous.
func (l *Log) TermAt(index int64) (int64, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index <= 0 || index > int64(len(l.entries)) {
		return 0, false
	}
	return l.entries[index-1].Term, true
}

func (l *Log) At(index int64) (wal.Entry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index <= 0 || index > int64(len(l.entries)) {
		return wal.Entry{}, false
	}
	return l.entries[index-1], true
}

// From returns every entry from `index` to the end -- what the leader ships
// to a follower that is behind.
func (l *Log) From(index int64) []wal.Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index <= 0 || index > int64(len(l.entries)) {
		return nil
	}
	// Copy: the caller marshals this to JSON on another goroutine, and
	// handing out the live slice would be a data race.
	out := make([]wal.Entry, len(l.entries[index-1:]))
	copy(out, l.entries[index-1:])
	return out
}

// Append writes an entry to disk (fsynced) and to the in-memory index.
func (l *Log) Append(e wal.Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	pos, err := l.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	// WriteWAL already fsyncs. Disk FIRST: if we crash right after this,
	// replay finds the entry. If we updated memory first and crashed, we
	// would have acknowledged data that no longer exists.
	if err := wal.WriteWAL(l.file, e); err != nil {
		return err
	}

	l.entries = append(l.entries, e)
	l.offsets = append(l.offsets, pos)
	return nil
}

// TruncateFrom deletes entry `index` AND EVERYTHING AFTER IT.
//
// WHY delete the whole suffix and not just the one bad entry: entries after
// a conflicting one were built on a premise now known to be false. Raft's
// Log Matching Property says if two logs agree at some (index, term) then
// everything before it agrees too -- keeping a later entry past a conflict
// would break that, and the leader could never trust its consistency check
// again.
//
// This is also where a minority partition's uncommitted writes get thrown
// away. Raft never merges logs. The leader's log is the truth.
func (l *Log) TruncateFrom(index int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index <= 0 || index > int64(len(l.entries)) {
		return nil // nothing to cut
	}

	cutAt := l.offsets[index-1]
	if err := l.file.Truncate(cutAt); err != nil {
		return fmt.Errorf("truncate file: %w", err)
	}
	if err := l.file.Sync(); err != nil {
		return err
	}

	l.entries = l.entries[:index-1]
	l.offsets = l.offsets[:index-1]
	return nil
}

func (l *Log) Close() error { return l.file.Close() }
