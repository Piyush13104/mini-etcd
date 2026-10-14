// State Machine

package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var ErrNotFound = errors.New("Key Not Found...")

type Op string

const (
	OpPut    Op = "PUT"
	OpDelete Op = "DELETE"
)

// main Payload fir State Machine
type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// for PUT and DELETE command
func DecodeCommand(b []byte) (Command, error) {
	var c Command

	if err := json.Unmarshal(b, &c); err != nil {
		return c, fmt.Errorf("decode command: %w", err)
	}
	// accept "Put" / "PUT" / "put"
	// this is string function
	c.Op = Op(strings.ToUpper(string(c.Op)))

	// if not OpPut or not OpDelete
	switch c.Op {
	case OpPut: // grab snacker do nothing
	case OpDelete: // same
	default:
		{
			return c, fmt.Errorf("Unknown Op %q (want PUT or DELETE)", c.Op)
		}
	}

	if c.Key == "" {
		return c, errors.New("empty key")
	}

	// everthing is fine
	return c, nil
}

type VersionedValue struct {
	Value string `json:"value"`

	// Global revsions during when the key was first created -- birthcertificate
	CreateRevision int64 `json:"create_revision"`

	// Gloabal revision when this version was written
	// Moves on every update. Used for compare-and-swap later. -- after each update
	ModRevision int64 `json:"mod_revision"`

	// how many times key has been changed
	Version int64 `json:"version"`

	// Deleted marks a tombstone.
	// WHY not just delete from the map: removing the entry would destroy the
	// history, and a Watch client asking "what changed since revision 5"
	// would never learn the key was deleted. A deletion IS an event.
	Deleted bool `json:"deleted,omitempty"`
}

// KeyValue
type KeyValue struct {
	Key string `json:"key"`
	VersionedValue
}

// that we going to return
type Result struct {
	Revision int64           `json:"revision"`
	Key      string          `json:"key"`
	Applied  bool            `json:"applied"`
	Value    *VersionedValue `json:"value,omitempty"`
}

type Store struct {
	mu sync.RWMutex

	// global clock +1 after mutation
	rev int64 // by default 0
	// array of all versions
	kv map[string][]VersionedValue
}

// Constructer to create store
func New() *Store {
	return &Store{kv: make(map[string][]VersionedValue)}
}

// get the revision
func (s *Store) Revision() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rev
}

// main
func (s *Store) Apply(cmd Command) Result {

	// mutex on store
	s.mu.Lock()
	defer s.mu.Unlock()

	// retriving the old data
	history := s.kv[cmd.Key]

	// get the latest
	// prev vesrion (lastest)// has Prev version or not
	prev, alive := live(history)

	switch cmd.Op {
	case OpPut:
		{
			s.rev++
			// new version creating no birth no mod
			v := VersionedValue{
				Value:          cmd.Value,
				CreateRevision: s.rev,
				ModRevision:    s.rev,
				Version:        1,
			}

			if alive {
				// update the birth certificate
				v.CreateRevision = prev.CreateRevision

				// increase the version number
				v.Version = prev.Version + 1
			}

			// append the new version
			s.kv[cmd.Key] = append(history, v)

			return Result{Revision: s.rev, Key: cmd.Key, Applied: true, Value: &v}
		}
	case OpDelete:
		{
			// if no version
			if !alive {
				return Result{Revision: s.rev, Key: cmd.Key, Applied: false}
			}

			s.rev++
			v := VersionedValue{
				// copying birthcertifcate
				CreateRevision: prev.CreateRevision,
				ModRevision:    s.rev,
				Version:        prev.Version + 1,
				Deleted:        true,
			}
			s.kv[cmd.Key] = append(history, v)
			return Result{Revision: s.rev, Key: cmd.Key, Applied: true, Value: &v}
		}
	}

	// if failed
	return Result{Revision: s.rev, Key: cmd.Key, Applied: false}

}

// getting version
func (s *Store) Get(key string) (VersionedValue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if v, alive := live(s.kv[key]); alive {
		return v, nil
	}
	// If key not found
	return VersionedValue{}, ErrNotFound
}

// GetAtRevision answers "
func (s *Store) GetAtRevision(key string, rev int64) (VersionedValue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	history := s.kv[key]

	for i := len(history) - 1; i >= 0; i-- {
		// if we found the version which is less than rev
		if history[i].ModRevision <= rev {
			if history[i].Deleted {
				return VersionedValue{}, ErrNotFound
			}
			return history[i], nil
		}
	}

	return VersionedValue{}, ErrNotFound
}

// every live key with given prefix
func (s *Store) Range(prefix string) []KeyValue {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]KeyValue, 0, len(s.kv))

	for k, history := range s.kv {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if v, alive := live(history); alive {
			out = append(out, KeyValue{Key: k, VersionedValue: v})
		}
	}

	// go makes the maps randomised so
	sort.Slice(out, func(i, j int) bool {
		return out[i].Key < out[j].Key
	})
	return out
}

// data snapshots
type snapshotData struct {
	Revision int64                       `json:"revision"`
	KV       map[string][]VersionedValue `json:"kv"`
}

func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(snapshotData{Revision: s.rev, KV: s.kv})
}

func (s *Store) Restore(b []byte) error {
	var d snapshotData
	if err := json.Unmarshal(b, &d); err != nil {
		return err
	}
	if d.KV == nil {
		d.KV = make(map[string][]VersionedValue)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rev, s.kv = d.Revision, d.KV
	return nil
}

// return the newest version of key
func live(history []VersionedValue) (VersionedValue, bool) {

	// if no version
	if len(history) == 0 {
		return VersionedValue{}, false
	}
	// last entry
	v := history[len(history)-1]

	// if deleted
	if v.Deleted {
		return VersionedValue{}, false
	}

	// return latest of version
	return v, true
}
