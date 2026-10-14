package store

import "testing"

func put(s *Store, k, v string) Result { return s.Apply(Command{Op: OpPut, Key: k, Value: v}) }
func del(s *Store, k string) Result    { return s.Apply(Command{Op: OpDelete, Key: k}) }

// Catches the classic MVCC mistake: conflating the per-key version counter
// with the global revision counter.
func TestUpdateKeepsCreateRevision(t *testing.T) {
	s := New()
	put(s, "/a", "1") // global rev 1
	put(s, "/b", "x") // global rev 2  <- a DIFFERENT key moves the clock
	put(s, "/a", "2") // global rev 3

	v, _ := s.Get("/a")
	if v.CreateRevision != 1 {
		t.Errorf("create_revision = %d, want 1 (must freeze at creation)", v.CreateRevision)
	}
	if v.Version != 2 {
		t.Errorf("version = %d, want 2 (/a changed twice)", v.Version)
	}
	if v.ModRevision != 3 {
		t.Errorf("mod_revision = %d, want 3 (global clock, not /a's counter)", v.ModRevision)
	}
}

func TestDeleteIsTombstone(t *testing.T) {
	s := New()
	put(s, "/a", "1")
	del(s, "/a")

	if _, err := s.Get("/a"); err != ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	// The history must OUTLIVE the delete, or time travel and Watch break.
	if n := len(s.kv["/a"]); n != 2 {
		t.Fatalf("history = %d entries, want 2 (value + tombstone)", n)
	}
}

func TestDeleteMissingKeyDoesNotMoveRevision(t *testing.T) {
	s := New()
	put(s, "/a", "1")
	r := del(s, "/missing")

	if r.Applied {
		t.Error("a no-op must not report Applied")
	}
	// If this ever bumps, nodes that disagree about whether a key existed
	// will drift apart forever.
	if r.Revision != 1 {
		t.Errorf("revision = %d, want 1", r.Revision)
	}
}

func TestGetAtRevision(t *testing.T) {
	s := New()
	put(s, "/a", "piyush") // rev 1
	put(s, "/a", "bhoyar") // rev 2
	del(s, "/a")           // rev 3

	cases := []struct {
		rev     int64
		want    string
		wantErr bool
	}{
		{rev: 0, wantErr: true}, // did not exist yet
		{rev: 1, want: "piyush"},
		{rev: 2, want: "bhoyar"},
		{rev: 3, wantErr: true},  // deleted at 3
		{rev: 99, wantErr: true}, // still deleted
	}
	for _, tc := range cases {
		v, err := s.GetAtRevision("/a", tc.rev)
		switch {
		case tc.wantErr && err == nil:
			t.Errorf("rev %d: got %q, want error", tc.rev, v.Value)
		case !tc.wantErr && err != nil:
			t.Errorf("rev %d: %v", tc.rev, err)
		case !tc.wantErr && v.Value != tc.want:
			t.Errorf("rev %d: got %q, want %q", tc.rev, v.Value, tc.want)
		}
	}
}

func TestRangeExcludesTombstones(t *testing.T) {
	s := New()
	put(s, "/cfg/db", "postgres")
	put(s, "/cfg/port", "5432")
	put(s, "/other", "zzz")
	put(s, "/cfg/gone", "x")
	del(s, "/cfg/gone")

	got := s.Range("/cfg/")
	if len(got) != 2 {
		t.Fatalf("got %d, want 2: %+v", len(got), got)
	}
	if got[0].Key != "/cfg/db" || got[1].Key != "/cfg/port" {
		t.Fatalf("not sorted or wrong keys: %q %q", got[0].Key, got[1].Key)
	}
}

// THE MOST IMPORTANT TEST IN THE PROJECT.
//
// Replaying the same commands on two fresh stores must give byte-identical
// state. This is the exact property Raft depends on: three nodes, same log,
// same result. If this test ever fails, your cluster will diverge silently
// in production and you will have no way to detect it.
//
// Re-run this after EVERY change to Apply.
func TestDeterminism(t *testing.T) {
	cmds := []Command{
		{Op: OpPut, Key: "/a", Value: "1"},
		{Op: OpPut, Key: "/b", Value: "2"},
		{Op: OpPut, Key: "/a", Value: "3"},
		{Op: OpDelete, Key: "/b"},
		{Op: OpDelete, Key: "/nope"}, // no-op must behave identically on both
	}

	a, b := New(), New()
	for _, c := range cmds {
		a.Apply(c)
	}
	for _, c := range cmds {
		b.Apply(c)
	}

	sa, _ := a.Snapshot()
	sb, _ := b.Snapshot()
	if string(sa) != string(sb) {
		t.Fatalf("DIVERGENCE:\n%s\n%s", sa, sb)
	}
}

func TestSnapshotKeepsHistory(t *testing.T) {
	s := New()
	put(s, "/a", "1")
	put(s, "/a", "2")

	blob, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	fresh := New()
	if err := fresh.Restore(blob); err != nil {
		t.Fatal(err)
	}

	if fresh.Revision() != s.Revision() {
		t.Errorf("revision = %d, want %d", fresh.Revision(), s.Revision())
	}
	// Current values surviving isn't enough -- the HISTORY has to survive,
	// or a post-snapshot node can't serve time-travel reads or watches.
	if v, err := fresh.GetAtRevision("/a", 1); err != nil || v.Value != "1" {
		t.Errorf("history lost: %+v %v", v, err)
	}
}

func TestDecodeCommand(t *testing.T) {
	if _, err := DecodeCommand([]byte(`{"op":"put","key":"/a","value":"1"}`)); err != nil {
		t.Errorf("lowercase op should normalise: %v", err)
	}
	if _, err := DecodeCommand([]byte(`{"op":"PUT","key":"","value":"1"}`)); err == nil {
		t.Error("empty key must be rejected")
	}
	if _, err := DecodeCommand([]byte(`{"op":"DROP","key":"/a"}`)); err == nil {
		t.Error("unknown op must be rejected")
	}
	if _, err := DecodeCommand([]byte(`not json`)); err == nil {
		t.Error("garbage must be rejected")
	}
}
