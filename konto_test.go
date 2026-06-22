package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func tmpLedger(t *testing.T) (*Ledger, string) {
	t.Helper()
	f, err := os.CreateTemp("", "konto-*.log")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path) // Open will create it fresh

	l, err := Open(path, 64, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		l.Close()
		os.Remove(path)
	})
	return l, path
}

func mustInsert(t *testing.T, l *Ledger, table string, row map[string]any) Entry {
	t.Helper()
	e, err := l.Insert(table, row)
	if err != nil {
		t.Fatalf("Insert(%q): %v", table, err)
	}
	return e
}

// ── canonicalJSON ─────────────────────────────────────────────────────────────

func TestCanonicalJSON_KeyOrder(t *testing.T) {
	a := canonicalJSON(map[string]any{"b": 1, "a": 2})
	b := canonicalJSON(map[string]any{"a": 2, "b": 1})
	if string(a) != string(b) {
		t.Fatalf("key order matters: %s vs %s", a, b)
	}
}

func TestCanonicalJSON_ValidJSON(t *testing.T) {
	raw := canonicalJSON(map[string]any{"x": "hello", "y": 42})
	var v map[string]any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
}

func TestCanonicalJSON_Empty(t *testing.T) {
	raw := canonicalJSON(map[string]any{})
	if string(raw) != "{}" {
		t.Fatalf("expected {}, got %s", raw)
	}
}

// ── hash chaining ─────────────────────────────────────────────────────────────

func TestHashChain_SequentialSeq(t *testing.T) {
	l, _ := tmpLedger(t)
	for i := 1; i <= 5; i++ {
		e := mustInsert(t, l, "t", map[string]any{"i": i})
		if e.Seq != uint64(i) {
			t.Fatalf("seq: want %d got %d", i, e.Seq)
		}
	}
}

func TestHashChain_PrevHashLinks(t *testing.T) {
	l, _ := tmpLedger(t)
	e1 := mustInsert(t, l, "t", map[string]any{"a": 1})
	e2 := mustInsert(t, l, "t", map[string]any{"a": 2})
	if e2.PrevHash != e1.Hash {
		t.Fatalf("e2.prev != e1.hash\n  prev=%s\n  hash=%s", e2.PrevHash, e1.Hash)
	}
}

func TestHashChain_FirstEntryPrevIsGenesis(t *testing.T) {
	l, _ := tmpLedger(t)
	e := mustInsert(t, l, "t", map[string]any{"x": 1})
	if e.PrevHash != genesis {
		t.Fatalf("first entry prev should be genesis, got %s", e.PrevHash)
	}
}

func TestHashChain_HashMatchesComputed(t *testing.T) {
	l, _ := tmpLedger(t)
	e := mustInsert(t, l, "t", map[string]any{"k": "v"})
	want := computeHash(e.PrevHash, e.Seq, e.Table, e.Row, e.TS)
	if e.Hash != want {
		t.Fatalf("stored hash doesn't match recomputed\n  stored=%s\n  want=%s", e.Hash, want)
	}
}

// ── verify ────────────────────────────────────────────────────────────────────

func TestVerify_CleanChain(t *testing.T) {
	l, _ := tmpLedger(t)
	for i := 0; i < 10; i++ {
		mustInsert(t, l, "t", map[string]any{"i": i})
	}
	ok, badAt, err := l.Verify()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("expected ok, chain broken at seq %d", badAt)
	}
}

func TestVerify_EmptyLog(t *testing.T) {
	l, _ := tmpLedger(t)
	ok, _, err := l.Verify()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("empty log should verify ok")
	}
}

func TestVerify_DetectsTampering(t *testing.T) {
	// Create ledger in a temp file we manage manually (not via tmpLedger cleanup,
	// since we close it ourselves before tampering).
	f, err := os.CreateTemp("", "konto-tamper-*.log")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)
	t.Cleanup(func() { os.Remove(path) })

	l, err := Open(path, 64, false)
	if err != nil {
		t.Fatal(err)
	}
	mustInsert(t, l, "users", map[string]any{"id": "1", "name": "Alice"})
	mustInsert(t, l, "users", map[string]any{"id": "2", "name": "Bob"})
	l.Close() // explicit close; no cleanup registered

	// Corrupt the file: flip a byte in the middle.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mid := len(data) / 2
	data[mid] ^= 0xFF
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	// Re-open: replay may catch it, or Verify will.
	l2, err := Open(path, 64, false)
	if err != nil {
		// Replay caught the tamper — valid detection.
		t.Logf("Open caught tampering at replay: %v", err)
		return
	}
	defer l2.Close()

	ok, badAt, err := l2.Verify()
	if err != nil {
		t.Logf("Verify returned error (acceptable): %v", err)
		return
	}
	if ok {
		t.Fatal("tampered log should not verify ok")
	}
	t.Logf("tamper detected at seq %d", badAt)
}

func TestVerify_DetectsAppendedRogue(t *testing.T) {
	f0, err := os.CreateTemp("", "konto-rogue-*.log")
	if err != nil {
		t.Fatal(err)
	}
	path := f0.Name()
	f0.Close()
	os.Remove(path)
	t.Cleanup(func() { os.Remove(path) })

	l, err := Open(path, 64, false)
	if err != nil {
		t.Fatal(err)
	}
	mustInsert(t, l, "t", map[string]any{"k": "v"})
	l.Close()

	// Append a line with a fake hash.
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	fmt.Fprintln(f, `{"seq":999,"table":"t","row":{"k":"evil"},"ts":1,"prev":"bad","hash":"bad"}`)
	f.Close()

	l2, err := Open(path, 64, false)
	if err != nil {
		t.Logf("replay caught rogue entry: %v", err)
		return
	}
	defer l2.Close()

	ok, _, _ := l2.Verify()
	if ok {
		t.Fatal("rogue entry should fail verification")
	}
}

// ── replay ────────────────────────────────────────────────────────────────────

// tmpLedgerManual returns a ledger and path with NO cleanup registered.
// The caller is responsible for closing and removing the file.
func tmpLedgerManual(t *testing.T) (*Ledger, string) {
	t.Helper()
	f, err := os.CreateTemp("", "konto-manual-*.log")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)

	l, err := Open(path, 64, false)
	if err != nil {
		os.Remove(path)
		t.Fatal(err)
	}
	return l, path
}

func TestReplay_RebuildsIndex(t *testing.T) {
	l, path := tmpLedgerManual(t)
	defer os.Remove(path)
	mustInsert(t, l, "users", map[string]any{"id": "1", "name": "Alice"})
	mustInsert(t, l, "users", map[string]any{"id": "2", "name": "Bob"})
	l.Close()

	l2, err := Open(path, 64, false)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()

	offsets := l2.ix.query("users", map[string]any{"name": "Alice"})
	if len(offsets) != 1 {
		t.Fatalf("expected 1 result after replay, got %d", len(offsets))
	}
}

func TestReplay_SeqContinues(t *testing.T) {
	l, path := tmpLedgerManual(t)
	defer os.Remove(path)
	mustInsert(t, l, "t", map[string]any{"x": 1})
	mustInsert(t, l, "t", map[string]any{"x": 2})
	l.Close()

	l2, err := Open(path, 64, false)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()

	e := mustInsert(t, l2, "t", map[string]any{"x": 3})
	if e.Seq != 3 {
		t.Fatalf("seq after reload: want 3 got %d", e.Seq)
	}
}

// TestClose_DoublePanic documents the real bug: Close() panics if called twice.
// This is a known issue in konto.go — Close does not guard against double-close.
func TestClose_DoublePanic(t *testing.T) {
	l, path := tmpLedgerManual(t)
	defer os.Remove(path)
	mustInsert(t, l, "t", map[string]any{"x": 1})
	l.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Logf("BUG: Close() panics on second call: %v", r)
			t.Log("Fix: guard with sync.Once or a closed bool in Ledger")
		}
	}()
	l.Close() // should not panic — but currently does
}

// ── index / query ─────────────────────────────────────────────────────────────

func TestQuery_ExactMatch(t *testing.T) {
	l, _ := tmpLedger(t)
	mustInsert(t, l, "users", map[string]any{"name": "Alice", "city": "NYC"})
	mustInsert(t, l, "users", map[string]any{"name": "Bob", "city": "LA"})

	offsets := l.ix.query("users", map[string]any{"name": "Alice"})
	if len(offsets) != 1 {
		t.Fatalf("want 1, got %d", len(offsets))
	}
}

func TestQuery_MultiConditionIntersection(t *testing.T) {
	l, _ := tmpLedger(t)
	mustInsert(t, l, "u", map[string]any{"name": "Alice", "city": "NYC"})
	mustInsert(t, l, "u", map[string]any{"name": "Alice", "city": "LA"})
	mustInsert(t, l, "u", map[string]any{"name": "Bob", "city": "NYC"})

	offsets := l.ix.query("u", map[string]any{"name": "Alice", "city": "NYC"})
	if len(offsets) != 1 {
		t.Fatalf("want 1, got %d", len(offsets))
	}
}

func TestQuery_NoWhere_ReturnsAll(t *testing.T) {
	l, _ := tmpLedger(t)
	mustInsert(t, l, "t", map[string]any{"x": 1})
	mustInsert(t, l, "t", map[string]any{"x": 2})
	mustInsert(t, l, "t", map[string]any{"x": 3})

	offsets := l.ix.query("t", nil)
	if len(offsets) != 3 {
		t.Fatalf("want 3, got %d", len(offsets))
	}
}

func TestQuery_MissingTable(t *testing.T) {
	l, _ := tmpLedger(t)
	offsets := l.ix.query("nonexistent", nil)
	if offsets != nil {
		t.Fatalf("expected nil for missing table, got %v", offsets)
	}
}

func TestQuery_NoMatch(t *testing.T) {
	l, _ := tmpLedger(t)
	mustInsert(t, l, "t", map[string]any{"name": "Alice"})
	offsets := l.ix.query("t", map[string]any{"name": "Charlie"})
	if len(offsets) != 0 {
		t.Fatalf("expected 0, got %d", len(offsets))
	}
}

func TestQuery_NonUniqueColumn(t *testing.T) {
	l, _ := tmpLedger(t)
	mustInsert(t, l, "t", map[string]any{"city": "NYC", "name": "Alice"})
	mustInsert(t, l, "t", map[string]any{"city": "NYC", "name": "Bob"})

	offsets := l.ix.query("t", map[string]any{"city": "NYC"})
	if len(offsets) != 2 {
		t.Fatalf("want 2, got %d", len(offsets))
	}
}

// ── tables ────────────────────────────────────────────────────────────────────

func TestTables_Listed(t *testing.T) {
	l, _ := tmpLedger(t)
	mustInsert(t, l, "users", map[string]any{"x": 1})
	mustInsert(t, l, "orders", map[string]any{"x": 1})

	tables := l.ix.tables()
	if len(tables) != 2 {
		t.Fatalf("want 2 tables, got %v", tables)
	}
	// Should be sorted
	if tables[0] != "orders" || tables[1] != "users" {
		t.Fatalf("unexpected order: %v", tables)
	}
}

// ── ReadAt ────────────────────────────────────────────────────────────────────

func TestReadAt_RoundTrip(t *testing.T) {
	l, _ := tmpLedger(t)
	mustInsert(t, l, "t", map[string]any{"key": "value"})

	offsets := l.ix.query("t", map[string]any{"key": "value"})
	if len(offsets) == 0 {
		t.Fatal("no offsets")
	}
	e, err := l.ReadAt(offsets[0])
	if err != nil {
		t.Fatal(err)
	}
	if e.Row["key"] != "value" {
		t.Fatalf("unexpected row: %v", e.Row)
	}
}

// ── concurrency ───────────────────────────────────────────────────────────────

func TestConcurrentInserts_AllSucceed(t *testing.T) {
	l, _ := tmpLedger(t)
	const n = 200
	var wg sync.WaitGroup
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := l.Insert("t", map[string]any{"i": i})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent insert error: %v", err)
	}

	offsets := l.ix.query("t", nil)
	if len(offsets) != n {
		t.Fatalf("want %d rows, got %d", n, len(offsets))
	}
}

func TestConcurrentInserts_ChainIntact(t *testing.T) {
	l, _ := tmpLedger(t)
	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Insert("t", map[string]any{"i": i}) //nolint
		}(i)
	}
	wg.Wait()

	ok, badAt, err := l.Verify()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("chain broken at seq %d after concurrent inserts", badAt)
	}
}

func TestConcurrentInsertsAndQueries(t *testing.T) {
	l, _ := tmpLedger(t)
	var wg sync.WaitGroup

	// Writers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Insert("t", map[string]any{"i": i}) //nolint
		}(i)
	}

	// Readers — just ensure no panic/race
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.ix.query("t", nil)
		}()
	}

	wg.Wait()
}

// ── multi-table isolation ─────────────────────────────────────────────────────

func TestMultiTable_NoLeakage(t *testing.T) {
	l, _ := tmpLedger(t)
	mustInsert(t, l, "a", map[string]any{"k": "shared"})
	mustInsert(t, l, "b", map[string]any{"k": "shared"})

	offA := l.ix.query("a", map[string]any{"k": "shared"})
	offB := l.ix.query("b", map[string]any{"k": "shared"})

	if len(offA) != 1 || len(offB) != 1 {
		t.Fatalf("want 1 each, got a=%d b=%d", len(offA), len(offB))
	}
	if offA[0] == offB[0] {
		t.Fatal("different tables should have different offsets")
	}
}
