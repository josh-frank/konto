/*
 * konto — tamper-evident append-only database in one Go file
 *
 * Storage: newline-delimited JSON log (append.log).
 * Every entry's hash covers prev_hash+seq+table+canonical_row_json+ts,
 * forming an unbroken chain detectable by GET /__konto.
 *
 * Indexes (both rebuilt from the log at startup):
 *
 *   Hash index  map[table][col][val]→[]offset
 *     O(1) exact-match on any column; also enforces PK uniqueness at insert time.
 *     Weakness: useless for ranges, full scans are O(n) with a dedup pass.
 *
 *   B-tree index  per-table B-tree keyed on (seq, ts, offset)
 *     O(log n) ordered access; supports range queries on seq and ts.
 *     Weakness: only covers the two built-in numeric keys (seq, ts), not
 *     arbitrary columns — those still go through the hash index.
 *
 *   Together: exact column match → hash index (O(1)); range/order → B-tree
 *   (O(log n)); both path types avoid full log scans.
 *
 * API:
 *   POST /<table>                                    insert row (pk mandatory)
 *   GET  /<table>                                    all rows (B-tree order)
 *   GET  /<table>?col=val[&col2=val2]                exact-match filter
 *   GET  /<table>?seq_gte=N&seq_lte=M                seq range
 *   GET  /<table>?ts_gte=N&ts_lte=M                  ts range (Unix ms)
 *   GET  /<table>?seq_gte=N&col=val                  range + column filter
 *   GET  /__konto                                    health / meta / verify
 *
 * Threading: single writer goroutine; RWMutex on both indexes; Verify on
 * its own fd so it never blocks inserts or queries.
 *
 * Zero external dependencies — stdlib only.
 * Build: go build -o konto konto.go
 */

package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ── entry ─────────────────────────────────────────────────────────────────────

type Entry struct {
	Seq      uint64         `json:"seq"`
	Table    string         `json:"table"`
	Row      map[string]any `json:"row"`
	TS       int64          `json:"ts"`
	PrevHash string         `json:"prev"`
	Hash     string         `json:"hash"`
}

// ── hashing ───────────────────────────────────────────────────────────────────

func computeHash(prev string, seq uint64, table string, row map[string]any, ts int64) string {
	h := sha256.New()
	io.WriteString(h, prev)
	io.WriteString(h, strconv.FormatUint(seq, 10))
	io.WriteString(h, table)
	h.Write(canonicalJSON(row))
	io.WriteString(h, strconv.FormatInt(ts, 10))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func canonicalJSON(m map[string]any) []byte {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		vb, _ := json.Marshal(m[k])
		b.Write(kb)
		b.WriteByte(':')
		b.Write(vb)
	}
	b.WriteByte('}')
	return []byte(b.String())
}

// ── B-tree (stdlib-only, order-16) ───────────────────────────────────────────
//
// A classic B-tree keyed on btreeKey{seq, ts, offset}.
// All three fields together are globally unique (seq alone would suffice,
// but carrying ts and offset avoids a second lookup in range scans).
//
// Degree d=16 → each node holds 15..31 keys; tree height ≤ ceil(log_16(n)).
// At 1 million rows that's ≤5 levels — every range scan touches ≤5 nodes
// before it starts streaming leaf keys in order.

const btreeDegree = 16 // min keys per non-root node = d-1 = 15

type btreeKey struct {
	seq    uint64
	ts     int64
	offset int64
}

func (a btreeKey) less(b btreeKey) bool {
	if a.seq != b.seq {
		return a.seq < b.seq
	}
	if a.ts != b.ts {
		return a.ts < b.ts
	}
	return a.offset < b.offset
}

type btreeNode struct {
	keys     []btreeKey
	children []*btreeNode // nil for leaves
}

func (n *btreeNode) isLeaf() bool { return n.children == nil }

type btree struct {
	root *btreeNode
	size int
}

func newBtree() *btree {
	return &btree{root: &btreeNode{}}
}

// insert adds k into the tree (duplicates silently ignored).
func (t *btree) insert(k btreeKey) {
	// If root is full, split it first.
	if len(t.root.keys) == 2*btreeDegree-1 {
		old := t.root
		t.root = &btreeNode{children: []*btreeNode{old}}
		t.splitChild(t.root, 0)
	}
	t.insertNonFull(t.root, k)
	t.size++
}

func (t *btree) insertNonFull(n *btreeNode, k btreeKey) {
	i := len(n.keys) - 1
	if n.isLeaf() {
		n.keys = append(n.keys, btreeKey{})
		for i >= 0 && k.less(n.keys[i]) {
			n.keys[i+1] = n.keys[i]
			i--
		}
		n.keys[i+1] = k
		return
	}
	for i >= 0 && k.less(n.keys[i]) {
		i--
	}
	i++
	if len(n.children[i].keys) == 2*btreeDegree-1 {
		t.splitChild(n, i)
		if n.keys[i].less(k) {
			i++
		}
	}
	t.insertNonFull(n.children[i], k)
}

func (t *btree) splitChild(parent *btreeNode, i int) {
	d := btreeDegree
	full := parent.children[i]
	sibling := &btreeNode{}
	mid := full.keys[d-1]

	sibling.keys = append(sibling.keys, full.keys[d:]...)
	full.keys = full.keys[:d-1]

	if !full.isLeaf() {
		sibling.children = append(sibling.children, full.children[d:]...)
		full.children = full.children[:d]
	}

	// Insert mid key and new child into parent.
	parent.keys = append(parent.keys, btreeKey{})
	copy(parent.keys[i+1:], parent.keys[i:])
	parent.keys[i] = mid

	parent.children = append(parent.children, nil)
	copy(parent.children[i+2:], parent.children[i+1:])
	parent.children[i+1] = sibling
}

// scan calls fn(key) for every key with lo ≤ key ≤ hi (inclusive), in order.
// Return false from fn to stop early.
func (t *btree) scan(lo, hi btreeKey, fn func(btreeKey) bool) {
	t.scanNode(t.root, lo, hi, fn)
}

func (t *btree) scanNode(n *btreeNode, lo, hi btreeKey, fn func(btreeKey) bool) bool {
	if n == nil {
		return true
	}
	for i, k := range n.keys {
		// Descend into child[i] (keys < k) if it could contain values in [lo,hi].
		if !n.isLeaf() {
			// child[i] holds keys < k; skip only if all those keys are < lo,
			// which happens when k <= lo (so child keys are all < lo too).
			// Equivalently: descend unless k.less(lo) AND k != lo.
			if !k.less(lo) || !lo.less(k) { // k >= lo, so child might have keys in range
				if !t.scanNode(n.children[i], lo, hi, fn) {
					return false
				}
			}
		}
		// Emit k if lo <= k <= hi.
		if !k.less(lo) && !hi.less(k) {
			if !fn(k) {
				return false
			}
		}
		// If k > hi, no need to look further right.
		if hi.less(k) {
			return true
		}
	}
	// Descend into rightmost child.
	if !n.isLeaf() {
		return t.scanNode(n.children[len(n.children)-1], lo, hi, fn)
	}
	return true
}

// allOffsets returns every offset in insertion (seq) order.
func (t *btree) allOffsets() []int64 {
	out := make([]int64, 0, t.size)
	lo := btreeKey{seq: 0}
	hi := btreeKey{seq: ^uint64(0), ts: 1<<63 - 1, offset: 1<<63 - 1}
	t.scan(lo, hi, func(k btreeKey) bool {
		out = append(out, k.offset)
		return true
	})
	return out
}

// rangeOffsets returns offsets for entries where seqLo≤seq≤seqHi AND tsLo≤ts≤tsHi.
// Pass 0/maxint to leave a bound open.
func (t *btree) rangeOffsets(seqLo, seqHi uint64, tsLo, tsHi int64) []int64 {
	lo := btreeKey{seq: seqLo}
	hi := btreeKey{seq: seqHi, ts: tsHi, offset: 1<<63 - 1}
	var out []int64
	t.scan(lo, hi, func(k btreeKey) bool {
		if k.ts >= tsLo && k.ts <= tsHi {
			out = append(out, k.offset)
		}
		return true
	})
	return out
}

// ── hash index ────────────────────────────────────────────────────────────────

type idx struct {
	mu     sync.RWMutex
	data   map[string]map[string]map[string][]int64 // table→col→val→offsets
	btrees map[string]*btree                        // table→btree
}

func newIdx() *idx {
	return &idx{
		data:   make(map[string]map[string]map[string][]int64),
		btrees: make(map[string]*btree),
	}
}

// add indexes one row into both the hash index and the B-tree.
// Must be called under write lock (or at startup before goroutines start).
func (ix *idx) add(table string, row map[string]any, offset int64, seq uint64, ts int64) {
	// Hash index.
	t, ok := ix.data[table]
	if !ok {
		t = make(map[string]map[string][]int64)
		ix.data[table] = t
	}
	for col, val := range row {
		vs := fmt.Sprintf("%v", val)
		if t[col] == nil {
			t[col] = make(map[string][]int64)
		}
		t[col][vs] = append(t[col][vs], offset)
	}

	// B-tree index.
	bt, ok := ix.btrees[table]
	if !ok {
		bt = newBtree()
		ix.btrees[table] = bt
	}
	bt.insert(btreeKey{seq: seq, ts: ts, offset: offset})
}

// query returns sorted offsets matching all where conditions.
// If rangeFilter is non-nil it is applied as a post-filter on B-tree results.
func (ix *idx) query(table string, where map[string]any) []int64 {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	t, ok := ix.data[table]
	if !ok {
		return nil
	}

	if len(where) == 0 {
		// Full scan — use B-tree for ordered result, no dedup needed.
		bt := ix.btrees[table]
		if bt == nil {
			return nil
		}
		return bt.allOffsets()
	}

	// Exact-match via hash index.
	var result []int64
	for col, val := range where {
		vs := fmt.Sprintf("%v", val)
		colMap, ok := t[col]
		if !ok {
			return nil
		}
		offsets, ok := colMap[vs]
		if !ok {
			return nil
		}
		if result == nil {
			result = make([]int64, len(offsets))
			copy(result, offsets)
			continue
		}
		set := make(map[int64]struct{}, len(offsets))
		for _, o := range offsets {
			set[o] = struct{}{}
		}
		keep := result[:0]
		for _, o := range result {
			if _, ok := set[o]; ok {
				keep = append(keep, o)
			}
		}
		result = keep
		if len(result) == 0 {
			return nil
		}
	}
	return result
}

// queryRange uses the B-tree for seq/ts range, then intersects with hash index
// if additional column filters are present.
func (ix *idx) queryRange(table string, seqLo, seqHi uint64, tsLo, tsHi int64, where map[string]any) []int64 {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	bt, ok := ix.btrees[table]
	if !ok {
		return nil
	}
	offsets := bt.rangeOffsets(seqLo, seqHi, tsLo, tsHi)
	if len(where) == 0 || len(offsets) == 0 {
		return offsets
	}

	// Build a set from B-tree results, then intersect with hash index matches.
	btSet := make(map[int64]struct{}, len(offsets))
	for _, o := range offsets {
		btSet[o] = struct{}{}
	}

	t := ix.data[table]
	var result []int64
	for col, val := range where {
		vs := fmt.Sprintf("%v", val)
		colMap, ok := t[col]
		if !ok {
			return nil
		}
		hashOffsets, ok := colMap[vs]
		if !ok {
			return nil
		}
		if result == nil {
			for _, o := range hashOffsets {
				if _, inBT := btSet[o]; inBT {
					result = append(result, o)
				}
			}
			continue
		}
		set := make(map[int64]struct{}, len(hashOffsets))
		for _, o := range hashOffsets {
			set[o] = struct{}{}
		}
		keep := result[:0]
		for _, o := range result {
			if _, ok := set[o]; ok {
				keep = append(keep, o)
			}
		}
		result = keep
		if len(result) == 0 {
			return nil
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}


func (ix *idx) tables() []string {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	out := make([]string, 0, len(ix.data))
	for t := range ix.data {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// ── ledger ────────────────────────────────────────────────────────────────────

type insertJob struct {
	table string
	row   map[string]any
	reply chan<- insertResult
}

type insertResult struct {
	entry Entry
	err   error
}

type Ledger struct {
	closeOnce sync.Once
	path      string
	doSync    bool
	file      *os.File   // write fd (append)
	rfile     *os.File   // read-only fd for ReadAt; never written
	bw        *bufio.Writer
	ix        *idx
	insertCh  chan insertJob
	lastHash  string
	seq       uint64
}

const genesis = "0000000000000000000000000000000000000000000000000000000000000000"

func Open(path string, chanBuf int, doSync bool) (*Ledger, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	l := &Ledger{
		path:     path,
		doSync:   doSync,
		file:     f,
		bw:       bufio.NewWriterSize(f, 64*1024),
		ix:       newIdx(),
		insertCh: make(chan insertJob, chanBuf),
		lastHash: genesis,
	}
	rfile, err := os.Open(path)
	if err != nil {
		f.Close()
		return nil, err
	}
	l.rfile = rfile
	if err := l.replay(); err != nil {
		f.Close()
		rfile.Close()
		return nil, err
	}
	go l.writerLoop()
	return l, nil
}

func (l *Ledger) replay() error {
	f, err := os.Open(l.path)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 4<<20), 4<<20)
	var offset int64
	for sc.Scan() {
		raw := sc.Bytes()
		if len(raw) == 0 {
			offset++
			continue
		}
		var e Entry
		if err := json.Unmarshal(raw, &e); err != nil {
			return fmt.Errorf("replay line %d: %w", l.seq+1, err)
		}
		want := computeHash(l.lastHash, e.Seq, e.Table, e.Row, e.TS)
		if e.Hash != want {
			return fmt.Errorf("chain broken at seq %d", e.Seq)
		}
		l.ix.add(e.Table, e.Row, offset, e.Seq, e.TS)
		l.lastHash = e.Hash
		l.seq = e.Seq
		offset += int64(len(raw)) + 1
	}
	return sc.Err()
}

func (l *Ledger) writerLoop() {
	for job := range l.insertCh {
		e, err := l.appendEntry(job.table, job.row)
		job.reply <- insertResult{e, err}
	}
}

func (l *Ledger) appendEntry(table string, row map[string]any) (Entry, error) {
	l.seq++
	ts := time.Now().UnixMilli()
	hash := computeHash(l.lastHash, l.seq, table, row, ts)
	e := Entry{
		Seq:      l.seq,
		Table:    table,
		Row:      row,
		TS:       ts,
		PrevHash: l.lastHash,
		Hash:     hash,
	}

	line, err := json.Marshal(e)
	if err != nil {
		l.seq--
		return Entry{}, fmt.Errorf("marshal: %w", err)
	}
	if err := l.bw.Flush(); err != nil {
		l.seq--
		return Entry{}, fmt.Errorf("flush: %w", err)
	}
	offset, err := l.file.Seek(0, io.SeekCurrent)
	if err != nil {
		l.seq--
		return Entry{}, fmt.Errorf("seek: %w", err)
	}

	l.bw.Write(line)
	l.bw.WriteByte('\n')
	if err := l.bw.Flush(); err != nil {
		l.seq--
		return Entry{}, fmt.Errorf("flush after write: %w", err)
	}
	if l.doSync {
		l.file.Sync()
	}

	l.lastHash = hash
	l.ix.mu.Lock()
	l.ix.add(table, row, offset, l.seq, ts)
	l.ix.mu.Unlock()

	return e, nil
}

func (l *Ledger) Insert(table string, row map[string]any) (Entry, error) {
	reply := make(chan insertResult, 1)
	l.insertCh <- insertJob{table: table, row: row, reply: reply}
	r := <-reply
	return r.entry, r.err
}

func (l *Ledger) ReadAt(offset int64) (Entry, error) {
	// Read one line from the dedicated read fd using a small sliding buffer.
	// This avoids the 4MB-per-row allocation and races with the write fd.
	const chunk = 4096
	var line []byte
	pos := offset
	for {
		buf := make([]byte, chunk)
		n, err := l.rfile.ReadAt(buf, pos)
		if n > 0 {
			if nl := strings.IndexByte(string(buf[:n]), '\n'); nl >= 0 {
				line = append(line, buf[:nl]...)
				break
			}
			line = append(line, buf[:n]...)
			pos += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return Entry{}, err
		}
	}
	var e Entry
	return e, json.Unmarshal(line, &e)
}

func (l *Ledger) Verify() (ok bool, badAt uint64, err error) {
	f, err := os.Open(l.path)
	if err != nil {
		return false, 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 4<<20), 4<<20)
	prev := genesis
	for sc.Scan() {
		raw := sc.Bytes()
		if len(raw) == 0 {
			continue
		}
		var e Entry
		if err := json.Unmarshal(raw, &e); err != nil {
			return false, 0, err
		}
		if computeHash(prev, e.Seq, e.Table, e.Row, e.TS) != e.Hash {
			return false, e.Seq, nil
		}
		prev = e.Hash
	}
	return true, 0, sc.Err()
}

func (l *Ledger) Close() {
	l.closeOnce.Do(func() {
		close(l.insertCh)
		l.bw.Flush()
		l.file.Sync()
		l.file.Close()
		l.rfile.Close()
	})
}

// ── HTTP handlers ──────────────────────────────────────────────────────────────

type server struct{ l *Ledger }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// parseRangeParams extracts seq_gte, seq_lte, ts_gte, ts_lte from query string.
// Returns hasRange=true if any range param is present.
func parseRangeParams(q map[string][]string) (seqLo, seqHi uint64, tsLo, tsHi int64, hasRange bool) {
	seqHi = ^uint64(0)
	tsHi = 1<<63 - 1

	if v := q["seq_gte"]; len(v) > 0 {
		if n, err := strconv.ParseUint(v[0], 10, 64); err == nil {
			seqLo, hasRange = n, true
		}
	}
	if v := q["seq_lte"]; len(v) > 0 {
		if n, err := strconv.ParseUint(v[0], 10, 64); err == nil {
			seqHi, hasRange = n, true
		}
	}
	if v := q["ts_gte"]; len(v) > 0 {
		if n, err := strconv.ParseInt(v[0], 10, 64); err == nil {
			tsLo, hasRange = n, true
		}
	}
	if v := q["ts_lte"]; len(v) > 0 {
		if n, err := strconv.ParseInt(v[0], 10, 64); err == nil {
			tsHi, hasRange = n, true
		}
	}
	return
}

func (s *server) handleInsert(w http.ResponseWriter, r *http.Request, table string) {
	var row map[string]any
	if err := json.NewDecoder(r.Body).Decode(&row); err != nil {
		writeErr(w, http.StatusBadRequest, "bad JSON: "+err.Error())
		return
	}
	if len(row) == 0 {
		writeErr(w, http.StatusBadRequest, "empty row")
		return
	}
	e, err := s.l.Insert(table, row)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, e)
}

func (s *server) handleQuery(w http.ResponseWriter, r *http.Request, table string) {
	rangeParams := map[string]bool{"seq_gte": true, "seq_lte": true, "ts_gte": true, "ts_lte": true}

	// Separate range params from column filters.
	q := r.URL.Query()
	where := map[string]any{}
	for col, vals := range q {
		if !rangeParams[col] {
			where[col] = vals[0]
		}
	}

	seqLo, seqHi, tsLo, tsHi, hasRange := parseRangeParams(q)

	var offsets []int64
	if hasRange {
		// B-tree range scan, with optional hash-index column filter.
		offsets = s.l.ix.queryRange(table, seqLo, seqHi, tsLo, tsHi, where)
	} else if len(where) > 0 {
		// Pure exact-match via hash index.
		offsets = s.l.ix.query(table, where)
	} else {
		// Full table — B-tree gives ordered result cheaply.
		offsets = s.l.ix.query(table, nil)
	}

	rows := make([]map[string]any, 0, len(offsets))
	for _, off := range offsets {
		e, err := s.l.ReadAt(off)
		if err != nil {
			log.Printf("readAt %d: %v", off, err)
			continue
		}
		rows = append(rows, e.Row)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"table": table,
		"rows":  rows,
		"count": len(rows),
	})
}

func (s *server) handleMeta(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "GET required")
		return
	}
	ok, badAt, err := s.l.Verify()
	chain := map[string]any{"ok": ok}
	if err != nil {
		chain["error"] = err.Error()
	}
	if !ok {
		chain["tampered_at"] = badAt
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"healthy": true,
		"entries": s.l.seq,
		"tables":  s.l.ix.tables(),
		"chain":   chain,
	})
}

func (s *server) handleTable(w http.ResponseWriter, r *http.Request) {
	table := strings.TrimPrefix(r.URL.Path, "/")
	if table == "" {
		writeErr(w, http.StatusBadRequest, "table name required")
		return
	}
	switch r.Method {
	case http.MethodPost:
		s.handleInsert(w, r, table)
	case http.MethodGet:
		s.handleQuery(w, r, table)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "POST or GET required")
	}
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	addr    := flag.String("addr", ":7878",      "listen address")
	logPath := flag.String("log",  "append.log", "log file path")
	doSync  := flag.Bool("sync",   false,        "fsync on every insert")
	bufSize := flag.Int("buf",     256,          "insert channel buffer depth")
	flag.Parse()

	ledger, err := Open(*logPath, *bufSize, *doSync)
	if err != nil {
		log.Fatalf("open ledger: %v", err)
	}
	defer ledger.Close()

	log.Printf("replayed %d entries from %s", ledger.seq, *logPath)

	srv := &server{l: ledger}
	mux := http.NewServeMux()
	mux.HandleFunc("/__konto", srv.handleMeta)
	mux.HandleFunc("/", srv.handleTable)

	hs := &http.Server{
		Addr:         *addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("konto listening on %s (log: %s, sync: %v)", *addr, *logPath, *doSync)
		if err := hs.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hs.Shutdown(ctx)
}