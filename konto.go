/*
 * konto — tamper-evident append-only database in one Go file
 *
 * Storage: a single newline-delimited JSON log (append.log):
 *   {"seq":1,"table":"users","row":{"id":"1","name":"Alice"},
 *    "ts":1719000000000,"prev":"0000...","hash":"a3f9..."}
 *
 * Every row's hash is SHA-256 of:
 *   prev_hash + seq + table + canonical_row_json + timestamp
 * forming an unbroken chain. Any modification, deletion, or insertion
 * into past history breaks every subsequent hash — detectable instantly
 * by GET /verify. No Merkle tree needed: for logs you can replay, hash
 * chaining gives the same tamper-evidence guarantee with far less complexity.
 *
 * Index: rebuilt in memory at startup by replaying the log.
 *   map[table]map[column]map[value][]offset
 * Allows O(1) exact-match lookups on any column with no schema declaration.
 * The log file is always the source of truth; the index is a read cache.
 *
 * API (HTTP/JSON, nginx-ready, no custom protocol):
 *   POST /insert   {"table":"users","row":{"id":"1","name":"Alice",...}}
 *   POST /query    {"table":"users","where":{"name":"Alice"}}  (where optional)
 *   GET  /verify   walks the full chain → {"ok":true} or {"ok":false,"tampered_at":N}
 *   GET  /tables   → {"tables":["users","orders",...]}
 *
 * Threading:
 *   net/http manages a goroutine per request — no pthread_create loops.
 *   All writes funnel through one writer goroutine via a buffered channel;
 *   hash chaining requires strict ordering, so this replaces the C version's
 *   80-line mutex/condvar/ring-buffer with an 8-line channel pattern.
 *   Index reads use sync.RWMutex — concurrent reads never block each other.
 *   Verify opens a second file descriptor and reads independently, so it
 *   never blocks inserts or queries.
 *
 * No artificial limits:
 *   Rows can have any number of columns, any column name, any value length.
 *   The index grows with your data. Query results are real Go slices.
 *   The only real limit is RAM for the index — the same bet every production
 *   database makes (Redis, MySQL's buffer pool, Postgres shared_buffers).
 *
 * Usage:
 *   go run konto.go [-addr :7878] [-log append.log] [-sync] [-buf 256]
 *   go build -o konto konto.go
 *
 * Talk to it:
 *   curl -s -X POST localhost:7878/insert \
 *        -d '{"table":"users","row":{"id":"1","name":"Alice"}}'
 *   curl -s -X POST localhost:7878/query \
 *        -d '{"table":"users","where":{"name":"Alice"}}'
 *   curl -s localhost:7878/verify
 *
 * nginx config (TLS termination + rate limiting, konto handles none of it):
 *   upstream konto { server 127.0.0.1:7878; keepalive 32; }
 *   server {
 *     listen 443 ssl;
 *     location /db/ {
 *       proxy_pass         http://konto/;
 *       proxy_http_version 1.1;
 *       proxy_set_header   Connection "";
 *       proxy_read_timeout 10s;
 *       limit_req          zone=api burst=50 nodelay;
 *     }
 *   }
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

// Entry is one row in the log. Every field is included in the hash so nothing
// can be changed after the fact without detection.
type Entry struct {
	Seq      uint64         `json:"seq"`
	Table    string         `json:"table"`
	Row      map[string]any `json:"row"`
	TS       int64          `json:"ts"`   // Unix milliseconds
	PrevHash string         `json:"prev"` // hash of the previous entry
	Hash     string         `json:"hash"` // SHA-256 of this entry's content
}

// ── hashing ───────────────────────────────────────────────────────────────────

// computeHash returns the SHA-256 of the entry's content fields.
// Input is deterministic: prev+seq+table+canonicalJSON(row)+ts.
// canonicalJSON sorts keys so {"b":1,"a":2} and {"a":2,"b":1} hash identically,
// which matters because json.Marshal does not guarantee key order for map[string]any.
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

// ── index ─────────────────────────────────────────────────────────────────────

// idx maps table → column → value → sorted slice of byte offsets in the log file.
// Offsets point to the start of each JSON line. We store []int64 so a non-unique
// column (e.g. "city") can match multiple rows.
type idx struct {
	mu   sync.RWMutex
	data map[string]map[string]map[string][]int64 // table→col→val→offsets
}

func newIdx() *idx {
	return &idx{data: make(map[string]map[string]map[string][]int64)}
}

// add indexes every column of the row. Called under write lock (or at startup
// before any goroutines are running, so locking is not needed then either).
func (ix *idx) add(table string, row map[string]any, offset int64) {
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
}

// query returns the sorted offsets of all rows in table matching every
// key=value pair in where. Empty where → all rows in the table.
// Intersection is computed cheaply: start with the smallest matching set,
// then filter against each additional condition.
func (ix *idx) query(table string, where map[string]any) []int64 {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	t, ok := ix.data[table]
	if !ok {
		return nil
	}

	if len(where) == 0 {
		// Collect all offsets across every column, deduplicate.
		seen := make(map[int64]struct{})
		for _, colMap := range t {
			for _, offsets := range colMap {
				for _, off := range offsets {
					seen[off] = struct{}{}
				}
			}
		}
		result := make([]int64, 0, len(seen))
		for off := range seen {
			result = append(result, off)
		}
		sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
		return result
	}

	// Start with the first condition's matches, then intersect.
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

// Ledger owns the log file, the index, and the insert channel.
// All public methods are safe for concurrent use.
type Ledger struct {
	closeOnce sync.Once
	path     string
	doSync   bool
	file     *os.File
	bw       *bufio.Writer  // wraps file; flushed after every insert
	ix       *idx
	insertCh chan insertJob
	lastHash string
	seq      uint64
}

const genesis = "0000000000000000000000000000000000000000000000000000000000000000"

// Open opens (or creates) the log at path, replays it to rebuild the index
// and verify the chain, then starts the writer goroutine.
func Open(path string, chanBuf int, doSync bool) (*Ledger, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
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
	if err := l.replay(); err != nil {
		f.Close()
		return nil, fmt.Errorf("replay: %w", err)
	}
	go l.writerLoop()
	return l, nil
}

// replay reads the log from byte 0, rebuilds ix, and verifies the chain.
// Called once before the writer goroutine starts, so no locking needed.
func (l *Ledger) replay() error {
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	sc := bufio.NewScanner(l.file)
	sc.Buffer(make([]byte, 4<<20), 4<<20) // 4 MB max line
	var offset int64
	for sc.Scan() {
		raw := sc.Bytes()
		if len(raw) == 0 {
			offset++
			continue
		}
		var e Entry
		if err := json.Unmarshal(raw, &e); err != nil {
			return fmt.Errorf("seq %d: bad JSON: %w", l.seq+1, err)
		}
		want := computeHash(l.lastHash, e.Seq, e.Table, e.Row, e.TS)
		if want != e.Hash {
			return fmt.Errorf("chain broken at seq %d", e.Seq)
		}
		l.ix.add(e.Table, e.Row, offset)
		l.lastHash = e.Hash
		l.seq = e.Seq
		offset += int64(len(raw)) + 1 // +1 for the newline
	}
	return sc.Err()
}

// writerLoop is the one goroutine that appends to the log.
// Runs until insertCh is closed (by Close).
func (l *Ledger) writerLoop() {
	for job := range l.insertCh {
		e, err := l.appendEntry(job.table, job.row)
		job.reply <- insertResult{e, err}
	}
}

// appendEntry writes one entry, updates the index, and advances the chain.
// Only called from writerLoop — never concurrent, never needs a file mutex.
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

	// Flush the buffer before noting the offset so we know exactly where
	// this entry will land in the file.
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

	// Update index under write lock.
	l.ix.mu.Lock()
	l.ix.add(table, row, offset)
	l.ix.mu.Unlock()

	return e, nil
}

// Insert sends an insert job to the writer goroutine and waits for the result.
// Safe to call from multiple goroutines concurrently.
func (l *Ledger) Insert(table string, row map[string]any) (Entry, error) {
	reply := make(chan insertResult, 1)
	l.insertCh <- insertJob{table: table, row: row, reply: reply}
	r := <-reply
	return r.entry, r.err
}

// ReadAt reads the Entry that starts at the given log offset.
// Uses ReadAt (pread) so it is safe to call concurrently with appends.
func (l *Ledger) ReadAt(offset int64) (Entry, error) {
	// Read enough for any realistic row; 4MB matches the replay scanner limit.
	buf := make([]byte, 4<<20)
	n, err := l.file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return Entry{}, err
	}
	if nl := strings.IndexByte(string(buf[:n]), '\n'); nl >= 0 {
		buf = buf[:nl]
	} else {
		buf = buf[:n]
	}
	var e Entry
	return e, json.Unmarshal(buf, &e)
}

// Verify opens a fresh fd and walks the entire chain. Returns (true, 0, nil)
// if intact, or (false, N, nil) where N is the sequence number of the first
// broken link. Does not block inserts or queries.
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

// Close drains the writer goroutine, flushes, and syncs.
// Safe to call more than once; subsequent calls are no-ops.
func (l *Ledger) Close() {
	l.closeOnce.Do(func() {
		close(l.insertCh)
		l.bw.Flush()
		l.file.Sync()
		l.file.Close()
	})
}

// ── HTTP handlers ──────────────────────────────────────────────────────────────

type server struct{ l *Ledger }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (s *server) insert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Table string         `json:"table"`
		Row   map[string]any `json:"row"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Table == "" || len(req.Row) == 0 {
		http.Error(w, `{"error":"table and row required"}`, http.StatusBadRequest)
		return
	}
	e, err := s.l.Insert(req.Table, req.Row)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, e)
}

func (s *server) query(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Table string         `json:"table"`
		Where map[string]any `json:"where"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Table == "" {
		http.Error(w, `{"error":"table required"}`, http.StatusBadRequest)
		return
	}

	offsets := s.l.ix.query(req.Table, req.Where)
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
		"rows":  rows,
		"count": len(rows),
	})
}

func (s *server) verify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET required", http.StatusMethodNotAllowed)
		return
	}
	ok, badAt, err := s.l.Verify()
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	if ok {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "seq": s.l.seq})
	} else {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "tampered_at": badAt})
	}
}

func (s *server) tables(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET required", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tables": s.l.ix.tables()})
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	addr    := flag.String("addr", ":7878",      "listen address")
	logPath := flag.String("log",  "append.log", "log file path")
	doSync  := flag.Bool("sync",   false,        "fsync on every insert (durable but slower)")
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
	mux.HandleFunc("/insert", srv.insert)
	mux.HandleFunc("/query",  srv.query)
	mux.HandleFunc("/verify", srv.verify)
	mux.HandleFunc("/tables", srv.tables)

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
