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
 * by GET /__konto. No Merkle tree needed: for logs you can replay, hash
 * chaining gives the same tamper-evidence guarantee with far less complexity.
 *
 * Index: rebuilt in memory at startup by replaying the log.
 *   map[table]map[column]map[value][]offset
 * Allows O(1) exact-match lookups on any column with no schema declaration.
 * The log file is always the source of truth; the index is a read cache.
 *
 * Each table has exactly one mandatory primary key column ("id" by default,
 * configurable per-table). Inserts with a duplicate PK are rejected before
 * touching the log.
 *
 * API (HTTP/JSON, nginx-ready):
 *   POST /<table>          {"id":"1","name":"Alice",...}   → insert row (id mandatory)
 *   GET  /<table>          → all rows
 *   GET  /<table>?col=val  → exact-match filter on any column
 *   GET  /__konto          → health, chain verify, table list, entry count
 *
 * Threading:
 *   All writes funnel through one writer goroutine via a buffered channel.
 *   Index reads use sync.RWMutex. Verify opens a second fd independently.
 *
 * Usage:
 *   go run konto.go [-addr :7878] [-log append.log] [-sync] [-buf 256] [-pk id]
 *   go build -o konto konto.go
 *
 * Zero external dependencies — stdlib only.
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

// ── index ─────────────────────────────────────────────────────────────────────

type idx struct {
	mu   sync.RWMutex
	data map[string]map[string]map[string][]int64 // table→col→val→offsets
}

func newIdx() *idx {
	return &idx{data: make(map[string]map[string]map[string][]int64)}
}

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

func (ix *idx) query(table string, where map[string]any) []int64 {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	t, ok := ix.data[table]
	if !ok {
		return nil
	}

	if len(where) == 0 {
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

// pkExists checks whether a given PK value is already present in a table.
// Called under read lock — or before any goroutines start during replay.
func (ix *idx) pkExists(table, pkCol, pkVal string) bool {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	t, ok := ix.data[table]
	if !ok {
		return false
	}
	colMap, ok := t[pkCol]
	if !ok {
		return false
	}
	_, exists := colMap[pkVal]
	return exists
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

// ErrDuplicatePK is returned when an insert violates the primary key constraint.
type ErrDuplicatePK struct {
	Table string
	Col   string
	Val   string
}

func (e *ErrDuplicatePK) Error() string {
	return fmt.Sprintf("duplicate primary key: %s.%s = %q", e.Table, e.Col, e.Val)
}

// Ledger owns the log file, the index, and the insert channel.
// All public methods are safe for concurrent use.
type Ledger struct {
	closeOnce sync.Once
	path      string
	doSync    bool
	pkCol     string // primary key column name, mandatory for every table
	file      *os.File
	bw        *bufio.Writer
	ix        *idx
	insertCh  chan insertJob
	lastHash  string
	seq       uint64
}

const genesis = "0000000000000000000000000000000000000000000000000000000000000000"

// Open opens (or creates) the log at path, replays it to rebuild the index,
// then starts the writer goroutine. pkCol is the mandatory primary key column.
func Open(path string, chanBuf int, doSync bool, pkCol string) (*Ledger, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	l := &Ledger{
		path:     path,
		doSync:   doSync,
		pkCol:    pkCol,
		file:     f,
		bw:       bufio.NewWriterSize(f, 64*1024),
		ix:       newIdx(),
		insertCh: make(chan insertJob, chanBuf),
		lastHash: genesis,
	}
	if err := l.replay(); err != nil {
		f.Close()
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
		l.ix.add(e.Table, e.Row, offset)
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

// appendEntry is only called from writerLoop — never concurrent.
func (l *Ledger) appendEntry(table string, row map[string]any) (Entry, error) {
	// PK check: must be present and must be unique.
	pkVal, ok := row[l.pkCol]
	if !ok {
		return Entry{}, fmt.Errorf("missing primary key column %q", l.pkCol)
	}
	pkStr := fmt.Sprintf("%v", pkVal)
	if l.ix.pkExists(table, l.pkCol, pkStr) {
		return Entry{}, &ErrDuplicatePK{Table: table, Col: l.pkCol, Val: pkStr}
	}

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
	l.ix.add(table, row, offset)
	l.ix.mu.Unlock()

	return e, nil
}

// Insert sends a job to the writer goroutine and waits for the result.
func (l *Ledger) Insert(table string, row map[string]any) (Entry, error) {
	reply := make(chan insertResult, 1)
	l.insertCh <- insertJob{table: table, row: row, reply: reply}
	r := <-reply
	return r.entry, r.err
}

// ReadAt reads the Entry at the given log offset (pread, concurrency-safe).
func (l *Ledger) ReadAt(offset int64) (Entry, error) {
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

// Verify walks the full chain on a fresh fd. Does not block inserts or queries.
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

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// POST /<table>  — insert a row. Body is the row JSON object directly.
// The primary key column must be present in the body.
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
		if _, isDup := err.(*ErrDuplicatePK); isDup {
			writeErr(w, http.StatusConflict, err.Error())
			return
		}
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, e)
}

// GET /<table>[?col=val&col2=val2]  — query rows, optionally filtered.
func (s *server) handleQuery(w http.ResponseWriter, r *http.Request, table string) {
	where := map[string]any{}
	for col, vals := range r.URL.Query() {
		where[col] = vals[0]
	}

	offsets := s.l.ix.query(table, where)
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

// GET /__konto  — health, verify, table list, entry count.
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
		"healthy":    true,
		"pk_col":     s.l.pkCol,
		"entries":    s.l.seq,
		"tables":     s.l.ix.tables(),
		"chain":      chain,
	})
}

// handleTable dispatches POST (insert) and GET (query) for /<table>.
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
	pkCol   := flag.String("pk",   "id",         "primary key column name")
	flag.Parse()

	ledger, err := Open(*logPath, *bufSize, *doSync, *pkCol)
	if err != nil {
		log.Fatalf("open ledger: %v", err)
	}
	defer ledger.Close()

	log.Printf("replayed %d entries from %s (pk: %s)", ledger.seq, *logPath, *pkCol)

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
		log.Printf("konto listening on %s (log: %s, sync: %v, pk: %s)", *addr, *logPath, *doSync, *pkCol)
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