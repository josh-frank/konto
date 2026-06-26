// cmd/bench — benchmark a running konto server over HTTP.
//
// Measures realistic end-to-end latency including HTTP, nginx proxy (if any),
// JSON encode/decode, index lookup, ReadAt, and response serialization.
//
// Usage:
//   # Start konto first:
//   ./konto -addr :7878
//
//   # Then run benchmarks:
//   go run ./cmd/bench
//   go run ./cmd/bench -server http://localhost:7878 -rows 50000 -iters 500
//   go run ./cmd/bench -bench insert,exact,contains
//   go run ./cmd/bench -rows 100000 -profile cpu.prof
//
// Benchmarks:
//   insert      POST /<table> throughput (serial and concurrent)
//   exact       GET /<table>?col=val latency
//   range       GET /<table>?seq_gte=N&seq_lte=M latency at varying sizes
//   contains    GET /<table>?col__contains=val latency
//   full        GET /<table> (full table scan) latency
//   verify      GET /__konto chain verification latency
//   breakdown   per-phase timing: connect, ttfb, body read, json decode

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"
)

// ── synthetic data ────────────────────────────────────────────────────────────

var (
	firstNames = []string{"Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank", "Iris", "Jack"}
	lastNames  = []string{"Smith", "Jones", "Williams", "Brown", "Taylor", "Davies", "Wilson", "Evans", "Thomas", "Roberts"}
	cities     = []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"}
	statuses   = []string{"lead", "prospect", "customer", "churned", "vip"}
	notes      = []string{
		"Called and left voicemail",
		"Sent follow-up email regarding proposal",
		"Met at conference, very interested",
		"Requested pricing information",
		"Signed contract, onboarding scheduled",
		"No response after three attempts",
		"Upgraded to enterprise plan",
		"Referred two new leads",
	}
)

func makeRow(i int) map[string]any {
	r := rand.New(rand.NewSource(int64(i * 2654435761))) // good spread
	return map[string]any{
		"first_name": firstNames[r.Intn(len(firstNames))],
		"last_name":  lastNames[r.Intn(len(lastNames))],
		"city":       cities[r.Intn(len(cities))],
		"status":     statuses[r.Intn(len(statuses))],
		"score":      r.Intn(100),
		"note":       notes[r.Intn(len(notes))],
		"email":      fmt.Sprintf("bench_user_%d@example.com", i),
	}
}

// ── HTTP client ───────────────────────────────────────────────────────────────

type client struct {
	base string
	http *http.Client
}

func newClient(base string) *client {
	return &client{
		base: strings.TrimRight(base, "/"),
		http: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        200,
				MaxIdleConnsPerHost: 200,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (c *client) insert(table string, row map[string]any) (int, error) {
	body, _ := json.Marshal(row)
	resp, err := c.http.Post(c.base+"/"+table, "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func (c *client) get(path string) ([]byte, int, error) {
	resp, err := c.http.Get(c.base + path)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return body, resp.StatusCode, err
}

func (c *client) ping() error {
	_, status, err := c.get("/__konto")
	if err != nil {
		return err
	}
	if status != 200 {
		return fmt.Errorf("unexpected status %d", status)
	}
	return nil
}

// ── stats ─────────────────────────────────────────────────────────────────────

type stats struct {
	name    string
	samples []float64
	unit    string
}

func (s *stats) record(d time.Duration) {
	s.samples = append(s.samples, float64(d.Nanoseconds()))
}

func (s *stats) recordRPS(count int, d time.Duration) {
	s.samples = append(s.samples, float64(count)/d.Seconds())
}

func (s *stats) summarise() (min, mean, p50, p95, p99, max float64) {
	if len(s.samples) == 0 {
		return
	}
	sorted := make([]float64, len(s.samples))
	copy(sorted, s.samples)
	sort.Float64s(sorted)
	n := len(sorted)
	min = sorted[0]
	max = sorted[n-1]
	p50 = sorted[int(float64(n)*0.50)]
	p95 = sorted[int(float64(n)*0.95)]
	p99 = sorted[int(float64(n)*0.99)]
	var sum float64
	for _, v := range sorted {
		sum += v
	}
	mean = sum / float64(n)
	return
}

func (s *stats) display(w *tabwriter.Writer) {
	min, mean, p50, p95, p99, max := s.summarise()
	scale := 1.0
	unit := s.unit
	if unit == "ns" {
		switch {
		case mean >= 1e9:
			scale, unit = 1e-9, "s"
		case mean >= 1e6:
			scale, unit = 1e-6, "ms"
		case mean >= 1e3:
			scale, unit = 1e-3, "µs"
		}
	}
	fmt.Fprintf(w, "  %-36s\t%-6s\t%8.2f\t%8.2f\t%8.2f\t%8.2f\t%8.2f\t%8.2f\t%5d\n",
		s.name, unit,
		min*scale, mean*scale, p50*scale, p95*scale, p99*scale, max*scale,
		len(s.samples),
	)
}

// ── seed helpers ──────────────────────────────────────────────────────────────

// seedTable inserts n rows into table, returns the highest seq inserted.
func seedTable(c *client, table string, n int) uint64 {
	fmt.Printf("  seeding %d rows into %s...", n, table)
	start := time.Now()
	var lastSeq uint64
	for i := 0; i < n; i++ {
		row := makeRow(i)
		body, _ := json.Marshal(row)
		resp, err := c.http.Post(c.base+"/"+table, "application/json", bytes.NewReader(body))
		if err != nil {
			fmt.Printf("\n  seed error at row %d: %v\n", i, err)
			return lastSeq
		}
		var result map[string]any
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if seq, ok := result["seq"].(float64); ok {
			lastSeq = uint64(seq)
		}
		if (i+1)%5000 == 0 {
			fmt.Printf(" %d", i+1)
		}
	}
	fmt.Printf(" done (%s)\n", time.Since(start).Round(time.Millisecond))
	return lastSeq
}

// getTableSeqRange queries /__konto and returns the current entry count.
func getEntryCount(c *client) uint64 {
	body, _, err := c.get("/__konto")
	if err != nil {
		return 0
	}
	var m map[string]any
	json.Unmarshal(body, &m)
	if n, ok := m["entries"].(float64); ok {
		return uint64(n)
	}
	return 0
}

// ── benchmarks ────────────────────────────────────────────────────────────────

func benchInsertSerial(c *client, table string, rows, iters int) *stats {
	s := &stats{name: "insert/serial", unit: "rows/s"}
	for i := 0; i < iters; i++ {
		start := time.Now()
		ok := 0
		for j := 0; j < rows; j++ {
			status, err := c.insert(table, makeRow(i*rows+j))
			if err == nil && (status == 201 || status == 200) {
				ok++
			}
		}
		s.recordRPS(ok, time.Since(start))
	}
	return s
}

func benchInsertConcurrent(c *client, table string, rows, iters, workers int) *stats {
	s := &stats{name: fmt.Sprintf("insert/concurrent(%dw)", workers), unit: "rows/s"}
	for i := 0; i < iters; i++ {
		var wg sync.WaitGroup
		var count atomic.Int64
		perWorker := rows / workers
		start := time.Now()
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(wid, iter int) {
				defer wg.Done()
				base := iter*rows + wid*perWorker
				for j := 0; j < perWorker; j++ {
					status, err := c.insert(table, makeRow(base+j))
					if err == nil && (status == 201 || status == 200) {
						count.Add(1)
					}
				}
			}(w, i)
		}
		wg.Wait()
		s.recordRPS(int(count.Load()), time.Since(start))
	}
	return s
}

func benchExactMatch(c *client, table string, iters int) *stats {
	s := &stats{name: "query/exact-match", unit: "ns"}
	r := rand.New(rand.NewSource(42))
	for i := 0; i < iters; i++ {
		city := cities[r.Intn(len(cities))]
		start := time.Now()
		c.get(fmt.Sprintf("/%s?city=%s", table, strings.ReplaceAll(city, " ", "+")))
		s.record(time.Since(start))
	}
	return s
}

func benchRange(c *client, table string, iters int, maxSeq, rangeSize uint64) *stats {
	s := &stats{name: fmt.Sprintf("query/range(n=%d)", rangeSize), unit: "ns"}
	r := rand.New(rand.NewSource(42))
	if maxSeq < rangeSize {
		maxSeq = rangeSize
	}
	for i := 0; i < iters; i++ {
		lo := uint64(r.Intn(int(maxSeq-rangeSize))) + 1
		hi := lo + rangeSize - 1
		start := time.Now()
		c.get(fmt.Sprintf("/%s?seq_gte=%d&seq_lte=%d", table, lo, hi))
		s.record(time.Since(start))
	}
	return s
}

func benchContains(c *client, table string, iters int) *stats {
	s := &stats{name: "query/contains", unit: "ns"}
	r := rand.New(rand.NewSource(42))
	terms := []string{"Smith", "New York", "customer", "voicemail", "conference", "Jones"}
	cols  := []string{"last_name", "city", "status", "note"}
	for i := 0; i < iters; i++ {
		term := terms[r.Intn(len(terms))]
		col  := cols[r.Intn(len(cols))]
		start := time.Now()
		c.get(fmt.Sprintf("/%s?%s__contains=%s", table, col, strings.ReplaceAll(term, " ", "+")))
		s.record(time.Since(start))
	}
	return s
}

func benchFullScan(c *client, table string, iters int) *stats {
	s := &stats{name: "query/full-scan", unit: "ns"}
	for i := 0; i < iters; i++ {
		start := time.Now()
		c.get("/" + table)
		s.record(time.Since(start))
	}
	return s
}

func benchVerify(c *client, iters int) *stats {
	s := &stats{name: "verify (/__konto)", unit: "ns"}
	for i := 0; i < iters; i++ {
		start := time.Now()
		c.get("/__konto")
		s.record(time.Since(start))
	}
	return s
}

// benchBreakdown times the three phases of a GET request separately:
// DNS+connect, time-to-first-byte, body read + JSON decode.
func benchBreakdown(c *client, table string, iters int) (*stats, *stats, *stats) {
	connect   := &stats{name: "  breakdown/http-connect", unit: "ns"}
	ttfb      := &stats{name: "  breakdown/time-to-first-byte", unit: "ns"}
	bodydec   := &stats{name: "  breakdown/body-read+json-decode", unit: "ns"}

	r := rand.New(rand.NewSource(99))
	for i := 0; i < iters; i++ {
		city := cities[r.Intn(len(cities))]
		url  := c.base + fmt.Sprintf("/%s?city=%s", table, strings.ReplaceAll(city, " ", "+"))

		t0 := time.Now()
		req, _ := http.NewRequest("GET", url, nil)
		resp, err := c.http.Do(req)
		if err != nil {
			continue
		}
		t1 := time.Now()
		connect.record(t1.Sub(t0))

		// Read first byte.
		oneByte := make([]byte, 1)
		resp.Body.Read(oneByte)
		t2 := time.Now()
		ttfb.record(t2.Sub(t1))

		// Read rest + decode.
		rest, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		full := append(oneByte, rest...)
		var m map[string]any
		json.Unmarshal(full, &m)
		t3 := time.Now()
		bodydec.record(t3.Sub(t2))
	}
	return connect, ttfb, bodydec
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	server  := flag.String("server",  "http://localhost:7878", "konto server URL")
	rows    := flag.Int("rows",    10_000, "rows to seed per benchmark table")
	iters   := flag.Int("iters",   200,    "query iterations per benchmark")
	profile := flag.String("profile", "",  "write CPU profile to this path")
	bench   := flag.String("bench",  "all","comma-separated: insert,exact,range,contains,full,verify,breakdown")
	table   := flag.String("table",  "",   "reuse existing table instead of seeding (skips insert bench)")
	flag.Parse()

	c := newClient(*server)

	// Ping.
	if err := c.ping(); err != nil {
		fmt.Fprintf(os.Stderr, "✗ cannot reach konto at %s: %v\n", *server, err)
		fmt.Fprintf(os.Stderr, "  Start konto first: ./konto -addr :7878\n")
		os.Exit(1)
	}
	fmt.Printf("✓ connected to %s\n\n", *server)

	// CPU profile.
	if *profile != "" {
		f, err := os.Create(*profile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot create profile: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		fmt.Printf("CPU profile → %s\n", *profile)
		fmt.Printf("Inspect:      go tool pprof -http=:8080 %s\n\n", *profile)
	}

	want := map[string]bool{}
	for _, b := range strings.Split(*bench, ",") {
		want[strings.TrimSpace(b)] = true
	}
	all := want["all"]
	run := func(name string) bool { return all || want[name] }

	// Determine benchmark table.
	benchTable := *table
	maxSeq := uint64(0)

	if benchTable == "" {
		benchTable = fmt.Sprintf("bench_%d", time.Now().Unix())
	}

	// Print header.
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "  %-36s\t%-6s\t%8s\t%8s\t%8s\t%8s\t%8s\t%8s\t%5s\n",
		"benchmark", "unit", "min", "mean", "p50", "p95", "p99", "max", "n")
	sep := "  " + strings.Repeat("─", 102)

	// Insert benchmarks (also seeds the table).
	if run("insert") {
		fmt.Fprintln(w, sep)
		fmt.Fprintf(w, "  %-36s\t(throughput)\n", "── INSERT")
		// Small batch for timing accuracy.
		seedRows := min(*rows/10, 500)
		benchInsertSerial(c, benchTable, seedRows, min(*iters/10, 5)).display(w)
		benchInsertConcurrent(c, benchTable, seedRows, min(*iters/10, 5), 4).display(w)
		benchInsertConcurrent(c, benchTable, seedRows, min(*iters/10, 5), 8).display(w)
		w.Flush()
	}

	// Seed the table if not already done / provided.
	if *table == "" {
		current := getEntryCount(c)
		if current < uint64(*rows) {
			fmt.Println()
			maxSeq = seedTable(c, benchTable, *rows-int(current))
		} else {
			maxSeq = current
		}
		fmt.Println()
	} else {
		maxSeq = getEntryCount(c)
	}

	if run("exact") {
		fmt.Fprintln(w, sep)
		fmt.Fprintf(w, "  %-36s\t(hash index)\n", "── EXACT MATCH")
		benchExactMatch(c, benchTable, *iters).display(w)
	}

	if run("range") {
		fmt.Fprintln(w, sep)
		fmt.Fprintf(w, "  %-36s\t(B-tree)\n", "── RANGE SCAN")
		for _, size := range []uint64{10, 100, 1000} {
			if maxSeq > size {
				benchRange(c, benchTable, *iters, maxSeq, size).display(w)
			}
		}
	}

	if run("contains") {
		fmt.Fprintln(w, sep)
		fmt.Fprintf(w, "  %-36s\t(trigram index)\n", "── CONTAINS")
		benchContains(c, benchTable, *iters).display(w)
	}

	if run("full") {
		fmt.Fprintln(w, sep)
		fmt.Fprintf(w, "  %-36s\t(B-tree full scan)\n", "── FULL SCAN")
		benchFullScan(c, benchTable, min(*iters, 20)).display(w)
	}

	if run("verify") {
		fmt.Fprintln(w, sep)
		fmt.Fprintf(w, "  %-36s\t(chain walk)\n", "── VERIFY")
		benchVerify(c, min(*iters, 10)).display(w)
	}

	if run("breakdown") {
		fmt.Fprintln(w, sep)
		fmt.Fprintf(w, "  %-36s\t(per-phase)\n", "── REQUEST BREAKDOWN")
		conn, ttfb, body := benchBreakdown(c, benchTable, *iters)
		conn.display(w)
		ttfb.display(w)
		body.display(w)
	}

	fmt.Fprintln(w, sep)
	w.Flush()

	fmt.Printf("\nTable used: %s  (entries: %d)\n", benchTable, getEntryCount(c))
	fmt.Println("\nTips:")
	fmt.Println("  Reuse this table next run:  -table " + benchTable)
	fmt.Println("  CPU flame graph:            -profile=cpu.prof  →  go tool pprof -http=:8080 cpu.prof")
	fmt.Println("  Scale up:                   -rows=100000 -iters=500")
	fmt.Println("  Single benchmark:           -bench=contains")

	if *profile != "" {
		// Brief pause to let pprof finish writing.
		time.Sleep(100 * time.Millisecond)
		fmt.Printf("\nOpening pprof in browser...\n")
		exec.Command("go", "tool", "pprof", "-http=:8080", *profile).Start()
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
