// cmd/import — bulk-load records into a running konto server.
//
// Usage:
//   go run ./cmd/import -server http://localhost:7878 -table users data.csv
//   go run ./cmd/import -server http://localhost:7878 -table orders data.csv
//
// CSV: first row is headers; each header becomes a column name.
// The primary key column (default "id") must be present in the CSV.
//
// Adding a new format:
//  1. Implement the Importer interface below.
//  2. Register it in the importers map in main().
//  3. The -format flag or file extension auto-detection will pick it up.

package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ── importer interface ────────────────────────────────────────────────────────

// Importer reads rows from r and sends each to out.
// Each row is a map[string]any ready to POST to konto.
// Close out when done; send an error to errc on failure.
type Importer interface {
	Import(r io.Reader, out chan<- map[string]any, errc chan<- error)
}

// ── CSV importer ──────────────────────────────────────────────────────────────

type csvImporter struct{}

func (csvImporter) Import(r io.Reader, out chan<- map[string]any, errc chan<- error) {
	defer close(out)
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true

	headers, err := cr.Read()
	if err != nil {
		errc <- fmt.Errorf("csv: read headers: %w", err)
		return
	}

	for {
		record, err := cr.Read()
		if err == io.EOF {
			return
		}
		if err != nil {
			errc <- fmt.Errorf("csv: read row: %w", err)
			return
		}
		row := make(map[string]any, len(headers))
		for i, h := range headers {
			if i < len(record) {
				row[h] = record[i]
			}
		}
		out <- row
	}
}

// ── placeholder: JSON-lines importer ─────────────────────────────────────────
// Each line must be a JSON object {"col":"val",...}.
// Uncomment and complete to enable.

// type jsonlImporter struct{}
//
// func (jsonlImporter) Import(r io.Reader, out chan<- map[string]any, errc chan<- error) {
// 	defer close(out)
// 	sc := bufio.NewScanner(r)
// 	for sc.Scan() {
// 		var row map[string]any
// 		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
// 			errc <- fmt.Errorf("jsonl: %w", err)
// 			return
// 		}
// 		out <- row
// 	}
// 	if err := sc.Err(); err != nil {
// 		errc <- err
// 	}
// }

// ── placeholder: TSV importer ─────────────────────────────────────────────────
// Like CSV but tab-delimited.

// type tsvImporter struct{}
//
// func (tsvImporter) Import(r io.Reader, out chan<- map[string]any, errc chan<- error) {
// 	// Same as csvImporter but set cr.Comma = '\t'
// }

// ── placeholder: Parquet / Arrow / Excel ─────────────────────────────────────
// These require external dependencies. Sketch:
//
// type parquetImporter struct{}
// func (parquetImporter) Import(r io.Reader, out chan<- map[string]any, errc chan<- error) {
// 	// use github.com/parquet-go/parquet-go
// }

// ── registry ──────────────────────────────────────────────────────────────────

var importers = map[string]Importer{
	"csv": csvImporter{},
	// "jsonl":   jsonlImporter{},
	// "tsv":     tsvImporter{},
	// "parquet": parquetImporter{},
}

func detectFormat(path string) string {
	ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(path)), ".")
	if _, ok := importers[ext]; ok {
		return ext
	}
	return ""
}

// ── konto client ─────────────────────────────────────────────────────────────

type kontoClient struct {
	server string
	table  string
	http   *http.Client
}

func newClient(server, table string) *kontoClient {
	return &kontoClient{
		server: strings.TrimRight(server, "/"),
		table:  table,
		http:   &http.Client{Timeout: 15 * time.Second},
	}
}

func (c *kontoClient) insert(row map[string]any) error {
	body, err := json.Marshal(row)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s/%s", c.server, c.table)
	resp, err := c.http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusCreated {
		return nil
	}
	var e struct{ Error string `json:"error"` }
	json.NewDecoder(resp.Body).Decode(&e)
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, e.Error)
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	server := flag.String("server", "http://localhost:7878", "konto server URL")
	table  := flag.String("table", "", "target table name (required)")
	format := flag.String("format", "", "import format: csv (default: auto-detect from extension)")
	dryRun := flag.Bool("dry-run", false, "parse and validate without inserting")
	flag.Parse()

	if *table == "" {
		log.Fatal("-table is required")
	}

	args := flag.Args()
	if len(args) == 0 {
		log.Fatal("provide a file path as argument")
	}
	path := args[0]

	// Resolve importer.
	fmt := *format
	if fmt == "" {
		fmt = detectFormat(path)
	}
	if fmt == "" {
		log.Fatalf("could not detect format for %q; use -format csv", path)
	}
	imp, ok := importers[fmt]
	if !ok {
		log.Fatalf("unknown format %q; supported: %s", fmt, supportedFormats())
	}

	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	client := newClient(*server, *table)
	out  := make(chan map[string]any, 64)
	errc := make(chan error, 1)

	go imp.Import(f, out, errc)

	var inserted, skipped, total int
	start := time.Now()

	for row := range out {
		total++
		if *dryRun {
			inserted++
			continue
		}
		if err := client.insert(row); err != nil {
			log.Printf("row %d skipped: %v", total, err)
			skipped++
			continue
		}
		inserted++
		if inserted%100 == 0 {
			log.Printf("  %d inserted...", inserted)
		}
	}

	if err := <-errc; err != nil {
		log.Printf("import error: %v", err)
	}

	elapsed := time.Since(start)
	rps := float64(inserted) / elapsed.Seconds()
	if *dryRun {
		log.Printf("dry-run: parsed %d rows in %s", total, elapsed.Round(time.Millisecond))
	} else {
		log.Printf("done: %d inserted, %d skipped in %s (%.0f rows/s)",
			inserted, skipped, elapsed.Round(time.Millisecond), rps)
	}
}

func supportedFormats() string {
	out := make([]string, 0, len(importers))
	for k := range importers {
		out = append(out, k)
	}
	return strings.Join(out, ", ")
}