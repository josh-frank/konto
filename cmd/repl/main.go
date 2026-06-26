// SQL-ish REPL for konto
//
// Supported statements:
//   SHOW TABLES
//   VERIFY
//   SELECT * FROM <table> [WHERE col = 'val' [AND col2 = val2 ...]]
//   INSERT INTO <table> (col1, col2, ...) VALUES ('v1', v2, ...)
//   HELP
//   EXIT / QUIT / \q
//
// Zero external dependencies — stdlib only.

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"unicode"
)

// ── colours ───────────────────────────────────────────────────────────────────

var (
	bold   = attr("1")
	green  = attr("32")
	red    = attr("31")
	cyan   = attr("36")
	yellow = attr("33")
	dim    = attr("2")
	reset  = "\033[0m"
)

func attr(code string) func(string) string {
	return func(s string) string {
		return "\033[" + code + "m" + s + reset
	}
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

var baseURL string

func apiGet(path string) (map[string]any, error) {
	resp, err := http.Get(baseURL + path)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return decodeMap(resp.Body)
}

func apiPost(path string, body any) (map[string]any, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(baseURL+path, "application/json", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return decodeMap(resp.Body)
}

func decodeMap(r io.Reader) (map[string]any, error) {
	var m map[string]any
	return m, json.NewDecoder(r).Decode(&m)
}

// ── SQL parser ────────────────────────────────────────────────────────────────

type stmt struct {
	kind  string         // SHOW_TABLES | VERIFY | SELECT | INSERT | HELP | EXIT
	table string
	cols  []string
	vals  []any
	where    map[string]any
	contains map[string]string // col → substr for CONTAINS queries
	limit    int // 0 = no limit
	page     int // 1-based, requires limit
}

var (
	reInsert = regexp.MustCompile(`(?i)^INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s+VALUES\s*\((.+)\)\s*;?$`)
	reSelect = regexp.MustCompile(`(?i)^SELECT\s+\*\s+FROM\s+(\w+)(\s+WHERE\s+(.+?))?\s*;?$`)
	reLimitPage = regexp.MustCompile(`(?i)\s+LIMIT\s+(\d+)(?:\s+PAGE\s+(\d+))?`)
	reContains = regexp.MustCompile(`(?i)^(\w+)\s+CONTAINS\s+(.+)$`)
)

func parse(raw string) (stmt, error) {
	s := strings.TrimRight(strings.TrimSpace(raw), ";")
	up := strings.ToUpper(s)

	switch up {
	case "SHOW TABLES":
		return stmt{kind: "SHOW_TABLES"}, nil
	case "VERIFY":
		return stmt{kind: "VERIFY"}, nil
	case "HELP", "\\H":
		return stmt{kind: "HELP"}, nil
	case "EXIT", "QUIT", "\\Q":
		return stmt{kind: "EXIT"}, nil
	}

	if m := reInsert.FindStringSubmatch(s); m != nil {
		cols := splitComma(m[2])
		rawVals := splitValues(m[3])
		if len(cols) != len(rawVals) {
			return stmt{}, fmt.Errorf("column count (%d) != value count (%d)", len(cols), len(rawVals))
		}
		vals := make([]any, len(rawVals))
		for i, v := range rawVals {
			vals[i] = coerce(v)
		}
		return stmt{kind: "INSERT", table: m[1], cols: cols, vals: vals}, nil
	}

	if m := reSelect.FindStringSubmatch(s); m != nil {
		where := map[string]any{}
		if whereStr := strings.TrimSpace(m[3]); whereStr != "" {
			contains := map[string]string{}
			parts := regexp.MustCompile(`(?i)\s+AND\s+`).Split(whereStr, -1)
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if cm := reContains.FindStringSubmatch(p); cm != nil {
					contains[cm[1]] = strings.Trim(strings.TrimSpace(cm[2]), "'\"")
					continue
				}
				kv := regexp.MustCompile(`\s*=\s*`).Split(p, 2)
				if len(kv) != 2 {
					return stmt{}, fmt.Errorf("bad WHERE clause: %s", p)
				}
				where[strings.TrimSpace(kv[0])] = coerce(strings.TrimSpace(kv[1]))
			}
			return stmt{kind: "SELECT", table: m[1], where: where, contains: contains}, nil
		}
		return stmt{kind: "SELECT", table: m[1], where: where}, nil
	}

	return stmt{}, fmt.Errorf("unrecognised statement — type HELP for syntax")
}

// splitComma splits on commas, trimming whitespace.
func splitComma(s string) []string {
	parts := strings.Split(s, ",")
	for i, p := range parts {
		parts[i] = strings.TrimSpace(p)
	}
	return parts
}

// splitValues handles quoted strings containing commas.
func splitValues(s string) []string {
	var out []string
	var cur strings.Builder
	inQ := false
	var qch rune
	for _, c := range s {
		switch {
		case inQ && c == qch:
			inQ = false
		case !inQ && (c == '\'' || c == '"'):
			inQ = true
			qch = c
		case !inQ && c == ',':
			out = append(out, strings.TrimSpace(cur.String()))
			cur.Reset()
			continue
		default:
			cur.WriteRune(c)
		}
	}
	out = append(out, strings.TrimSpace(cur.String()))
	return out
}

// coerce turns SQL literals into Go values.
func coerce(v string) any {
	if (strings.HasPrefix(v, "'") && strings.HasSuffix(v, "'")) ||
		(strings.HasPrefix(v, `"`) && strings.HasSuffix(v, `"`)) {
		return v[1 : len(v)-1]
	}
	if n, err := strconv.ParseFloat(v, 64); err == nil {
		return n
	}
	switch strings.ToLower(v) {
	case "true":
		return true
	case "false":
		return false
	case "null", "nil":
		return nil
	}
	return v
}

// ── execution ─────────────────────────────────────────────────────────────────

func exec(st stmt) {
	switch st.kind {

	case "SHOW_TABLES":
		m, err := apiGet("/__konto")
		if err != nil {
			errln(err)
			return
		}
		tables, _ := m["tables"].([]any)
		if len(tables) == 0 {
			fmt.Println(dim("(no tables)"))
			return
		}
		for _, t := range tables {
			fmt.Println(" " + cyan(fmt.Sprint(t)))
		}
		fmt.Printf(dim(" %d table(s)\n"), len(tables))

	case "VERIFY":
		m, err := apiGet("/__konto")
		if err != nil {
			errln(err)
			return
		}
		chain, _ := m["chain"].(map[string]any)
		if chain == nil {
			errln(fmt.Errorf("unexpected response"))
			return
		}
		if ok, _ := chain["ok"].(bool); ok {
			seq := int64(m["entries"].(float64))
			fmt.Printf("%s chain intact — %s entries\n", green("✓"), bold(strconv.FormatInt(seq, 10)))
		} else {
			at := chain["tampered_at"]
			fmt.Printf("%s tampered at seq %s\n", red("✗"), bold(fmt.Sprint(at)))
		}

	case "SELECT":
		q := url.Values{}
		for k, v := range st.where {
			q.Set(k, fmt.Sprint(v))
		}
		for k, v := range st.contains {
			q.Set(k+"__contains", v)
		}
		path := "/" + st.table
		if len(q) > 0 {
			path += "?" + q.Encode()
		}
		m, err := apiGet(path)
		if err != nil {
			errln(err)
			return
		}
		rawRows, _ := m["rows"].([]any)
		total := len(rawRows)

		// Apply LIMIT / PAGE slicing client-side.
		if st.limit > 0 {
			start := (st.page - 1) * st.limit
			if start >= total {
				fmt.Println(dim("(0 rows — page out of range)"))
				totalPages := (total + st.limit - 1) / st.limit
				fmt.Printf(dim(" page %d of %d (%d total rows)\n"), st.page, totalPages, total)
				return
			}
			end := start + st.limit
			if end > total {
				end = total
			}
			rawRows = rawRows[start:end]
		}

		if len(rawRows) == 0 {
			fmt.Println(dim("(0 rows)"))
			return
		}

		// Collect all column names in sorted order.
		colSet := map[string]struct{}{}
		var cols []string
		for _, r := range rawRows {
			row, _ := r.(map[string]any)
			for k := range row {
				if _, seen := colSet[k]; !seen {
					colSet[k] = struct{}{}
					cols = append(cols, k)
				}
			}
		}
		sort.Strings(cols)
		printTable(cols, rawRows)

		if st.limit > 0 {
			totalPages := (total + st.limit - 1) / st.limit
			fmt.Printf(dim(" %d row(s) — page %d of %d (%d total)\n"),
				len(rawRows), st.page, totalPages, total)
		} else {
			fmt.Printf(dim(" %d row(s)\n"), len(rawRows))
		}

	case "INSERT":
		row := map[string]any{}
		for i, c := range st.cols {
			row[c] = st.vals[i]
		}
		m, err := apiPost("/"+st.table, row)
		if err != nil {
			errln(err)
			return
		}
		seq := m["seq"]
		hash, _ := m["hash"].(string)
		if len(hash) > 16 {
			hash = hash[:16] + "…"
		}
		fmt.Printf("%s inserted — seq=%s hash=%s\n",
			green("✓"), bold(fmt.Sprint(seq)), dim(hash))

	case "HELP":
		printHelp()

	case "EXIT":
		fmt.Println(dim("bye"))
		os.Exit(0)
	}
}

// ── rendering ─────────────────────────────────────────────────────────────────

func printTable(cols []string, rawRows []any) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	// header
	header := make([]string, len(cols))
	for i, c := range cols {
		header[i] = bold(strings.ToUpper(c))
	}
	fmt.Fprintln(w, " "+strings.Join(header, "\t"))
	// separator
	seps := make([]string, len(cols))
	for i, c := range cols {
		seps[i] = strings.Repeat("─", len(c))
	}
	fmt.Fprintln(w, dim(" "+strings.Join(seps, "\t")))
	// rows
	for _, r := range rawRows {
		row, _ := r.(map[string]any)
		cells := make([]string, len(cols))
		for i, c := range cols {
			cells[i] = fmt.Sprint(row[c])
		}
		fmt.Fprintln(w, " "+strings.Join(cells, "\t"))
	}
	w.Flush()
}

func printHelp() {
	lines := [][2]string{
		{"SHOW TABLES", "list all tables"},
		{"VERIFY", "walk the full hash chain"},
		{"SELECT * FROM <tbl>", "return all rows"},
		{"SELECT * FROM <tbl> LIMIT 20", "first 20 rows"},
		{"SELECT * FROM <tbl> LIMIT 20 PAGE 3", "rows 41-60"},
		{"SELECT * FROM <tbl> WHERE col = 'val' [AND ...]", "exact-match query"},
		{"INSERT INTO <tbl> (c1, c2) VALUES ('v1', v2)", "append a row"},
		{"HELP / \\h", "this message"},
		{"EXIT / QUIT / \\q", "exit the REPL"},
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	for _, l := range lines {
		fmt.Fprintf(w, "  %s\t%s\n", cyan(l[0]), dim(l[1]))
	}
	w.Flush()
}

func errln(err error) {
	fmt.Fprintf(os.Stderr, "%s %s\n", red("error:"), err)
}

// ── REPL loop ─────────────────────────────────────────────────────────────────

func main() {
	addr := flag.String("addr", "http://localhost:7878", "konto base URL")
	flag.Parse()
	baseURL = strings.TrimRight(*addr, "/")

	// ping
	m, err := apiGet("/__konto")
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s cannot reach konto at %s: %v\n", red("✗"), baseURL, err)
		os.Exit(1)
	}
	tables, _ := m["tables"].([]any)
	fmt.Printf("%s connected to %s (%d table(s))\n", green("✓"), cyan(baseURL), len(tables))
	fmt.Println(dim("type HELP for syntax, EXIT to quit"))
	fmt.Println()

	sc := bufio.NewScanner(os.Stdin)
	var buf strings.Builder

	for {
		if buf.Len() == 0 {
			fmt.Print(bold("konto") + "> ")
		} else {
			fmt.Print("     -> ")
		}

		if !sc.Scan() {
			fmt.Println()
			break
		}

		line := sc.Text()

		// allow multi-line input: keep reading until a line ends with ;
		// or is a keyword that doesn't need one.
		buf.WriteString(line)
		buf.WriteRune(' ')

		full := strings.TrimFunc(buf.String(), unicode.IsSpace)
		up := strings.ToUpper(full)
		needsSemi := up != "SHOW TABLES" && up != "VERIFY" && up != "HELP" &&
			up != "EXIT" && up != "QUIT" && up != "\\Q" && up != "\\H"

		if needsSemi && !strings.HasSuffix(full, ";") {
			continue // wait for more input
		}

		buf.Reset()
		if full == "" {
			continue
		}

		st, err := parse(full)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s %s\n", yellow("parse error:"), err)
			continue
		}
		exec(st)
		fmt.Println()
	}
}