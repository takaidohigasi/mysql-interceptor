package compare

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// readReportLines closes the reporter (flushing the writer) and returns
// each JSONL line decoded into a generic map. Use map[string]any rather
// than CompareResult/HeartbeatRecord so the tests can assert on heartbeat
// records and diff records from the same call.
func readReportLines(t *testing.T, r *Reporter, path string) []map[string]any {
	t.Helper()
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open report: %v", err)
	}
	defer f.Close()
	var out []map[string]any
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		out = append(out, m)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return out
}

// TestReporter_LogMatchesOff confirms the default suppresses match and
// ignored records inline but still emits diffs.
func TestReporter_LogMatchesOff(t *testing.T) {
	path := filepath.Join(t.TempDir(), "diffs.jsonl")
	r, err := NewReporterFromOptions(ReporterOptions{OutputFile: path})
	if err != nil {
		t.Fatalf("NewReporter: %v", err)
	}

	r.Record(&CompareResult{Query: "SELECT 1", Match: true})
	r.Record(&CompareResult{Query: "SELECT 2", Match: true, Ignored: true})
	r.Record(&CompareResult{
		Query:       "SELECT 3",
		Match:       false,
		Differences: []Difference{{Type: "cell_value", Column: "x", Original: "a", Replay: "b"}},
	})

	lines := readReportLines(t, r, path)
	if len(lines) != 1 {
		t.Fatalf("expected exactly 1 line emitted (the diff), got %d: %+v", len(lines), lines)
	}
	if got, _ := lines[0]["query"].(string); got != "SELECT 3" {
		t.Errorf("expected diff line for SELECT 3, got query=%q", got)
	}
}

// TestReporter_LogMatchesOn confirms LogMatches=true keeps the previous
// "log every comparison" behavior intact.
func TestReporter_LogMatchesOn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "all.jsonl")
	r, err := NewReporterFromOptions(ReporterOptions{
		OutputFile: path,
		LogMatches: true,
	})
	if err != nil {
		t.Fatalf("NewReporter: %v", err)
	}

	r.Record(&CompareResult{Query: "SELECT 1", Match: true})
	r.Record(&CompareResult{Query: "SELECT 2", Match: true, Ignored: true})
	r.Record(&CompareResult{Query: "SELECT 3", Match: false})

	lines := readReportLines(t, r, path)
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines with LogMatches=true, got %d", len(lines))
	}
}

// TestReporter_Heartbeat checks the heartbeat record carries
// per-window deltas and that a second tick reflects only what
// happened in between.
func TestReporter_Heartbeat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.jsonl")
	r, err := NewReporterFromOptions(ReporterOptions{OutputFile: path})
	if err != nil {
		t.Fatalf("NewReporter: %v", err)
	}

	// First window: 2 matches, 1 diff.
	r.Record(&CompareResult{Match: true})
	r.Record(&CompareResult{Match: true})
	r.Record(&CompareResult{Match: false})
	if err := r.WriteHeartbeat(60 * time.Second); err != nil {
		t.Fatalf("WriteHeartbeat #1: %v", err)
	}

	// Second window: 1 ignored, no diff.
	r.Record(&CompareResult{Match: true, Ignored: true})
	if err := r.WriteHeartbeat(60 * time.Second); err != nil {
		t.Fatalf("WriteHeartbeat #2: %v", err)
	}

	lines := readReportLines(t, r, path)

	var diffs, heartbeats []map[string]any
	for _, l := range lines {
		if t, _ := l["type"].(string); t == "heartbeat" {
			heartbeats = append(heartbeats, l)
		} else {
			diffs = append(diffs, l)
		}
	}

	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff line emitted (the Match=false), got %d", len(diffs))
	}
	if len(heartbeats) != 2 {
		t.Fatalf("expected 2 heartbeat lines, got %d", len(heartbeats))
	}

	// First heartbeat: total=3, matched=2, differed=1, ignored=0.
	hb1 := heartbeats[0]
	if got := hb1["window_total"].(float64); got != 3 {
		t.Errorf("hb1 window_total: want 3, got %v", got)
	}
	if got := hb1["window_matched"].(float64); got != 2 {
		t.Errorf("hb1 window_matched: want 2, got %v", got)
	}
	if got := hb1["window_differed"].(float64); got != 1 {
		t.Errorf("hb1 window_differed: want 1, got %v", got)
	}
	if got := hb1["window_ignored"].(float64); got != 0 {
		t.Errorf("hb1 window_ignored: want 0, got %v", got)
	}
	if got := hb1["window_seconds"].(float64); got != 60 {
		t.Errorf("hb1 window_seconds: want 60, got %v", got)
	}

	// Second heartbeat: only the ignored record landed in this window.
	hb2 := heartbeats[1]
	if got := hb2["window_total"].(float64); got != 1 {
		t.Errorf("hb2 window_total: want 1, got %v", got)
	}
	if got := hb2["window_matched"].(float64); got != 0 {
		t.Errorf("hb2 window_matched: want 0, got %v", got)
	}
	if got := hb2["window_differed"].(float64); got != 0 {
		t.Errorf("hb2 window_differed: want 0, got %v", got)
	}
	if got := hb2["window_ignored"].(float64); got != 1 {
		t.Errorf("hb2 window_ignored: want 1, got %v", got)
	}

	// Cumulative total in hb2 must include both windows.
	if got := hb2["cumulative_total"].(float64); got != 4 {
		t.Errorf("hb2 cumulative_total: want 4, got %v", got)
	}
}

// TestReporter_CloseIdempotent confirms that calling Close more than
// once is safe — the underlying close(r.writeCh) would panic if it
// fired twice, so sync.Once is the load-bearing guard. The first
// call's writer.Close error is returned from subsequent calls too.
func TestReporter_CloseIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rep.jsonl")
	r, err := NewReporterFromOptions(ReporterOptions{OutputFile: path})
	if err != nil {
		t.Fatalf("NewReporter: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second close must not panic. A return value can be the same
	// error as the first call (or nil, if the first call's writer
	// close succeeded).
	if err := r.Close(); err != nil {
		t.Errorf("second Close should match first or be nil, got %v", err)
	}
}

// TestReporter_AsyncWriter_NoLostRecords confirms the async writer
// drains every queued record before Close returns. Lots of producers
// → one writer; the file must contain exactly the records we sent
// (no losses, no duplicates) and no goroutine racing on the writer
// state. Run with -race; the producers all touch r.totalCount /
// digestStats / writeCh in parallel.
func TestReporter_AsyncWriter_NoLostRecords(t *testing.T) {
	const (
		numGoroutines = 32
		recordsEach   = 100
		totalRecords  = numGoroutines * recordsEach
	)
	path := filepath.Join(t.TempDir(), "diffs.jsonl")
	r, err := NewReporterFromOptions(ReporterOptions{
		OutputFile: path,
		LogMatches: true, // emit every record so we can count them deterministically
	})
	if err != nil {
		t.Fatalf("NewReporter: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < recordsEach; i++ {
				r.Record(&CompareResult{
					Query:       fmt.Sprintf("SELECT * FROM tab_%s WHERE id = %d", alphaName(seed), i),
					QueryDigest: fmt.Sprintf("select * from tab_%s where id = ?", alphaName(seed)),
					Match:       i%2 == 0,
				})
			}
		}(g)
	}
	wg.Wait()

	// Close drains writeCh and waits for the writer goroutine.
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read file and count lines.
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	var lines int
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	for sc.Scan() {
		if strings.HasPrefix(strings.TrimSpace(sc.Text()), "{") {
			lines++
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if lines != totalRecords {
		t.Errorf("expected %d lines in report, got %d (drops counted as comparisons_report_dropped if writeCh saturated)", totalRecords, lines)
	}
}
