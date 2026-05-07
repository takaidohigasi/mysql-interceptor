package compare

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/takaidohigasi/mysql-interceptor/internal/metrics"
)

type Reporter struct {
	writer       io.WriteCloser
	mu           sync.Mutex
	enc          *json.Encoder
	totalCount   atomic.Int64
	matchCount   atomic.Int64
	diffCount    atomic.Int64
	ignoredCount atomic.Int64
	// Snapshots taken on the last heartbeat tick, used to compute
	// per-window deltas without locking. Atomic Swap on each call
	// keeps the deltas consistent with whatever cumulative counts
	// are visible at the same instant.
	lastTotal    atomic.Int64
	lastMatched  atomic.Int64
	lastDiffered atomic.Int64
	lastIgnored  atomic.Int64
	logMatches   bool
	digestStats  *DigestStats
}

// ReporterOptions bundles the constructor knobs so callers don't need
// to chase positional arguments as new options get added.
type ReporterOptions struct {
	OutputFile       string
	MaxUniqueDigests int
	// LogMatches: when true, every comparison result is written to the
	// output file. When false (default), only diffs are written; matched
	// and ignored results are summarized via the periodic heartbeat
	// (see WriteHeartbeat) instead of one line per query.
	LogMatches bool
}

func NewReporter(outputFile string) (*Reporter, error) {
	return NewReporterFromOptions(ReporterOptions{OutputFile: outputFile})
}

// NewReporterWithDigestCap is kept for backward compatibility with
// callers that haven't moved to ReporterOptions yet. New call sites
// should use NewReporterFromOptions.
func NewReporterWithDigestCap(outputFile string, maxUniqueDigests int) (*Reporter, error) {
	return NewReporterFromOptions(ReporterOptions{
		OutputFile:       outputFile,
		MaxUniqueDigests: maxUniqueDigests,
	})
}

// NewReporterFromOptions constructs a Reporter with full control over
// digest capacity and per-record logging behavior. An empty OutputFile
// or "-" routes output to stdout.
func NewReporterFromOptions(opts ReporterOptions) (*Reporter, error) {
	var w io.WriteCloser
	if opts.OutputFile == "" || opts.OutputFile == "-" {
		w = os.Stdout
	} else {
		f, err := os.OpenFile(opts.OutputFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("opening report file: %w", err)
		}
		w = f
	}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	return &Reporter{
		writer:      w,
		enc:         enc,
		logMatches:  opts.LogMatches,
		digestStats: NewDigestStatsWithCap(opts.MaxUniqueDigests),
	}, nil
}

// shouldEmit reports whether a comparison result should be written
// inline. Diff results always emit; clean matches and ignored entries
// are only emitted when LogMatches is enabled.
func (r *Reporter) shouldEmit(result *CompareResult) bool {
	if r.logMatches {
		return true
	}
	if result.Ignored {
		return false
	}
	return !result.Match
}

func (r *Reporter) Record(result *CompareResult) {
	r.totalCount.Add(1)
	metrics.Global.ComparisonsTotal.Add(1)
	switch {
	case result.Ignored:
		r.ignoredCount.Add(1)
		metrics.Global.ComparisonsIgnored.Add(1)
	case result.Match:
		r.matchCount.Add(1)
		metrics.Global.ComparisonsMatched.Add(1)
	default:
		r.diffCount.Add(1)
		metrics.Global.ComparisonsDiffered.Add(1)
	}

	r.digestStats.Record(result)

	if !r.shouldEmit(result) {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.enc.Encode(result); err != nil {
		slog.Error("failed to write comparison result", "err", err)
	}
}

// HeartbeatRecord is the JSON shape emitted by WriteHeartbeat. It
// carries a "type" field so consumers can distinguish heartbeats from
// diff records (CompareResult lacks a "type" field).
type HeartbeatRecord struct {
	Type            string  `json:"type"`
	Timestamp       string  `json:"timestamp"`
	WindowSeconds   float64 `json:"window_seconds"`
	WindowTotal     int64   `json:"window_total"`
	WindowMatched   int64   `json:"window_matched"`
	WindowDiffered  int64   `json:"window_differed"`
	WindowIgnored   int64   `json:"window_ignored"`
	CumulativeTotal int64   `json:"cumulative_total"`
	CumulativeDiff  int64   `json:"cumulative_differed"`
}

// WriteHeartbeat emits a single line summarizing comparison activity
// since the previous call (or since startup, for the first call). It's
// safe to invoke concurrently with Record. Window deltas are derived
// from atomic Swap on the snapshot fields, so missing-by-one against
// in-flight records is possible — the deltas always converge over the
// next heartbeat or at shutdown.
func (r *Reporter) WriteHeartbeat(window time.Duration) error {
	total := r.totalCount.Load()
	matched := r.matchCount.Load()
	diffed := r.diffCount.Load()
	ignored := r.ignoredCount.Load()

	hb := HeartbeatRecord{
		Type:            "heartbeat",
		Timestamp:       time.Now().UTC().Format(time.RFC3339Nano),
		WindowSeconds:   window.Seconds(),
		WindowTotal:     total - r.lastTotal.Swap(total),
		WindowMatched:   matched - r.lastMatched.Swap(matched),
		WindowDiffered:  diffed - r.lastDiffered.Swap(diffed),
		WindowIgnored:   ignored - r.lastIgnored.Swap(ignored),
		CumulativeTotal: total,
		CumulativeDiff:  diffed,
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return r.enc.Encode(&hb)
}

func (r *Reporter) Summary() string {
	total := r.totalCount.Load()
	matched := r.matchCount.Load()
	diffed := r.diffCount.Load()
	ignored := r.ignoredCount.Load()

	s := fmt.Sprintf("Comparison summary: total=%d matched=%d different=%d ignored=%d",
		total, matched, diffed, ignored)
	s += r.digestStats.PrintSummary()
	return s
}

func (r *Reporter) DigestStats() *DigestStats {
	return r.digestStats
}

func (r *Reporter) Close() error {
	return r.writer.Close()
}
