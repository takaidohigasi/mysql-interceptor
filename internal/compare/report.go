package compare

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"

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
	digestStats  *DigestStats
}

func NewReporter(outputFile string) (*Reporter, error) {
	return NewReporterWithDigestCap(outputFile, 0)
}

// NewReporterWithDigestCap constructs a Reporter with a bound on the
// number of unique query digests tracked in the stats map. 0 or negative
// falls back to DefaultMaxUniqueDigests.
func NewReporterWithDigestCap(outputFile string, maxUniqueDigests int) (*Reporter, error) {
	var w io.WriteCloser
	if outputFile == "" || outputFile == "-" {
		w = os.Stdout
	} else {
		f, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
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
		digestStats: NewDigestStatsWithCap(maxUniqueDigests),
	}, nil
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

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.enc.Encode(result); err != nil {
		slog.Error("failed to write comparison result", "err", err)
	}
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
