package compare

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
)

type Reporter struct {
	writer      io.WriteCloser
	mu          sync.Mutex
	enc         *json.Encoder
	totalCount  atomic.Int64
	matchCount  atomic.Int64
	diffCount   atomic.Int64
	digestStats *DigestStats
}

func NewReporter(outputFile string) (*Reporter, error) {
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
		digestStats: NewDigestStats(),
	}, nil
}

func (r *Reporter) Record(result *CompareResult) {
	r.totalCount.Add(1)
	if result.Match {
		r.matchCount.Add(1)
	} else {
		r.diffCount.Add(1)
	}

	r.digestStats.Record(result)

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.enc.Encode(result); err != nil {
		log.Printf("failed to write comparison result: %v", err)
	}
}

func (r *Reporter) Summary() string {
	total := r.totalCount.Load()
	matched := r.matchCount.Load()
	diffed := r.diffCount.Load()

	s := fmt.Sprintf("Comparison summary: total=%d matched=%d different=%d", total, matched, diffed)
	s += r.digestStats.PrintSummary()
	return s
}

func (r *Reporter) DigestStats() *DigestStats {
	return r.digestStats
}

func (r *Reporter) Close() error {
	return r.writer.Close()
}
