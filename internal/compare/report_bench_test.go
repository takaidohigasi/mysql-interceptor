package compare

import (
	"bufio"
	"io"
	"path/filepath"
	"sync"
	"testing"
)

// nullCloser wraps an io.Writer with a no-op Close so it satisfies
// io.WriteCloser (Reporter requires WriteCloser for the underlying
// writer). io.Discard is enough to take the file-system cost out of
// the bench so we measure marshal + channel + writer-goroutine
// overhead, not raw disk throughput.
type nullCloser struct{ io.Writer }

func (nullCloser) Close() error { return nil }

// newBenchReporter is a constructor variant that bypasses the file
// open and points the writer at io.Discard. We need this so the
// benchmark doesn't measure os.OpenFile / Sync / inode cache effects.
func newBenchReporter(b *testing.B) *Reporter {
	b.Helper()
	r := &Reporter{
		writer:      nullCloser{Writer: io.Discard},
		writeCh:     make(chan []byte, reporterWriteChCap),
		writerDone:  make(chan struct{}),
		logMatches:  true, // emit every Record so the encode path is on the hot path
		digestStats: NewDigestStats(),
	}
	go r.runWriter()
	b.Cleanup(func() { _ = r.Close() })
	return r
}

// newBufferedBenchReporter mirrors newBenchReporter but enables the
// bufio.Writer wrap so benchmarks see the same code path used in
// production for file-backed sinks. io.Discard still removes the
// disk component, so we measure the marshal + channel + writer +
// bufio coalescing overhead without disk noise.
func newBufferedBenchReporter(b *testing.B) *Reporter {
	b.Helper()
	w := nullCloser{Writer: io.Discard}
	r := &Reporter{
		writer:      w,
		bw:          bufio.NewWriterSize(w, reporterBufSize),
		writeCh:     make(chan []byte, reporterWriteChCap),
		writerDone:  make(chan struct{}),
		logMatches:  true,
		digestStats: NewDigestStats(),
	}
	go r.runWriter()
	b.Cleanup(func() { _ = r.Close() })
	return r
}

// BenchmarkReporter_Record_Diff measures the per-record cost of the
// encode + channel-enqueue path with the emitter pool. Runs each
// goroutine producing CompareResult records in parallel; the writer
// goroutine drains into io.Discard.
//
// Compare allocs/op against the pre-pool path to size the win:
//
//	go test -bench BenchmarkReporter_Record -run='^$' -benchmem -count=3 ./internal/compare/...
func BenchmarkReporter_Record_Diff(b *testing.B) {
	r := newBenchReporter(b)
	// Diff record — has Differences populated, exercises the
	// JSON-encoding path the way real divergence records do.
	tmpl := &CompareResult{
		Query:       "SELECT id, name, email, hashed_password, num_sell_items, num_ticket FROM users WHERE id = ?",
		QueryDigest: "select id, name, email, hashed_password, num_sell_items, num_ticket from users where id = ?",
		SessionID:   42,
		Match:       false,
		Differences: []Difference{
			{Type: "cell_value", Row: 0, Column: "iv_cert", Original: "ad7e8d29bbc3589350ff74fe5295422e9818d14c", Replay: "7dd686cbc139d353f2fca63586af9a9c3da466b8"},
			{Type: "cell_value", Row: 0, Column: "updated", Original: "2026-05-08 10:00:00", Replay: "2026-05-08 09:59:50"},
		},
		OriginalTimeMs: 2.5,
		ReplayTimeMs:   2.6,
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Record(tmpl)
		}
	})
}

// BenchmarkReporter_Record_DiffBuffered mirrors Diff but exercises
// the bufio-wrapped writer path (the file-backed default in
// production). Comparing vs Diff isolates the cost added by bufio
// coalescing.
func BenchmarkReporter_Record_DiffBuffered(b *testing.B) {
	r := newBufferedBenchReporter(b)
	tmpl := &CompareResult{
		Query:       "SELECT id, name, email, hashed_password, num_sell_items, num_ticket FROM users WHERE id = ?",
		QueryDigest: "select id, name, email, hashed_password, num_sell_items, num_ticket from users where id = ?",
		SessionID:   42,
		Match:       false,
		Differences: []Difference{
			{Type: "cell_value", Row: 0, Column: "iv_cert", Original: "ad7e8d29bbc3589350ff74fe5295422e9818d14c", Replay: "7dd686cbc139d353f2fca63586af9a9c3da466b8"},
			{Type: "cell_value", Row: 0, Column: "updated", Original: "2026-05-08 10:00:00", Replay: "2026-05-08 09:59:50"},
		},
		OriginalTimeMs: 2.5,
		ReplayTimeMs:   2.6,
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Record(tmpl)
		}
	})
}

// BenchmarkReporter_Record_Match measures the suppressed-match path
// (no JSON encode, no channel send — just counters + DigestStats).
// Should be cheap; this bench detects regressions in the gate.
func BenchmarkReporter_Record_Match(b *testing.B) {
	path := filepath.Join(b.TempDir(), "rep.jsonl")
	r, err := NewReporterFromOptions(ReporterOptions{
		OutputFile: path,
		LogMatches: false, // matches are suppressed; emit() not called
	})
	if err != nil {
		b.Fatalf("NewReporter: %v", err)
	}
	b.Cleanup(func() { _ = r.Close() })

	tmpl := &CompareResult{
		Query:          "SELECT 1",
		QueryDigest:    "select ?",
		Match:          true,
		OriginalTimeMs: 1.0,
		ReplayTimeMs:   1.0,
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Record(tmpl)
		}
	})
}

// BenchmarkDigestStats_RecordSharded measures concurrent Record
// throughput on the sharded DigestStats. Distinct digests per
// goroutine ensure shards see parallel updates rather than serializing
// on the same shard's mutex; that's the case the sharding is meant
// to win.
func BenchmarkDigestStats_RecordSharded(b *testing.B) {
	ds := NewDigestStatsWithCap(100000) // big enough to never overflow
	// Pre-warm: run each digest once to skip the new-digest CAS path
	// during the actual measurement.
	const distinctDigests = 256
	digests := make([]string, distinctDigests)
	for i := range digests {
		digests[i] = "select * from tab_" + alphaName(i) + " where id = ?"
		ds.Record(&CompareResult{
			Query:       "warm",
			QueryDigest: digests[i],
			Match:       true,
		})
	}

	b.ReportAllocs()
	b.ResetTimer()

	var counter sync.Mutex
	var i int
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine cycles through a unique stride so different
		// shards see parallel updates.
		counter.Lock()
		stride := i % distinctDigests
		i++
		counter.Unlock()
		for pb.Next() {
			ds.Record(&CompareResult{
				Query:          "live",
				QueryDigest:    digests[stride],
				Match:          true,
				OriginalTimeMs: 1.0,
				ReplayTimeMs:   1.1,
			})
			stride = (stride + 1) % distinctDigests
		}
	})
}
