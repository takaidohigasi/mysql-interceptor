package compare

import (
	"bufio"
	"bytes"
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

// reporterWriteChCap is the buffer size of the async writer channel.
// Sized to absorb a few seconds of normal diff volume without
// blocking producers; once full the producer drops the record and
// counts it on comparisons_report_dropped.
const reporterWriteChCap = 4096

// reporterBufSize is the user-space write buffer wrapping the report
// file. Sized to coalesce ~20+ typical diff records (~700B each) into
// one write syscall. Bigger buffer = fewer syscalls but a larger crash
// window; 16KiB hits the same sweet spot bufio.NewWriter uses by
// default and keeps loss bounded for unflushed records on SIGKILL.
const reporterBufSize = 16 << 10

// reporterFlushInterval bounds how long an isolated record can sit in
// the bufio buffer before reaching the file. Under sustained load the
// 16KiB buffer auto-flushes well before this fires; the ticker only
// matters at low diff rates where a single record could otherwise be
// invisible to operators tailing the file.
const reporterFlushInterval = 250 * time.Millisecond

// emitterEntry is a pooled (bytes.Buffer, *json.Encoder) pair used
// by emit() to amortize per-record allocation. The Buffer's internal
// []byte is reused across pool gets — bytes.Buffer.Reset preserves
// its capacity — so steady-state encoding hits zero allocations on
// the buffer itself.
//
// The encoder is paired 1:1 with its buffer because json.NewEncoder
// captures the io.Writer at construction; you can't swap writers
// after the fact. SetEscapeHTML(false) is set on the encoder during
// pool construction so the per-call hot path doesn't need to set it.
type emitterEntry struct {
	buf *bytes.Buffer
	enc *json.Encoder
}

// emitterPool reuses (Buffer, Encoder) pairs across calls. Pool
// entries can be reclaimed by GC at any time — that's fine because
// the New func builds a fresh entry on Get when needed.
var emitterPool = sync.Pool{
	New: func() interface{} {
		buf := &bytes.Buffer{}
		enc := json.NewEncoder(buf)
		enc.SetEscapeHTML(false)
		return &emitterEntry{buf: buf, enc: enc}
	},
}

// Reporter writes comparison records (and periodic heartbeats) to an
// output sink, plus accumulates aggregate counters and per-digest
// stats. Encoding and writing happen on a dedicated background
// goroutine so producers (proxy / shadow worker / heartbeat ticker)
// don't serialize on the file write — at high diff rates the
// previous synchronous-with-mutex design pinned every recorder
// behind one Encode call. The producer still does the JSON
// marshaling on its own goroutine (so encoder cost is parallelized);
// it just hands the encoded bytes off via a buffered channel.
//
// Lifecycle: Close() closes the channel, waits for the writer
// goroutine to drain queued records, then closes the underlying
// writer. Calling Record concurrently with Close is unsafe (would
// send on a closed channel). Existing call sites already serialize
// against Close via ShadowSender.Close + bgWG / OfflineReplayer.Run
// teardown order, so this contract is met without extra locking.
type Reporter struct {
	writer io.WriteCloser
	// bw wraps writer with a user-space buffer for file-backed sinks.
	// Nil when the underlying writer is os.Stdout — stdout traffic is
	// usually a human tail or a small dev test, where added latency
	// from buffering is more annoying than the syscall savings.
	// Touched only by runWriter, so no locking is needed.
	bw         *bufio.Writer
	writeCh    chan []byte
	writerDone chan struct{}
	closeOnce  sync.Once
	closeErr   error

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
// or "-" routes output to stdout. Spawns the async writer goroutine;
// callers MUST call Close to flush and stop it.
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

	r := &Reporter{
		writer:      w,
		writeCh:     make(chan []byte, reporterWriteChCap),
		writerDone:  make(chan struct{}),
		logMatches:  opts.LogMatches,
		digestStats: NewDigestStatsWithCap(opts.MaxUniqueDigests),
	}
	if w != os.Stdout {
		r.bw = bufio.NewWriterSize(w, reporterBufSize)
	}
	go r.runWriter()
	return r, nil
}

// runWriter is the sole consumer of writeCh. By concentrating the
// underlying file Write into one goroutine we eliminate the previous
// design's mutex-around-encode hot path: producers now race for a
// channel send instead of a mutex, and the channel is non-blocking
// (drop-on-full).
//
// For file-backed sinks the bufio.Writer coalesces many records into
// each underlying syscall; bufio auto-flushes when its buffer fills,
// and a periodic ticker bounds the time an isolated record can sit
// in the buffer at low diff rates. On channel close (Reporter.Close)
// the final Flush happens before the goroutine returns, so Close
// observes a fully drained file.
func (r *Reporter) runWriter() {
	defer close(r.writerDone)

	// Pick the active write target once: bufio when wrapping a file,
	// the raw writer otherwise. Avoids a per-call branch on every
	// record.
	var sink io.Writer = r.writer
	if r.bw != nil {
		sink = r.bw
	}

	flush := func() {
		if r.bw == nil {
			return
		}
		if err := r.bw.Flush(); err != nil {
			slog.Error("failed to flush comparison records", "err", err)
		}
	}

	// The flush ticker is only useful when bufio buffering is enabled.
	// For stdout we leave tickerC nil so the select arm never fires.
	var tickerC <-chan time.Time
	if r.bw != nil {
		t := time.NewTicker(reporterFlushInterval)
		defer t.Stop()
		tickerC = t.C
	}

	for {
		select {
		case buf, ok := <-r.writeCh:
			if !ok {
				flush()
				return
			}
			if _, err := sink.Write(buf); err != nil {
				slog.Error("failed to write comparison record", "err", err)
			}
		case <-tickerC:
			flush()
		}
	}
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

	r.emit(result)
}

// emit JSON-encodes the value and hands the bytes to the writer
// goroutine via writeCh. Non-blocking: if the channel is full
// (writer fell behind producers), the record is dropped and counted
// on comparisons_report_dropped. Dropping is preferred to blocking
// because the alternative would back-pressure the proxy hot path.
func (r *Reporter) emit(v interface{}) {
	// Grab a pooled (Buffer, Encoder) pair. The encoder already has
	// SetEscapeHTML(false) — done once at pool construction — so the
	// hot path doesn't pay that cost per call. SetEscapeHTML(false)
	// matches the pre-async output format (no < / > / &
	// escapes for <, >, & in cell values).
	e := emitterPool.Get().(*emitterEntry)
	e.buf.Reset()
	if err := e.enc.Encode(v); err != nil {
		slog.Error("failed to marshal comparison record", "err", err)
		emitterPool.Put(e)
		return
	}
	// We must copy the bytes out before returning the entry to the
	// pool, because the next pool consumer will Reset() the buffer
	// and overwrite the underlying []byte. Without the copy, the
	// writer goroutine would race the next emit() call.
	//
	// json.Encoder.Encode appends a trailing newline, so the payload
	// is ready to write as-is.
	payload := make([]byte, e.buf.Len())
	copy(payload, e.buf.Bytes())
	emitterPool.Put(e)

	select {
	case r.writeCh <- payload:
	default:
		metrics.Global.ComparisonsReportDropped.Add(1)
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
// since the previous call (or since startup, for the first call). The
// returned error is reserved for future use — the actual write
// happens asynchronously in the writer goroutine, so a write error
// would not be visible here. A return of nil does NOT mean the bytes
// reached disk; it means the record was either queued for the writer
// or counted as comparisons_report_dropped if the queue was full.
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

	r.emit(&hb)
	return nil
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

// Close stops the async writer and closes the underlying writer.
// Drains any queued records first so callers see a consistent
// snapshot at shutdown. Idempotent: subsequent calls are no-ops and
// return the same error as the first call (sync.Once guards the
// channel close, which would otherwise panic on the second call).
//
// Concurrent calls to Record while Close runs are unsafe (sending on
// a closed channel panics). Callers must serialize Close after the
// last Record (existing usage in ShadowSender.Close after sessions +
// background goroutines drain, and OfflineReplayer.Run via defer at
// the end, satisfies this).
func (r *Reporter) Close() error {
	r.closeOnce.Do(func() {
		close(r.writeCh)
		<-r.writerDone
		r.closeErr = r.writer.Close()
	})
	return r.closeErr
}
