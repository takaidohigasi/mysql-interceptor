package compare

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/takaidohigasi/mysql-interceptor/internal/segment"
)

// rotateHarness drives a rotating Reporter with a settable clock and
// collects the segment events it reports.
type rotateHarness struct {
	t    *testing.T
	path string
	r    *Reporter

	mu     sync.Mutex
	clock  time.Time
	events []SegmentEvent
}

func newRotateHarness(t *testing.T, minSize int64, start time.Time) *rotateHarness {
	t.Helper()
	h := &rotateHarness{t: t, path: filepath.Join(t.TempDir(), "diff-report.jsonl"), clock: start}
	h.open(minSize)
	return h
}

func (h *rotateHarness) open(minSize int64) {
	h.t.Helper()
	r, err := NewReporterFromOptions(ReporterOptions{
		OutputFile:    h.path,
		Rotate:        true,
		RotateMinSize: minSize,
		OnSegment: func(ev SegmentEvent) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.events = append(h.events, ev)
		},
		Now: func() time.Time {
			h.mu.Lock()
			defer h.mu.Unlock()
			return h.clock
		},
	})
	if err != nil {
		h.t.Fatal(err)
	}
	h.r = r
}

func (h *rotateHarness) setClock(t time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clock = t
}

func (h *rotateHarness) got() []SegmentEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]SegmentEvent(nil), h.events...)
}

func (h *rotateHarness) diff(digest string) {
	h.r.Record(&CompareResult{QueryDigest: digest, Match: false})
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestRotate_ClosesTheHourAsASegment(t *testing.T) {
	t0 := time.Date(2026, 10, 9, 9, 15, 0, 0, time.UTC)
	h := newRotateHarness(t, 0, t0)

	h.diff("select 1")
	h.setClock(t0.Add(45 * time.Minute)) // 10:00
	h.r.rotateNow()
	h.diff("select 2")
	if err := h.r.Close(); err != nil {
		t.Fatal(err)
	}

	ev := h.got()
	if len(ev) != 2 {
		t.Fatalf("events = %+v, want 2 (hour + close)", ev)
	}
	first := segment.Name(h.path, t0)
	if ev[0].Path != first || !ev[0].Closed || !ev[0].Start.Equal(t0) {
		t.Fatalf("hour event = %+v, want closed %s started %v", ev[0], first, t0)
	}
	if got := readFile(t, first); !strings.Contains(got, "select 1") || strings.Contains(got, "select 2") {
		t.Fatalf("first segment = %q, want only the first hour", got)
	}
	if ev[0].Size != int64(len(readFile(t, first))) {
		t.Fatalf("hour event size = %d, want the segment size", ev[0].Size)
	}
	second := segment.Name(h.path, t0.Add(45*time.Minute))
	if ev[1].Path != second || !ev[1].Closed {
		t.Fatalf("close event = %+v, want closed %s", ev[1], second)
	}
	if got := readFile(t, second); !strings.Contains(got, "select 2") {
		t.Fatalf("second segment = %q, want the record written after the hour", got)
	}
	if _, err := os.Stat(h.path); !os.IsNotExist(err) {
		t.Fatalf("live file still there after Close: %v", err)
	}
}

func TestRotate_SmallFileIsKeptAndReportedAsSnapshot(t *testing.T) {
	t0 := time.Date(2026, 10, 9, 9, 15, 0, 0, time.UTC)
	h := newRotateHarness(t, 1<<20, t0) // 1 MiB minimum

	h.diff("select 1")
	h.setClock(t0.Add(45 * time.Minute))
	h.r.rotateNow()

	ev := h.got()
	if len(ev) != 1 || ev[0].Closed || ev[0].Path != h.path || !ev[0].Start.Equal(t0) {
		t.Fatalf("events = %+v, want one open snapshot of %s started %v", ev, h.path, t0)
	}
	if ev[0].Size == 0 || ev[0].Size != int64(len(readFile(t, h.path))) {
		t.Fatalf("snapshot size = %d, want the flushed file size", ev[0].Size)
	}

	// The next hour writes on into the same file, so the closing segment
	// still carries the original start.
	h.diff("select 2")
	if err := h.r.Close(); err != nil {
		t.Fatal(err)
	}
	ev = h.got()
	last := ev[len(ev)-1]
	if want := segment.Name(h.path, t0); last.Path != want || !last.Closed {
		t.Fatalf("close event = %+v, want closed %s", last, want)
	}
	got := readFile(t, last.Path)
	if !strings.Contains(got, "select 1") || !strings.Contains(got, "select 2") {
		t.Fatalf("segment = %q, want both hours", got)
	}
}

func TestRotate_EmptyFileIsNotRotated(t *testing.T) {
	t0 := time.Date(2026, 10, 9, 9, 15, 0, 0, time.UTC)
	h := newRotateHarness(t, 0, t0)
	h.r.rotateNow()
	if err := h.r.Close(); err != nil {
		t.Fatal(err)
	}
	if ev := h.got(); len(ev) != 0 {
		t.Fatalf("events = %+v, want none for an empty report", ev)
	}
	if segs, _ := segment.List(h.path); len(segs) != 0 {
		t.Fatalf("segments = %v, want none", segs)
	}
}

func TestRotate_AdoptsALeftoverFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "diff-report.jsonl")
	if err := os.WriteFile(path, []byte("{\"from\":\"previous process\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mtime := time.Date(2026, 10, 9, 8, 30, 0, 0, time.UTC)
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}

	h := &rotateHarness{t: t, path: path, clock: mtime.Add(time.Hour)}
	h.open(0)
	defer h.r.Close()

	ev := h.got()
	want := segment.Name(path, mtime)
	if len(ev) != 1 || ev[0].Path != want || !ev[0].Closed || !ev[0].Start.Equal(mtime) {
		t.Fatalf("events = %+v, want the leftover as closed %s", ev, want)
	}
	if got := readFile(t, want); !strings.Contains(got, "previous process") {
		t.Fatalf("leftover segment = %q", got)
	}
	if got := readFile(t, path); got != "" {
		t.Fatalf("new live file = %q, want empty", got)
	}
}

func TestRotate_OffKeepsOneFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "diff-report.jsonl")
	r, err := NewReporterFromOptions(ReporterOptions{OutputFile: path})
	if err != nil {
		t.Fatal(err)
	}
	r.Record(&CompareResult{QueryDigest: "select 1"})
	r.rotateNow()
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, path); !strings.Contains(got, "select 1") {
		t.Fatalf("report = %q, want the record in the single file", got)
	}
	if segs, _ := segment.List(path); len(segs) != 0 {
		t.Fatalf("segments = %v, want none without Rotate", segs)
	}
}

func TestUntilNextHour(t *testing.T) {
	at := time.Date(2026, 10, 9, 9, 59, 30, 0, time.FixedZone("JST", 9*3600))
	if got := untilNextHour(at); got != 30*time.Second {
		t.Fatalf("untilNextHour = %v, want 30s", got)
	}
}
