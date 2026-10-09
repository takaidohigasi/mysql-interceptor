package upload

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/takaidohigasi/mysql-interceptor/internal/compare"
	"github.com/takaidohigasi/mysql-interceptor/internal/segment"
)

// fakeGCS records the last body uploaded under each object name.
type fakeGCS struct {
	srv  *httptest.Server
	fail atomic.Bool

	mu      sync.Mutex
	objects map[string]string
	types   map[string]string
}

func newFakeGCS(t *testing.T) *fakeGCS {
	f := &fakeGCS{objects: map[string]string{}, types: map[string]string{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f.fail.Load() {
			_, _ = io.Copy(io.Discard, r.Body)
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		var rd io.Reader = r.Body
		if r.Header.Get("Content-Type") == "application/gzip" {
			gz, err := gzip.NewReader(r.Body)
			if err != nil {
				t.Errorf("not gzip: %v", err)
				return
			}
			rd = gz
		}
		b, _ := io.ReadAll(rd)
		name := r.URL.Query().Get("name")
		f.mu.Lock()
		f.objects[name] = string(b)
		f.types[name] = r.Header.Get("Content-Type")
		f.mu.Unlock()
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeGCS) get(name string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objects[name]
	return b, ok
}

func (f *fakeGCS) uploader() *GCSUploader {
	return &GCSUploader{Bucket: "b", Prefix: "p", Instance: "pod-a", Client: f.srv.Client(), Endpoint: f.srv.URL}
}

func writeFile(t *testing.T, p, content string) {
	t.Helper()
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func exists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

var t9 = time.Date(2026, 10, 9, 9, 15, 0, 0, time.UTC)

func TestShipper_SnapshotOverwritesHourlyAndLatest(t *testing.T) {
	gcs := newFakeGCS(t)
	path := filepath.Join(t.TempDir(), "diff-report.jsonl")
	s := NewShipper(path, gcs.uploader(), 24, 5*time.Second)

	writeFile(t, path, "{\"a\":1}\n")
	s.handle(compare.SegmentEvent{Path: path, Size: 8, Start: t9})
	writeFile(t, path, "{\"a\":1}\n{\"b\":2}\npart")
	s.handle(compare.SegmentEvent{Path: path, Size: 16, Start: t9})

	hourly := "p/2026100909/pod-a/diff-report.jsonl-20261009T091500Z.gz"
	if got, _ := gcs.get(hourly); got != "{\"a\":1}\n{\"b\":2}\n" {
		t.Fatalf("hourly object = %q, want the second snapshot (overwritten, partial record cut)", got)
	}
	latest := "latest/p/pod-a-diff-report.jsonl"
	if got, _ := gcs.get(latest); got != "{\"a\":1}\n{\"b\":2}\n" {
		t.Fatalf("latest object = %q", got)
	}
	if typ := gcs.types[latest]; typ != "application/x-ndjson" {
		t.Fatalf("latest content type = %q, want uncompressed", typ)
	}
	if !exists(path) {
		t.Fatal("the live file must stay")
	}
}

func TestShipper_ClosedSegmentIsRemovedAfterUpload(t *testing.T) {
	gcs := newFakeGCS(t)
	path := filepath.Join(t.TempDir(), "diff-report.jsonl")
	s := NewShipper(path, gcs.uploader(), 24, 5*time.Second)

	seg := segment.Name(path, t9)
	writeFile(t, seg, "{\"a\":1}\n")
	s.handle(compare.SegmentEvent{Path: seg, Size: 8, Start: t9, Closed: true})

	if got, ok := gcs.get("p/2026100909/pod-a/diff-report.jsonl-20261009T091500Z.gz"); !ok || got != "{\"a\":1}\n" {
		t.Fatalf("hourly object = %q, %v", got, ok)
	}
	if exists(seg) {
		t.Fatal("uploaded segment should be removed")
	}
}

func TestShipper_FailedSegmentIsKeptAndRetried(t *testing.T) {
	gcs := newFakeGCS(t)
	path := filepath.Join(t.TempDir(), "diff-report.jsonl")
	s := NewShipper(path, gcs.uploader(), 24, 5*time.Second)

	first := segment.Name(path, t9)
	writeFile(t, first, "{\"a\":1}\n")
	gcs.fail.Store(true)
	s.handle(compare.SegmentEvent{Path: first, Size: 8, Start: t9, Closed: true})
	if !exists(first) {
		t.Fatal("a segment whose upload failed must stay on disk")
	}

	gcs.fail.Store(false)
	second := segment.Name(path, t9.Add(time.Hour))
	writeFile(t, second, "{\"b\":2}\n")
	s.handle(compare.SegmentEvent{Path: second, Size: 8, Start: t9.Add(time.Hour), Closed: true})

	if exists(first) || exists(second) {
		t.Fatal("both segments should be uploaded and removed after recovery")
	}
	if _, ok := gcs.get("p/2026100909/pod-a/diff-report.jsonl-20261009T091500Z.gz"); !ok {
		t.Fatal("the failed segment was not retried")
	}
}

func TestShipper_WithoutUploaderKeepsNewest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "diff-report.jsonl")
	s := NewShipper(path, nil, 2, time.Second)
	var segs []string
	for i := 0; i < 4; i++ {
		seg := segment.Name(path, t9.Add(time.Duration(i)*time.Hour))
		writeFile(t, seg, "x\n")
		segs = append(segs, seg)
		s.handle(compare.SegmentEvent{Path: seg, Size: 2, Start: t9, Closed: true})
	}
	got, _ := segment.List(path)
	if len(got) != 2 || got[0] != segs[2] || got[1] != segs[3] {
		t.Fatalf("segments = %v, want the newest 2: %v", got, segs[2:])
	}
}

func TestShipper_StartAndCloseShipLeftovers(t *testing.T) {
	gcs := newFakeGCS(t)
	path := filepath.Join(t.TempDir(), "diff-report.jsonl")
	leftover := segment.Name(path, t9)
	writeFile(t, leftover, "{\"old\":1}\n")

	s := NewShipper(path, gcs.uploader(), 24, 5*time.Second)
	s.Start() // ships segments left by a previous process
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A segment whose upload failed while running is shipped by Close.
	gcs.fail.Store(true)
	failed := segment.Name(path, t9.Add(time.Hour))
	writeFile(t, failed, "{\"failed\":1}\n")
	s.Notify(compare.SegmentEvent{Path: failed, Size: 13, Start: t9.Add(time.Hour), Closed: true})
	for i := 0; i < 100 && len(s.events) > 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	gcs.fail.Store(false)
	s.Close(ctx)

	if exists(leftover) || exists(failed) {
		t.Fatal("the leftover and the failed segment should both be shipped by Close")
	}
	for _, name := range []string{
		"p/2026100909/pod-a/diff-report.jsonl-20261009T091500Z.gz",
		"p/2026100910/pod-a/diff-report.jsonl-20261009T101500Z.gz",
	} {
		if _, ok := gcs.get(name); !ok {
			t.Errorf("missing object %s", name)
		}
	}
}
