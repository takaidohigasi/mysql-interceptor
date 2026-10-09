package upload

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestObjectName(t *testing.T) {
	ts := time.Date(2026, 10, 5, 1, 2, 3, 0, time.UTC)
	cases := []struct {
		prefix, instance, want string
	}{
		{"mysql-interceptor/my-cluster", "pod-a", "mysql-interceptor/my-cluster/2026100501/pod-a/diff-report.jsonl-20261005T010203Z.gz"},
		{"/trimmed/", "/pod-a/", "trimmed/2026100501/pod-a/diff-report.jsonl-20261005T010203Z.gz"},
		{"", "pod-a", "2026100501/pod-a/diff-report.jsonl-20261005T010203Z.gz"},
	}
	for _, c := range cases {
		u := &GCSUploader{Prefix: c.prefix, Instance: c.instance}
		if got := u.ObjectName("diff-report.jsonl", ts); got != c.want {
			t.Errorf("ObjectName(%q, %q) = %q, want %q", c.prefix, c.instance, got, c.want)
		}
	}
}

func TestUploadGzip(t *testing.T) {
	dir := t.TempDir()
	report := filepath.Join(dir, "diff-report.jsonl")
	content := "{\"query_digest\":\"select ?\"}\n"
	if err := os.WriteFile(report, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	var gotPath, gotName, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotName = r.URL.Query().Get("name")
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Errorf("body is not gzip: %v", err)
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		b, _ := io.ReadAll(gz)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u := &GCSUploader{
		Bucket:   "my-bucket",
		Prefix:   "mysql-interceptor",
		Instance: "pod-a",
		Client:   srv.Client(),
		Endpoint: srv.URL,
		Now:      func() time.Time { return time.Date(2026, 10, 5, 1, 2, 3, 0, time.UTC) },
	}
	object, err := u.UploadGzip(context.Background(), report)
	if err != nil {
		t.Fatal(err)
	}
	want := "mysql-interceptor/2026100501/pod-a/diff-report.jsonl-20261005T010203Z.gz"
	if object != want || gotName != want {
		t.Errorf("object = %q, request name = %q, want %q", object, gotName, want)
	}
	if gotPath != "/upload/storage/v1/b/my-bucket/o" {
		t.Errorf("path = %q", gotPath)
	}
	if gotBody != content {
		t.Errorf("uploaded body = %q, want %q", gotBody, content)
	}
}

func TestUploadGzip_SkipsMissingAndEmpty(t *testing.T) {
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty.jsonl")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	u := &GCSUploader{Bucket: "b", Client: http.DefaultClient, Endpoint: "http://127.0.0.1:0"}
	for _, p := range []string{filepath.Join(dir, "missing.jsonl"), empty} {
		object, err := u.UploadGzip(context.Background(), p)
		if err != nil || object != "" {
			t.Errorf("UploadGzip(%s) = %q, %v; want skip", p, object, err)
		}
	}
}

func TestUploadGzip_ErrorStatus(t *testing.T) {
	report := filepath.Join(t.TempDir(), "diff-report.jsonl")
	if err := os.WriteFile(report, []byte("x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		http.Error(w, "permission denied", http.StatusForbidden)
	}))
	defer srv.Close()
	u := &GCSUploader{Bucket: "b", Instance: "pod-a", Client: srv.Client(), Endpoint: srv.URL}
	_, err := u.UploadGzip(context.Background(), report)
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Errorf("expected a 403 error, got %v", err)
	}
}

func TestObjectName_UsesUTCHour(t *testing.T) {
	u := &GCSUploader{Prefix: "p", Instance: "pod-a"}
	jst := time.Date(2026, 10, 9, 9, 30, 0, 0, time.FixedZone("JST", 9*3600))
	if got, want := u.ObjectName("r.jsonl", jst), "p/2026100900/pod-a/r.jsonl-20261009T003000Z.gz"; got != want {
		t.Errorf("ObjectName = %q, want %q", got, want)
	}
}

func TestLatestObjectName(t *testing.T) {
	cases := []struct{ prefix, instance, want string }{
		{"mysql-interceptor/my-cluster", "pod-a", "latest/mysql-interceptor/my-cluster/pod-a-diff-report.jsonl"},
		{"/trimmed/", "/pod-a/", "latest/trimmed/pod-a-diff-report.jsonl"},
		{"", "pod-a", "latest/pod-a-diff-report.jsonl"},
		{"p", "", "latest/p/diff-report.jsonl"},
	}
	for _, c := range cases {
		u := &GCSUploader{Prefix: c.prefix, Instance: c.instance}
		if got := u.LatestObjectName("diff-report.jsonl"); got != c.want {
			t.Errorf("LatestObjectName(%q, %q) = %q, want %q", c.prefix, c.instance, got, c.want)
		}
	}
}

// Upload sends only the first size bytes, gzipped or plain.
func TestUpload_RangeAndEncoding(t *testing.T) {
	report := filepath.Join(t.TempDir(), "diff-report.jsonl")
	if err := os.WriteFile(report, []byte("{\"a\":1}\n{\"b\":2}\npartial"), 0o600); err != nil {
		t.Fatal(err)
	}
	var gotType, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotType = r.Header.Get("Content-Type")
		var rd io.Reader = r.Body
		if gotType == "application/gzip" {
			gz, err := gzip.NewReader(r.Body)
			if err != nil {
				t.Errorf("body is not gzip: %v", err)
				return
			}
			rd = gz
		}
		b, _ := io.ReadAll(rd)
		gotBody = string(b)
	}))
	defer srv.Close()
	u := &GCSUploader{Bucket: "b", Client: srv.Client(), Endpoint: srv.URL}
	want := "{\"a\":1}\n{\"b\":2}\n"
	for _, gz := range []bool{true, false} {
		if err := u.Upload(context.Background(), report, int64(len(want)), "o", gz); err != nil {
			t.Fatal(err)
		}
		wantType := "application/x-ndjson"
		if gz {
			wantType = "application/gzip"
		}
		if gotType != wantType || gotBody != want {
			t.Errorf("gz=%v: type %q body %q, want %q %q", gz, gotType, gotBody, wantType, want)
		}
	}
}
