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
		{"mysql-interceptor/my-cluster", "pod-a", "mysql-interceptor/my-cluster/pod-a/diff-report.jsonl-20261005T010203Z.gz"},
		{"/trimmed/", "/pod-a/", "trimmed/pod-a/diff-report.jsonl-20261005T010203Z.gz"},
		{"", "pod-a", "pod-a/diff-report.jsonl-20261005T010203Z.gz"},
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
	want := "mysql-interceptor/pod-a/diff-report.jsonl-20261005T010203Z.gz"
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
