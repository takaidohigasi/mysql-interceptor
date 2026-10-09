package segment

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestNameAndStart(t *testing.T) {
	path := filepath.Join("logs", "diff-report.jsonl")
	start := time.Date(2026, 10, 9, 9, 0, 0, 0, time.FixedZone("JST", 9*3600))

	name := Name(path, start)
	if want := path + "-20261009T000000Z"; name != want {
		t.Fatalf("Name = %q, want %q (UTC)", name, want)
	}
	got, ok := Start(path, name)
	if !ok || !got.Equal(start) {
		t.Fatalf("Start(%q) = %v, %v; want %v, true", name, got, ok, start)
	}
	for _, s := range []string{path, path + "-", path + "-garbage", path + ".gz", "other-20261009T000000Z"} {
		if _, ok := Start(path, s); ok {
			t.Errorf("Start(%q) = ok, want not a segment", s)
		}
	}
}

func TestList(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "diff-report.jsonl")
	files := []string{
		"diff-report.jsonl",                  // the live file
		"diff-report.jsonl-20261009T100000Z", // segments, unsorted on purpose
		"diff-report.jsonl-20261009T090000Z",
		"diff-report.jsonl-not-a-time",
		"query.jsonl-20261009T090000Z",
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got, err := List(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		filepath.Join(dir, "diff-report.jsonl-20261009T090000Z"),
		filepath.Join(dir, "diff-report.jsonl-20261009T100000Z"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("List = %v, want %v", got, want)
	}

	if got, err := List(filepath.Join(dir, "missing", "r.jsonl")); err != nil || got != nil {
		t.Fatalf("List(missing dir) = %v, %v; want nil, nil", got, err)
	}
}
