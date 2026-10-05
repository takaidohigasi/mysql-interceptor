package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "diff-report.jsonl")
	data := `{"query":"SELECT secret FROM users WHERE id = 1","query_digest":"select secret from users where id = ?","session_id":1,"user":"app","timestamp":"2026-10-05T00:00:01Z","match":false,"differences":[{"type":"cell_value","row":0,"column":"secret","original":"topsecret","replay":"b"}],"original_time_ms":1,"replay_time_ms":2,"time_diff_ms":1,"time_diff_exceeded":false}
`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := report([]string{"--file", path}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "cell_value[secret]") {
		t.Errorf("missing difference summary:\n%s", out.String())
	}
	if strings.Contains(out.String(), "topsecret") || strings.Contains(out.String(), "id = 1") {
		t.Errorf("values leaked without --show-values:\n%s", out.String())
	}

	out.Reset()
	if err := report([]string{"--file", path, "--show-values"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "topsecret") {
		t.Errorf("--show-values should print values:\n%s", out.String())
	}

	out.Reset()
	if err := report([]string{"--file", path, "--summary"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "secret=1") {
		t.Errorf("summary missing column count:\n%s", out.String())
	}

	if err := report([]string{}, &out); err == nil {
		t.Errorf("expected an error without --file")
	}
}
