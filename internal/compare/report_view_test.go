package compare

import (
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const reportFixture = `{"type":"heartbeat","timestamp":"2026-10-05T00:00:00Z","window_seconds":60,"window_total":3,"window_matched":2,"window_differed":1,"window_ignored":0,"cumulative_total":3,"cumulative_differed":1}
{"query":"SELECT secret FROM users WHERE id = 1","query_digest":"select secret from users where id = ?","session_id":1,"user":"app","timestamp":"2026-10-05T00:00:01Z","match":false,"differences":[{"type":"cell_value","row":0,"column":"secret","original":"a","replay":"b"}],"original_time_ms":1,"replay_time_ms":2,"time_diff_ms":1,"time_diff_exceeded":false}
not json
{"query":"SELECT COUNT(*) FROM items","query_digest":"select count(?) from items","session_id":2,"timestamp":"2026-10-05T01:00:00Z","match":false,"differences":[{"type":"row_count","original":"1","replay":"2"}],"original_time_ms":1,"replay_time_ms":1,"time_diff_ms":0,"time_diff_exceeded":false}
{"query":"SELECT secret FROM users WHERE id = 2","query_digest":"select secret from users where id = ?","session_id":1,"user":"app","timestamp":"2026-10-05T02:00:00Z","match":false,"differences":[{"type":"cell_value","row":0,"column":"secret","original":"c","replay":"d"}],"original_time_ms":1,"replay_time_ms":2,"time_diff_ms":1,"time_diff_exceeded":false}
`

func TestReadReport_HidesValuesByDefault(t *testing.T) {
	records, skipped, err := ReadReport(strings.NewReader(reportFixture), ReportViewOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if skipped != 1 {
		t.Errorf("skipped = %d, want 1", skipped)
	}
	if len(records) != 3 {
		t.Fatalf("records = %d, want 3 (heartbeat excluded)", len(records))
	}
	for _, r := range records {
		if r.Query != hiddenValue {
			t.Errorf("query not hidden: %q", r.Query)
		}
		for _, d := range r.Differences {
			if d.Original != hiddenValue || d.Replay != hiddenValue {
				t.Errorf("difference values not hidden: %+v", d)
			}
		}
	}
}

func TestReadReport_ShowValuesAndFilters(t *testing.T) {
	records, _, err := ReadReport(strings.NewReader(reportFixture), ReportViewOptions{
		ShowValues: true,
		Digest:     "USERS",
		Since:      time.Date(2026, 10, 5, 1, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	if records[0].Query != "SELECT secret FROM users WHERE id = 2" || records[0].Differences[0].Original != "c" {
		t.Errorf("values not kept: %+v", records[0])
	}
}

func TestSummarizeReport(t *testing.T) {
	records, _, err := ReadReport(strings.NewReader(reportFixture), ReportViewOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := SummarizeReport(records)
	if len(got) != 2 {
		t.Fatalf("digests = %d, want 2", len(got))
	}
	if got[0].Digest != "select secret from users where id = ?" || got[0].Differed != 2 {
		t.Errorf("first summary = %+v", got[0])
	}
	if FormatCounts(got[0].Columns) != "secret=2" || FormatCounts(got[1].Types) != "row_count=1" {
		t.Errorf("counts = %s / %s", FormatCounts(got[0].Columns), FormatCounts(got[1].Types))
	}
	if FormatCounts(nil) != "-" {
		t.Errorf("empty counts should render as -")
	}
}

func TestOpenReport_Gzip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "diff-report.jsonl.gz")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	if _, err := gz.Write([]byte(reportFixture)); err != nil {
		t.Fatal(err)
	}
	gz.Close()
	f.Close()

	rc, err := OpenReport(path)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	records, _, err := ReadReport(rc, ReportViewOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Errorf("records = %d, want 3", len(records))
	}
}
