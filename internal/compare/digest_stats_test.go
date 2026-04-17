package compare

import (
	"strings"
	"testing"
	"time"
)

func TestDigestStats_GroupsByDigest(t *testing.T) {
	ds := NewDigestStats()

	ds.Record(&CompareResult{
		Query:          "SELECT * FROM users WHERE id = 1",
		Timestamp:      time.Now(),
		Match:          true,
		OriginalTimeMs: 2.0,
		ReplayTimeMs:   2.5,
	})
	ds.Record(&CompareResult{
		Query:          "SELECT * FROM users WHERE id = 42",
		Timestamp:      time.Now(),
		Match:          true,
		OriginalTimeMs: 3.0,
		ReplayTimeMs:   3.5,
	})
	ds.Record(&CompareResult{
		Query:          "SELECT * FROM orders WHERE user_id = 1",
		Timestamp:      time.Now(),
		Match:          false,
		Differences:    []Difference{{Type: "error"}},
		OriginalTimeMs: 1.0,
		ReplayTimeMs:   0.5,
	})

	summaries := ds.Summaries()

	if len(summaries) != 2 {
		t.Fatalf("expected 2 unique digests, got %d", len(summaries))
	}

	// Find users digest (should have count=2)
	var usersSummary, ordersSummary *DigestSummary
	for i := range summaries {
		if strings.Contains(summaries[i].Digest, "users") {
			usersSummary = &summaries[i]
		}
		if strings.Contains(summaries[i].Digest, "orders") {
			ordersSummary = &summaries[i]
		}
	}

	if usersSummary == nil {
		t.Fatal("expected to find users digest")
	}
	if usersSummary.Count != 2 {
		t.Errorf("expected users count=2, got %d", usersSummary.Count)
	}
	if usersSummary.MatchCount != 2 {
		t.Errorf("expected users match_count=2, got %d", usersSummary.MatchCount)
	}
	// avg of 2.0, 3.0 = 2.5
	if usersSummary.OriginalAvg != 2.5 {
		t.Errorf("expected users original avg=2.5, got %.2f", usersSummary.OriginalAvg)
	}
	// avg of 2.5, 3.5 = 3.0
	if usersSummary.ReplayAvg != 3.0 {
		t.Errorf("expected users replay avg=3.0, got %.2f", usersSummary.ReplayAvg)
	}

	if ordersSummary == nil {
		t.Fatal("expected to find orders digest")
	}
	if ordersSummary.Count != 1 {
		t.Errorf("expected orders count=1, got %d", ordersSummary.Count)
	}
	if ordersSummary.DiffCount != 1 {
		t.Errorf("expected orders diff_count=1, got %d", ordersSummary.DiffCount)
	}
	if ordersSummary.ErrorCount != 1 {
		t.Errorf("expected orders error_count=1, got %d", ordersSummary.ErrorCount)
	}
}

func TestDigestStats_Percentiles(t *testing.T) {
	ds := NewDigestStats()

	// Add 100 entries with increasing times
	for i := 1; i <= 100; i++ {
		ds.Record(&CompareResult{
			Query:          "SELECT 1",
			Timestamp:      time.Now(),
			Match:          true,
			OriginalTimeMs: float64(i),
			ReplayTimeMs:   float64(i) * 1.1,
		})
	}

	summaries := ds.Summaries()
	if len(summaries) != 1 {
		t.Fatalf("expected 1 digest, got %d", len(summaries))
	}

	s := summaries[0]

	if s.Count != 100 {
		t.Errorf("expected count=100, got %d", s.Count)
	}

	// avg of 1..100 = 50.5
	if s.OriginalAvg != 50.5 {
		t.Errorf("expected original avg=50.5, got %.2f", s.OriginalAvg)
	}

	// p95 of 1..100 = 95
	if s.OriginalP95 != 95 {
		t.Errorf("expected original p95=95, got %.2f", s.OriginalP95)
	}

	// p99 of 1..100 = 99
	if s.OriginalP99 != 99 {
		t.Errorf("expected original p99=99, got %.2f", s.OriginalP99)
	}
}

func TestDigestStats_PrintSummary(t *testing.T) {
	ds := NewDigestStats()

	ds.Record(&CompareResult{
		Query:          "SELECT * FROM users WHERE id = 1",
		Match:          true,
		OriginalTimeMs: 2.0,
		ReplayTimeMs:   3.0,
	})

	output := ds.PrintSummary()

	if !strings.Contains(output, "Query Digest Summary") {
		t.Error("expected summary header in output")
	}
	if !strings.Contains(output, "select * from users where id = ?") {
		t.Errorf("expected normalized digest in output, got:\n%s", output)
	}
}

func TestDigestStats_WriteJSON(t *testing.T) {
	ds := NewDigestStats()

	ds.Record(&CompareResult{
		Query:          "SELECT 1",
		Match:          true,
		OriginalTimeMs: 1.0,
		ReplayTimeMs:   1.5,
	})

	var buf strings.Builder
	if err := ds.WriteJSON(&buf); err != nil {
		t.Fatal(err)
	}

	output := buf.String()
	if !strings.Contains(output, `"digest"`) {
		t.Error("expected digest field in JSON output")
	}
	if !strings.Contains(output, `"original_avg_ms"`) {
		t.Error("expected original_avg_ms field in JSON output")
	}
}
