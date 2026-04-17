package compare

import (
	"math"
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

func TestDigestStats_CappedAtMax(t *testing.T) {
	ds := NewDigestStatsWithCap(3)

	// Each table name is genuinely distinct (Digest strips numeric
	// literals but not identifier letters), so these three produce
	// three different digests: "select * from users where id = ?",
	// "select * from orders where id = ?", "select * from products where id = ?".
	tables := []string{"users", "orders", "products"}
	for _, tbl := range tables {
		ds.Record(&CompareResult{
			Query:          "SELECT * FROM " + tbl + " WHERE id = 1",
			Match:          true,
			OriginalTimeMs: 1.0,
			ReplayTimeMs:   1.0,
		})
	}
	if got := ds.Overflow(); got != 0 {
		t.Errorf("expected no overflow before cap, got %d", got)
	}
	summaries := ds.Summaries()
	if len(summaries) != 3 {
		t.Errorf("expected 3 digests tracked, got %d", len(summaries))
	}

	// 4th distinct digest → dropped.
	ds.Record(&CompareResult{
		Query:          "SELECT * FROM sessions WHERE id = 1",
		Match:          true,
		OriginalTimeMs: 1.0,
	})
	if got := ds.Overflow(); got != 1 {
		t.Errorf("expected overflow=1 for 4th digest, got %d", got)
	}
	summaries = ds.Summaries()
	if len(summaries) != 3 {
		t.Errorf("expected digest count still 3 after overflow, got %d", len(summaries))
	}

	// Same digest as an already-tracked one → accepted (updates counts).
	ds.Record(&CompareResult{
		Query:          "SELECT * FROM users WHERE id = 99",
		Match:          true,
		OriginalTimeMs: 2.0,
	})
	if got := ds.Overflow(); got != 1 {
		t.Errorf("overflow shouldn't increment for existing digest, got %d", got)
	}
	after := ds.Summaries()
	for _, s := range after {
		if s.SampleQuery == "SELECT * FROM users WHERE id = 1" {
			if s.Count != 2 {
				t.Errorf("expected users digest count=2, got %d", s.Count)
			}
			return
		}
	}
	t.Error("expected users digest to still be tracked")
}

func TestDigestStats_DefaultCap(t *testing.T) {
	// NewDigestStats() (no arg) uses the default cap.
	ds := NewDigestStats()
	if ds.maxDigests != DefaultMaxUniqueDigests {
		t.Errorf("expected default cap %d, got %d", DefaultMaxUniqueDigests, ds.maxDigests)
	}
}

func TestDigestStats_NegativeCapUsesDefault(t *testing.T) {
	ds := NewDigestStatsWithCap(-5)
	if ds.maxDigests != DefaultMaxUniqueDigests {
		t.Errorf("expected negative cap to fall back to default, got %d", ds.maxDigests)
	}
}

func TestDigestStats_ReservoirBounded(t *testing.T) {
	ds := NewDigestStats()

	// Record far more than maxReservoirSize to ensure the reservoir caps.
	const n = maxReservoirSize * 3
	for i := 1; i <= n; i++ {
		ds.Record(&CompareResult{
			Query:          "SELECT 1",
			Match:          true,
			OriginalTimeMs: float64(i),
			ReplayTimeMs:   float64(i) * 1.1,
		})
	}

	ds.mu.Lock()
	entry := ds.digests["select ?"]
	ds.mu.Unlock()

	if entry == nil {
		t.Fatal("expected digest entry to exist")
	}
	if len(entry.OriginalTimes) != maxReservoirSize {
		t.Errorf("expected reservoir capped at %d, got %d", maxReservoirSize, len(entry.OriginalTimes))
	}
	if entry.OriginalCount != n {
		t.Errorf("expected exact count %d, got %d", n, entry.OriginalCount)
	}

	// Avg must be exact — computed from running sum, not the reservoir.
	// avg of 1..n = (n+1)/2
	summaries := ds.Summaries()
	expectedAvg := float64(n+1) / 2
	if math.Abs(summaries[0].OriginalAvg-expectedAvg) > 0.01 {
		t.Errorf("expected exact avg %.2f, got %.2f", expectedAvg, summaries[0].OriginalAvg)
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
