package compare

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
)

type DigestStats struct {
	mu      sync.Mutex
	digests map[string]*DigestEntry
}

type DigestEntry struct {
	Digest        string    `json:"digest"`
	SampleQuery   string    `json:"sample_query"`
	Count         int       `json:"count"`
	MatchCount    int       `json:"match_count"`
	DiffCount     int       `json:"diff_count"`
	ErrorCount    int       `json:"error_count"`
	OriginalTimes []float64 `json:"-"`
	ReplayTimes   []float64 `json:"-"`
}

type DigestSummary struct {
	Digest       string  `json:"digest"`
	SampleQuery  string  `json:"sample_query"`
	Count        int     `json:"count"`
	MatchCount   int     `json:"match_count"`
	DiffCount    int     `json:"diff_count"`
	ErrorCount   int     `json:"error_count"`
	OriginalAvg  float64 `json:"original_avg_ms"`
	OriginalP95  float64 `json:"original_p95_ms"`
	OriginalP99  float64 `json:"original_p99_ms"`
	ReplayAvg    float64 `json:"replay_avg_ms"`
	ReplayP95    float64 `json:"replay_p95_ms"`
	ReplayP99    float64 `json:"replay_p99_ms"`
	OverheadAvg  float64 `json:"overhead_avg_ms"`
	OverheadP95  float64 `json:"overhead_p95_ms"`
	OverheadP99  float64 `json:"overhead_p99_ms"`
}

func NewDigestStats() *DigestStats {
	return &DigestStats{
		digests: make(map[string]*DigestEntry),
	}
}

func (ds *DigestStats) Record(result *CompareResult) {
	digest := Digest(result.Query)

	ds.mu.Lock()
	defer ds.mu.Unlock()

	entry, ok := ds.digests[digest]
	if !ok {
		entry = &DigestEntry{
			Digest:      digest,
			SampleQuery: result.Query,
		}
		ds.digests[digest] = entry
	}

	entry.Count++
	if result.Match {
		entry.MatchCount++
	} else {
		entry.DiffCount++
	}

	for _, d := range result.Differences {
		if d.Type == "error" {
			entry.ErrorCount++
			break
		}
	}

	entry.OriginalTimes = append(entry.OriginalTimes, result.OriginalTimeMs)
	entry.ReplayTimes = append(entry.ReplayTimes, result.ReplayTimeMs)
}

func (ds *DigestStats) Summaries() []DigestSummary {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	summaries := make([]DigestSummary, 0, len(ds.digests))
	for _, entry := range ds.digests {
		s := DigestSummary{
			Digest:      entry.Digest,
			SampleQuery: entry.SampleQuery,
			Count:       entry.Count,
			MatchCount:  entry.MatchCount,
			DiffCount:   entry.DiffCount,
			ErrorCount:  entry.ErrorCount,
		}

		if len(entry.OriginalTimes) > 0 {
			s.OriginalAvg = mean64(entry.OriginalTimes)
			s.OriginalP95 = percentile64(entry.OriginalTimes, 95)
			s.OriginalP99 = percentile64(entry.OriginalTimes, 99)
		}
		if len(entry.ReplayTimes) > 0 {
			s.ReplayAvg = mean64(entry.ReplayTimes)
			s.ReplayP95 = percentile64(entry.ReplayTimes, 95)
			s.ReplayP99 = percentile64(entry.ReplayTimes, 99)
		}

		s.OverheadAvg = s.ReplayAvg - s.OriginalAvg
		s.OverheadP95 = s.ReplayP95 - s.OriginalP95
		s.OverheadP99 = s.ReplayP99 - s.OriginalP99

		summaries = append(summaries, s)
	}

	// Sort by count descending (most frequent digests first)
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Count > summaries[j].Count
	})

	return summaries
}

func (ds *DigestStats) PrintSummary() string {
	summaries := ds.Summaries()
	if len(summaries) == 0 {
		return "No digest stats collected"
	}

	var result string
	result += fmt.Sprintf("\n=== Query Digest Summary (%d unique digests) ===\n\n", len(summaries))
	result += fmt.Sprintf("%-60s %6s %6s %6s | %10s %10s %10s | %10s %10s %10s\n",
		"Digest", "Count", "Match", "Diff",
		"Orig Avg", "Orig P95", "Orig P99",
		"Rply Avg", "Rply P95", "Rply P99")
	result += fmt.Sprintf("%s\n", repeat("-", 160))

	for _, s := range summaries {
		digest := s.Digest
		if len(digest) > 58 {
			digest = digest[:55] + "..."
		}
		result += fmt.Sprintf("%-60s %6d %6d %6d | %8.2fms %8.2fms %8.2fms | %8.2fms %8.2fms %8.2fms\n",
			digest, s.Count, s.MatchCount, s.DiffCount,
			s.OriginalAvg, s.OriginalP95, s.OriginalP99,
			s.ReplayAvg, s.ReplayP95, s.ReplayP99)
	}

	return result
}

func (ds *DigestStats) WriteJSON(w io.Writer) error {
	summaries := ds.Summaries()
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for _, s := range summaries {
		if err := enc.Encode(s); err != nil {
			return err
		}
	}
	return nil
}

func mean64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return round2(sum / float64(len(values)))
}

func percentile64(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return round2(sorted[idx])
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

func repeat(s string, n int) string {
	var b []byte
	for i := 0; i < n; i++ {
		b = append(b, s...)
	}
	return string(b)
}
