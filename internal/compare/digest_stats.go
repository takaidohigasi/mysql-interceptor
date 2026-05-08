package compare

import (
	"encoding/json"
	"fmt"
	"hash/maphash"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/takaidohigasi/mysql-interceptor/internal/metrics"
)

// maxReservoirSize bounds the per-digest timing sample size. With a
// uniform reservoir of this size, p99 estimates are backed by ~100
// samples — enough for stable tail-latency numbers — while memory per
// digest stays O(10k floats) regardless of how long the process runs.
const maxReservoirSize = 10000

// DefaultMaxUniqueDigests is the fallback cap when NewDigestStats is
// called with 0 (typically from tests that don't go through config).
// Matches the default in config.ComparisonConfig.
const DefaultMaxUniqueDigests = 10000

// digestNumShards is the number of independent shards over which the
// digest map (and its mutex) are partitioned. Using a power of two
// lets shardFor mask the hash with digestShardMask instead of taking
// a modulo. 32 is a sweet spot: enough to absorb contention from
// dozens of producers per host without ballooning per-shard overhead
// (each shard adds 64 B of mutex + map header, ~2 KB total).
const digestNumShards = 32
const digestShardMask = digestNumShards - 1

// DigestStats accumulates per-query-digest counters and timing
// samples. It is safe for concurrent use from many goroutines.
//
// The digest map is sharded into digestNumShards independent buckets,
// each protected by its own mutex. A digest always lands on the same
// shard (via maphash.String), so per-digest updates never contend
// with updates to other digests on different shards. The global
// uniqueness cap is enforced via a single atomic counter so the
// "max_unique_digests" config knob retains its global meaning.
type DigestStats struct {
	seed           maphash.Seed
	shards         [digestNumShards]digestShard
	maxDigests     int          // global cap across all shards
	digestCount    atomic.Int64 // current global count of unique digests
	overflow       atomic.Int64 // count of new digests dropped due to cap
	overflowWarned atomic.Bool  // one-time log on first overflow
}

type digestShard struct {
	mu      sync.Mutex              // 8 bytes
	digests map[string]*DigestEntry // 8 bytes (map header is one pointer)
	// Pad each shard out to exactly 64 bytes so adjacent shards land
	// on different AMD64 / arm64 cache lines and never false-share.
	// Without padding to a full cache-line stride, shard[0]'s tail
	// would share a line with shard[1]'s head and concurrent updates
	// would invalidate each other. 8 + 8 + 48 = 64 = one cache line.
	_ [48]byte
}

type DigestEntry struct {
	Digest      string `json:"digest"`
	SampleQuery string `json:"sample_query"`
	Count       int    `json:"count"`
	MatchCount  int    `json:"match_count"`
	DiffCount   int    `json:"diff_count"`
	ErrorCount  int    `json:"error_count"`

	// Exact running sums for accurate mean regardless of reservoir size.
	// These are internal-only: the public JSON output uses DigestSummary
	// which exposes avg/p95/p99 derived from these.
	OriginalSum   float64 `json:"-"`
	OriginalCount int     `json:"-"`
	ReplaySum     float64 `json:"-"`
	ReplayCount   int     `json:"-"`

	// Bounded reservoirs for percentile estimation. Up to maxReservoirSize
	// floats each — never marshaled directly.
	OriginalTimes []float64 `json:"-"`
	ReplayTimes   []float64 `json:"-"`
}

type DigestSummary struct {
	Digest      string  `json:"digest"`
	SampleQuery string  `json:"sample_query"`
	Count       int     `json:"count"`
	MatchCount  int     `json:"match_count"`
	DiffCount   int     `json:"diff_count"`
	ErrorCount  int     `json:"error_count"`
	OriginalAvg float64 `json:"original_avg_ms"`
	OriginalP95 float64 `json:"original_p95_ms"`
	OriginalP99 float64 `json:"original_p99_ms"`
	ReplayAvg   float64 `json:"replay_avg_ms"`
	ReplayP95   float64 `json:"replay_p95_ms"`
	ReplayP99   float64 `json:"replay_p99_ms"`
	OverheadAvg float64 `json:"overhead_avg_ms"`
	OverheadP95 float64 `json:"overhead_p95_ms"`
	OverheadP99 float64 `json:"overhead_p99_ms"`
}

func NewDigestStats() *DigestStats {
	return NewDigestStatsWithCap(DefaultMaxUniqueDigests)
}

// NewDigestStatsWithCap constructs a DigestStats that tracks at most
// maxDigests unique query digests across all shards combined. Once
// reached, new digests are dropped (counted via Overflow()) but
// existing digests keep updating. A cap of 0 or negative is treated
// as DefaultMaxUniqueDigests.
func NewDigestStatsWithCap(maxDigests int) *DigestStats {
	if maxDigests <= 0 {
		maxDigests = DefaultMaxUniqueDigests
	}
	ds := &DigestStats{
		seed:       maphash.MakeSeed(),
		maxDigests: maxDigests,
	}
	for i := range ds.shards {
		ds.shards[i].digests = make(map[string]*DigestEntry)
	}
	return ds
}

// shardFor returns the shard owning a given digest. maphash.String is
// used so the distribution is hash-randomized per process (the seed
// is per-DigestStats), preventing pathological patterns where a
// caller could deliberately concentrate distinct digests on one
// shard.
func (ds *DigestStats) shardFor(digest string) *digestShard {
	h := maphash.String(ds.seed, digest)
	return &ds.shards[h&digestShardMask]
}

func (ds *DigestStats) Record(result *CompareResult) {
	// Reuse the digest already computed by Engine.Compare instead of
	// recomputing Digest(result.Query) here. The compute is non-trivial
	// (full SQL normalization pass) and runs per record on the hot path;
	// duplicating it costs ~1 µs/record at high QPS for no benefit.
	digest := result.QueryDigest
	if digest == "" {
		// Defensive fallback for callers (mostly tests) that bypass
		// Engine.Compare and construct CompareResult directly.
		digest = Digest(result.Query)
	}

	sh := ds.shardFor(digest)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	entry, ok := sh.digests[digest]
	if !ok {
		// Reserve a slot from the global cap before inserting. CAS loop
		// because multiple goroutines may hit different shards
		// concurrently and race for the last few slots; whoever loses
		// re-reads the new value and re-checks against the cap.
		for {
			cur := ds.digestCount.Load()
			if cur >= int64(ds.maxDigests) {
				ds.overflow.Add(1)
				metrics.Global.ComparisonsDigestOver.Add(1)
				if ds.overflowWarned.CompareAndSwap(false, true) {
					slog.Warn("digest stats cap reached; new query patterns are being dropped",
						"cap", ds.maxDigests,
						"hint", "tune comparison.max_unique_digests up or investigate high-cardinality query patterns")
				}
				return
			}
			if ds.digestCount.CompareAndSwap(cur, cur+1) {
				metrics.Global.ComparisonsDigestCount.Store(cur + 1)
				break
			}
		}
		entry = &DigestEntry{
			Digest:      digest,
			SampleQuery: result.Query,
		}
		sh.digests[digest] = entry
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

	// Running sums stay exact.
	entry.OriginalSum += result.OriginalTimeMs
	entry.OriginalCount++
	entry.ReplaySum += result.ReplayTimeMs
	entry.ReplayCount++

	// Bounded reservoir for percentile estimates.
	entry.OriginalTimes = reservoirAdd(entry.OriginalTimes, result.OriginalTimeMs, entry.OriginalCount)
	entry.ReplayTimes = reservoirAdd(entry.ReplayTimes, result.ReplayTimeMs, entry.ReplayCount)
}

// reservoirAdd implements Vitter's Algorithm R: the first maxReservoirSize
// samples fill the reservoir; subsequent samples replace a random slot with
// probability maxReservoirSize/n, preserving a uniform sample of all
// observations seen so far. n is the 1-indexed count of the current value.
func reservoirAdd(reservoir []float64, v float64, n int) []float64 {
	if len(reservoir) < maxReservoirSize {
		return append(reservoir, v)
	}
	// With probability k/n, replace a random slot.
	j := rand.IntN(n)
	if j < maxReservoirSize {
		reservoir[j] = v
	}
	return reservoir
}

// Overflow returns the number of new digests dropped because the cap
// was reached. Mostly useful for tests; operators should read the
// comparisons_digest_overflow metric instead.
func (ds *DigestStats) Overflow() int64 {
	return ds.overflow.Load()
}

// Summaries returns a snapshot of every tracked digest, sorted by
// count descending. Each shard is locked individually while its
// entries are summarized; no two shard locks are ever held
// simultaneously, so this can run concurrently with Record on
// different shards without contention beyond the per-shard lock.
func (ds *DigestStats) Summaries() []DigestSummary {
	// Best-effort capacity hint to avoid grow-doubling. The atomic
	// load is racy with concurrent inserts; one or two off doesn't
	// matter for an append-grow heuristic.
	summaries := make([]DigestSummary, 0, ds.digestCount.Load())
	for i := range ds.shards {
		sh := &ds.shards[i]
		sh.mu.Lock()
		for _, entry := range sh.digests {
			s := DigestSummary{
				Digest:      entry.Digest,
				SampleQuery: entry.SampleQuery,
				Count:       entry.Count,
				MatchCount:  entry.MatchCount,
				DiffCount:   entry.DiffCount,
				ErrorCount:  entry.ErrorCount,
			}
			if entry.OriginalCount > 0 {
				s.OriginalAvg = round2(entry.OriginalSum / float64(entry.OriginalCount))
				s.OriginalP95 = percentile64(entry.OriginalTimes, 95)
				s.OriginalP99 = percentile64(entry.OriginalTimes, 99)
			}
			if entry.ReplayCount > 0 {
				s.ReplayAvg = round2(entry.ReplaySum / float64(entry.ReplayCount))
				s.ReplayP95 = percentile64(entry.ReplayTimes, 95)
				s.ReplayP99 = percentile64(entry.ReplayTimes, 99)
			}
			s.OverheadAvg = round2(s.ReplayAvg - s.OriginalAvg)
			s.OverheadP95 = round2(s.ReplayP95 - s.OriginalP95)
			s.OverheadP99 = round2(s.ReplayP99 - s.OriginalP99)
			summaries = append(summaries, s)
		}
		sh.mu.Unlock()
	}

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
