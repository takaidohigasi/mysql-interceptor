package compare

import (
	"strconv"
	"sync"
	"time"
)

// compareResultPool amortizes the per-record allocation of
// CompareResult and its Differences slice across the comparison hot
// path. Engine.Compare pulls an entry via AcquireCompareResult; the
// caller (typically the shadow / offline replay record path) returns
// it via ReleaseCompareResult once Reporter.Record has consumed it.
//
// The Differences slice's capacity is preserved across pool round
// trips so a sustained high-divergence workload reaches steady state
// with zero per-record allocation on the slice header. The slice
// elements (Difference structs) hold strings; those still allocate
// per record because Go strings are immutable, but their headers
// reuse the slice's existing backing array slots.
//
// Pool entries can be reclaimed by GC at any time — that's fine
// because New rebuilds a zero-value CompareResult on next Get.
var compareResultPool = sync.Pool{
	New: func() interface{} { return &CompareResult{} },
}

// AcquireCompareResult returns a zeroed *CompareResult ready for
// population. Pair every call with ReleaseCompareResult once the
// result is no longer in use, or the pool wins evaporate (the
// allocation just shifts from the GC to the pool's New func).
//
// Tests that construct a *CompareResult literally (`&CompareResult{...}`)
// don't need to use this helper; they also don't need to release —
// short-lived test allocations are cheap and the GC handles them.
func AcquireCompareResult() *CompareResult {
	return compareResultPool.Get().(*CompareResult)
}

// ReleaseCompareResult zeros r's fields, trims its Differences slice
// to length 0 while preserving the underlying capacity, and returns
// r to the shared pool. Callers must not reference r after this
// call — the next Acquire could surface the same pointer to a
// different goroutine.
//
// Safe to call with a nil r; returns immediately.
func ReleaseCompareResult(r *CompareResult) {
	if r == nil {
		return
	}
	r.Query = ""
	r.QueryDigest = ""
	r.SessionID = 0
	r.User = ""
	r.Timestamp = time.Time{}
	r.Match = false
	r.Ignored = false
	r.Differences = r.Differences[:0]
	r.OriginalTimeMs = 0
	r.ReplayTimeMs = 0
	r.TimeDiffMs = 0
	r.TimeDiffExceed = false
	compareResultPool.Put(r)
}

type CompareResult struct {
	Query       string `json:"query"`
	QueryDigest string `json:"query_digest"`
	SessionID   uint64 `json:"session_id"`
	// User identifies which authenticated MySQL user issued the query.
	// Populated by the engine from whatever the caller passes; in shadow
	// mode this is the user authenticated on the inbound proxy session,
	// in offline mode it's LogEntry.User from the recorded log file.
	// Empty when unknown (zero value, omitted from JSON).
	User      string    `json:"user,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Match     bool      `json:"match"`
	// Ignored is true if the query matched an ignore pattern in
	// comparison.ignore_queries. Ignored results are still recorded (so
	// operators can audit them) but don't count toward the diff total.
	Ignored        bool         `json:"ignored,omitempty"`
	Differences    []Difference `json:"differences,omitempty"`
	OriginalTimeMs float64      `json:"original_time_ms"`
	ReplayTimeMs   float64      `json:"replay_time_ms"`
	TimeDiffMs     float64      `json:"time_diff_ms"`
	TimeDiffExceed bool         `json:"time_diff_exceeded"`
}

// appendJSON appends the JSON encoding of r (followed by a newline,
// matching json.Encoder.Encode) to buf. Hand-rolled to skip the
// reflection + interface dispatch in encoding/json on the hot path.
// Output matches json.Encoder with SetEscapeHTML(false); see
// jsonenc.go for the helpers that replicate the formatting rules.
func (r *CompareResult) appendJSON(buf []byte) []byte {
	buf = append(buf, '{')
	buf = append(buf, `"query":`...)
	buf = appendJSONString(buf, r.Query)
	buf = append(buf, `,"query_digest":`...)
	buf = appendJSONString(buf, r.QueryDigest)
	buf = append(buf, `,"session_id":`...)
	buf = strconv.AppendUint(buf, r.SessionID, 10)
	if r.User != "" {
		buf = append(buf, `,"user":`...)
		buf = appendJSONString(buf, r.User)
	}
	buf = append(buf, `,"timestamp":`...)
	buf = append(buf, '"')
	// time.Time MarshalJSON uses RFC3339Nano with quotes; replicate that.
	buf = r.Timestamp.AppendFormat(buf, time.RFC3339Nano)
	buf = append(buf, '"')
	buf = append(buf, `,"match":`...)
	buf = appendJSONBool(buf, r.Match)
	if r.Ignored {
		buf = append(buf, `,"ignored":true`...)
	}
	if len(r.Differences) > 0 {
		buf = append(buf, `,"differences":[`...)
		for i := range r.Differences {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = r.Differences[i].appendJSON(buf)
		}
		buf = append(buf, ']')
	}
	buf = append(buf, `,"original_time_ms":`...)
	buf = appendJSONFloat(buf, r.OriginalTimeMs)
	buf = append(buf, `,"replay_time_ms":`...)
	buf = appendJSONFloat(buf, r.ReplayTimeMs)
	buf = append(buf, `,"time_diff_ms":`...)
	buf = appendJSONFloat(buf, r.TimeDiffMs)
	buf = append(buf, `,"time_diff_exceeded":`...)
	buf = appendJSONBool(buf, r.TimeDiffExceed)
	buf = append(buf, '}', '\n')
	return buf
}

type Difference struct {
	Type     string `json:"type"`
	Row      int    `json:"row,omitempty"`
	Column   string `json:"column,omitempty"`
	Original string `json:"original"`
	Replay   string `json:"replay"`
}

// appendJSON appends the JSON encoding of d (no trailing newline —
// Differences are array elements). See CompareResult.appendJSON.
func (d *Difference) appendJSON(buf []byte) []byte {
	buf = append(buf, '{')
	buf = append(buf, `"type":`...)
	buf = appendJSONString(buf, d.Type)
	if d.Row != 0 {
		buf = append(buf, `,"row":`...)
		buf = strconv.AppendInt(buf, int64(d.Row), 10)
	}
	if d.Column != "" {
		buf = append(buf, `,"column":`...)
		buf = appendJSONString(buf, d.Column)
	}
	buf = append(buf, `,"original":`...)
	buf = appendJSONString(buf, d.Original)
	buf = append(buf, `,"replay":`...)
	buf = appendJSONString(buf, d.Replay)
	buf = append(buf, '}')
	return buf
}

type CapturedResult struct {
	Columns      []string
	Rows         [][]string
	AffectedRows uint64
	Error        string
	Duration     time.Duration
}
