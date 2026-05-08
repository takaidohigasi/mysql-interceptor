package compare

import (
	"testing"
	"time"
)

// TestReleaseCompareResult_Resets confirms that releasing then
// re-acquiring returns a fully-zeroed CompareResult — every field
// must be reset, including the slice length on Differences. If a
// field is forgotten in ReleaseCompareResult, a future Acquire will
// surface stale data and the diff report will leak previous-record
// state. This is the load-bearing invariant of the pool.
func TestReleaseCompareResult_Resets(t *testing.T) {
	r := AcquireCompareResult()
	r.Query = "SELECT 1"
	r.QueryDigest = "select ?"
	r.SessionID = 99
	r.User = "alice"
	r.Timestamp = time.Now()
	r.Match = false
	r.Ignored = true
	r.Differences = append(r.Differences,
		Difference{Type: "cell_value", Row: 1, Column: "x", Original: "a", Replay: "b"},
		Difference{Type: "error", Original: "broken", Replay: ""},
	)
	r.OriginalTimeMs = 12.5
	r.ReplayTimeMs = 13.0
	r.TimeDiffMs = 0.5
	r.TimeDiffExceed = true

	prevDiffCap := cap(r.Differences)
	ReleaseCompareResult(r)

	if r.Query != "" || r.QueryDigest != "" || r.SessionID != 0 || r.User != "" {
		t.Errorf("string/int fields not reset: %+v", r)
	}
	if !r.Timestamp.IsZero() {
		t.Errorf("Timestamp not reset: %v", r.Timestamp)
	}
	if r.Match || r.Ignored || r.TimeDiffExceed {
		t.Errorf("bool fields not reset: %+v", r)
	}
	if r.OriginalTimeMs != 0 || r.ReplayTimeMs != 0 || r.TimeDiffMs != 0 {
		t.Errorf("float fields not reset: %+v", r)
	}
	if len(r.Differences) != 0 {
		t.Errorf("Differences length not reset: %d", len(r.Differences))
	}
	if cap(r.Differences) != prevDiffCap {
		t.Errorf("Differences capacity not preserved: was %d, now %d (pool benefit lost)", prevDiffCap, cap(r.Differences))
	}
}

// TestReleaseCompareResult_NilSafe protects callers that defer
// release in early-return paths where the acquire was conditional.
func TestReleaseCompareResult_NilSafe(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("ReleaseCompareResult(nil) panicked: %v", r)
		}
	}()
	ReleaseCompareResult(nil)
}
