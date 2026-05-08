package compare

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"
	"time"
)

// TestAppendJSON_CompareResult_MatchesEncodingJSON verifies that the
// hand-rolled CompareResult.appendJSON output is byte-identical to
// what json.Encoder produces with SetEscapeHTML(false). If this test
// breaks after a schema change, the appendJSON method needs to be
// updated to match — we maintain wire compatibility on purpose so
// downstream consumers (operator tails, log pipelines) don't need to
// know which encoding path produced a record.
func TestAppendJSON_CompareResult_MatchesEncodingJSON(t *testing.T) {
	cases := []*CompareResult{
		// Match record (no Differences, Ignored absent).
		{
			Query:          "SELECT 1",
			QueryDigest:    "select ?",
			SessionID:      1,
			Timestamp:      time.Date(2026, 5, 8, 10, 0, 0, 123456789, time.UTC),
			Match:          true,
			OriginalTimeMs: 1.5,
			ReplayTimeMs:   1.6,
			TimeDiffMs:     0.1,
			TimeDiffExceed: false,
		},
		// Diff record with multiple Differences and User attached.
		{
			Query:       "SELECT * FROM users WHERE id = ?",
			QueryDigest: "select * from users where id = ?",
			SessionID:   42,
			User:        "app_user",
			Timestamp:   time.Date(2026, 5, 8, 10, 1, 0, 0, time.UTC),
			Match:       false,
			Differences: []Difference{
				{Type: "cell_value", Row: 0, Column: "iv_cert", Original: "abc\"def\\ghi", Replay: "xyz"},
				{Type: "cell_value", Row: 5, Column: "name", Original: "line1\nline2", Replay: "tab\there"},
				{Type: "error", Original: "<html>&malformed", Replay: ""},
			},
			OriginalTimeMs: 12.5,
			ReplayTimeMs:   25.0,
			TimeDiffMs:     12.5,
			TimeDiffExceed: true,
		},
		// Ignored=true.
		{
			Query:          "/* heartbeat */ SELECT 1",
			QueryDigest:    "select ?",
			SessionID:      7,
			Timestamp:      time.Date(2026, 5, 8, 10, 2, 0, 0, time.UTC),
			Match:          true,
			Ignored:        true,
			OriginalTimeMs: 0,
			ReplayTimeMs:   0,
		},
		// Empty / zero values everywhere — make sure omitempty
		// behavior matches encoding/json.
		{},
		// Control characters in user-supplied strings.
		{
			Query:       "select '\x01\x02\x03'",
			QueryDigest: "x",
			Timestamp:   time.Date(2026, 5, 8, 10, 3, 0, 0, time.UTC),
			Match:       true,
			Differences: []Difference{
				{Type: "cell_value", Original: "\x00null\x7fdel", Replay: "ok"},
			},
		},
		// UTF-8 + characters encoding/json with SetEscapeHTML(false)
		// passes through (HTML chars, U+2028, U+2029).
		{
			Query:       "<select> & 'a'     unicode 日本語",
			QueryDigest: "x",
			Timestamp:   time.Date(2026, 5, 8, 10, 4, 0, 0, time.UTC),
			Match:       false,
			Differences: []Difference{
				{Type: "cell_value", Original: "<>&", Replay: "日本語"},
			},
		},
	}

	for i, c := range cases {
		got := string(c.appendJSON(nil))
		want := encodeWithEncodingJSON(t, c)
		if got != want {
			t.Errorf("case %d: bytes differ\n got: %q\nwant: %q", i, got, want)
		}
	}
}

// TestAppendJSON_HeartbeatRecord_MatchesEncodingJSON pins HB record
// wire-format compat in the same way.
func TestAppendJSON_HeartbeatRecord_MatchesEncodingJSON(t *testing.T) {
	cases := []*HeartbeatRecord{
		{
			Type:            "heartbeat",
			Timestamp:       "2026-05-08T10:00:00.000Z",
			WindowSeconds:   60,
			WindowTotal:     1000,
			WindowMatched:   950,
			WindowDiffered:  40,
			WindowIgnored:   10,
			CumulativeTotal: 1000000,
			CumulativeDiff:  1234,
		},
		// Zero values.
		{Type: "heartbeat", Timestamp: ""},
	}
	for i, h := range cases {
		got := string(h.appendJSON(nil))
		want := encodeWithEncodingJSON(t, h)
		if got != want {
			t.Errorf("case %d: bytes differ\n got: %q\nwant: %q", i, got, want)
		}
	}
}

// TestAppendJSONString_EdgeCases exercises the string escaper across
// the byte-by-byte boundary cases that are easiest to get wrong.
func TestAppendJSONString_EdgeCases(t *testing.T) {
	for _, s := range []string{
		"",
		"plain ASCII",
		"\"quoted\"",
		"back\\slash",
		"\b\f\n\r\t",
		"\x00\x01\x02\x03\x04\x05\x06\x07",
		"\x0e\x0f\x10\x11\x1e\x1f",
		"\x7f", // DEL — encoding/json passes through; we should too
		"日本語",
		"emoji 🚀",
		"<html>&  ",
		// Mixed: escapable + non-escapable interleaved.
		"a\"b\nc\\d\te",
	} {
		got := string(appendJSONString(nil, s))
		want := encodeStringWithEncodingJSON(t, s)
		if got != want {
			t.Errorf("escape mismatch for %q\n got: %s\nwant: %s", s, got, want)
		}
	}
}

// TestAppendJSONFloat_MatchesEncodingJSON exercises the float
// formatter across the value ranges we actually emit (ms timing
// values) plus the edge cases that switch encoding/json's format
// character. Skips NaN/Inf — those produce errors in encoding/json
// and our encoder emits "null".
func TestAppendJSONFloat_MatchesEncodingJSON(t *testing.T) {
	for _, f := range []float64{
		0, 0.001, 0.1, 1, 1.5, 12.5, 100, 1000, 60000,
		-0.5, -100, -1.234567890,
		1e-5, 1e-7, 9.999e-7, 1e-6, // boundary around the 'e' threshold
		1e20, 1e21, 9.999e20,
		math.Pi, math.E,
	} {
		got := string(appendJSONFloat(nil, f))
		want := encodeFloatWithEncodingJSON(t, f)
		if got != want {
			t.Errorf("float %g: got %s, want %s", f, got, want)
		}
	}
}

// encodeWithEncodingJSON marshals v through json.Encoder with
// SetEscapeHTML(false) and returns the bytes verbatim (including the
// trailing newline json.Encoder appends). Used as the oracle for
// hand-rolled output.
func encodeWithEncodingJSON(t *testing.T, v any) string {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		t.Fatalf("encoding/json marshal: %v", err)
	}
	return buf.String()
}

// encodeStringWithEncodingJSON returns just the JSON-escaped form of
// s (with surrounding quotes). Implementation: marshal a struct with
// one string field and slice out the value portion. Cleaner than
// peeling the trailing newline off Encode().
func encodeStringWithEncodingJSON(t *testing.T, s string) string {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Marshal string %q: %v", s, err)
	}
	// json.Marshal does NOT escape HTML chars by default... actually
	// json.Marshal escapes <, >, & in strings but json.Encoder with
	// SetEscapeHTML(false) does not. Use Encoder for matching.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		t.Fatalf("Encode string %q: %v", s, err)
	}
	out := buf.String()
	// Strip trailing newline json.Encoder appends.
	out = out[:len(out)-1]
	_ = b
	return out
}

// encodeFloatWithEncodingJSON returns the JSON-encoded form of f
// (without trailing newline).
func encodeFloatWithEncodingJSON(t *testing.T, f float64) string {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(f); err != nil {
		t.Fatalf("Encode float %g: %v", f, err)
	}
	out := buf.String()
	return out[:len(out)-1]
}
