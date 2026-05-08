package compare

import (
	"strconv"
	"time"
)

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
