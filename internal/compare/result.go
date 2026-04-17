package compare

import "time"

type CompareResult struct {
	Query          string       `json:"query"`
	SessionID      uint64       `json:"session_id"`
	Timestamp      time.Time    `json:"timestamp"`
	Match          bool         `json:"match"`
	Differences    []Difference `json:"differences,omitempty"`
	OriginalTimeMs float64      `json:"original_time_ms"`
	ReplayTimeMs   float64      `json:"replay_time_ms"`
	TimeDiffMs     float64      `json:"time_diff_ms"`
	TimeDiffExceed bool         `json:"time_diff_exceeded"`
}

type Difference struct {
	Type     string `json:"type"`
	Row      int    `json:"row,omitempty"`
	Column   string `json:"column,omitempty"`
	Original string `json:"original"`
	Replay   string `json:"replay"`
}

type CapturedResult struct {
	Columns      []string
	Rows         [][]string
	AffectedRows uint64
	Error        string
	Duration     time.Duration
}
