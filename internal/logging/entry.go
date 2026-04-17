package logging

import "time"

type LogEntry struct {
	Timestamp    time.Time     `json:"timestamp"`
	SessionID    uint64        `json:"session_id"`
	SourceIP     string        `json:"source_ip"`
	User         string        `json:"user"`
	Database     string        `json:"database"`
	QueryType    string        `json:"query_type"`
	Query        string        `json:"query"`
	Args         []interface{} `json:"args,omitempty"`
	ResponseTime float64       `json:"response_time_ms"`
	RowsAffected uint64        `json:"rows_affected"`
	RowsReturned int           `json:"rows_returned,omitempty"`
	Error        string        `json:"error,omitempty"`
}
