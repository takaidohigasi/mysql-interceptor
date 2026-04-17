package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthz(t *testing.T) {
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	handleHealthz(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
	if got := rr.Body.String(); got != "ok\n" {
		t.Errorf("expected body 'ok\\n', got %q", got)
	}
}

func TestMetricsEndpointReturnsCounters(t *testing.T) {
	// Reset counters for test isolation; the global is a package singleton.
	Global.ActiveSessions.Store(3)
	Global.TotalSessions.Store(42)
	Global.QueriesHandled.Store(100)
	defer func() {
		Global.ActiveSessions.Store(0)
		Global.TotalSessions.Store(0)
		Global.QueriesHandled.Store(0)
	}()

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	handleMetrics(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected application/json, got %q", ct)
	}

	var snap map[string]int64
	if err := json.Unmarshal(rr.Body.Bytes(), &snap); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}
	if snap["active_sessions"] != 3 {
		t.Errorf("expected active_sessions=3, got %d", snap["active_sessions"])
	}
	if snap["total_sessions"] != 42 {
		t.Errorf("expected total_sessions=42, got %d", snap["total_sessions"])
	}
	if snap["queries_handled"] != 100 {
		t.Errorf("expected queries_handled=100, got %d", snap["queries_handled"])
	}
}

func TestNewServerNilForEmptyAddr(t *testing.T) {
	if s := NewServer(""); s != nil {
		t.Errorf("expected nil server for empty addr, got %v", s)
	}
	// Nil-safe methods
	var s *Server
	s.Start()
	s.Shutdown()
}
