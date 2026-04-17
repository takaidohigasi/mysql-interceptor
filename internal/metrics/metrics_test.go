package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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

func setupTestCounters(t *testing.T) {
	t.Helper()
	Global.ActiveSessions.Store(3)
	Global.TotalSessions.Store(42)
	Global.QueriesHandled.Store(100)
	Global.ComparisonsIgnored.Store(7)
	t.Cleanup(func() {
		Global.ActiveSessions.Store(0)
		Global.TotalSessions.Store(0)
		Global.QueriesHandled.Store(0)
		Global.ComparisonsIgnored.Store(0)
	})
}

func TestPrometheusEndpoint(t *testing.T) {
	setupTestCounters(t)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	handlePrometheus(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	ct := rr.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("expected text/plain..., got %q", ct)
	}

	body := rr.Body.String()

	// Each metric should have HELP, TYPE, and a value line.
	for _, want := range []string{
		"# HELP active_sessions",
		"# TYPE active_sessions gauge",
		"active_sessions 3",
		"# HELP total_sessions",
		"# TYPE total_sessions counter",
		"total_sessions 42",
		"queries_handled 100",
		"comparisons_ignored 7",
		"# TYPE heap_alloc_bytes gauge",
		"# TYPE num_goroutines gauge",
		"# TYPE gc_cycles_total counter",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("expected output to contain %q, body was:\n%s", want, body)
		}
	}

	// Sanity: the output should be valid Prometheus exposition format —
	// every non-blank, non-comment line should be "name value".
	for i, line := range strings.Split(body, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Errorf("line %d is not valid exposition format: %q", i, line)
		}
	}
}

func TestJSONMetricsEndpoint(t *testing.T) {
	setupTestCounters(t)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics.json", nil)
	handleJSONMetrics(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected application/json, got %q", ct)
	}

	var snap map[string]float64
	if err := json.Unmarshal(rr.Body.Bytes(), &snap); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}
	if snap["active_sessions"] != 3 {
		t.Errorf("expected active_sessions=3, got %v", snap["active_sessions"])
	}
	if snap["comparisons_ignored"] != 7 {
		t.Errorf("expected comparisons_ignored=7, got %v", snap["comparisons_ignored"])
	}
	if snap["heap_alloc_bytes"] <= 0 {
		t.Errorf("expected heap_alloc_bytes > 0, got %v", snap["heap_alloc_bytes"])
	}
	if snap["num_goroutines"] < 1 {
		t.Errorf("expected num_goroutines >= 1, got %v", snap["num_goroutines"])
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
