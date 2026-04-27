// Package metrics exposes liveness and basic counters over HTTP.
//
// It is intentionally thin: stdlib expvar for counters and a plain
// /healthz endpoint. No Prometheus dependency. Scrape /metrics for
// JSON counters (which any reasonable pipeline can reshape).
package metrics

import (
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime"
	"sort"
	"sync/atomic"
	"time"
)

// Counters holds the metrics the server exposes. Fields are pointers to
// atomic Int64 so handlers can observe them without a mutex.
type Counters struct {
	ActiveSessions            atomic.Int64
	TotalSessions             atomic.Int64
	SessionsClosedMaxLifetime atomic.Int64 // sessions closed because max_session_lifetime elapsed
	SessionsLifetimePostponed atomic.Int64 // expiry checks deferred because backend was in a transaction
	QueriesHandled            atomic.Int64
	QueryErrors               atomic.Int64
	LoggerDropped             atomic.Int64
	ShadowDropped             atomic.Int64
	ShadowSkipped             atomic.Int64
	ShadowDisabled            atomic.Int64 // sends rejected because shadow.enabled=false
	ShadowSampledOut          atomic.Int64 // sends dropped by sample_rate roll
	ShadowFilteredByCIDR      atomic.Int64 // sends rejected by CIDR allow/exclude filter
	ShadowEnabledGauge        atomic.Int64 // 0 or 1; current toggle state
	ShadowActiveSessions      atomic.Int64 // current count of pinned shadow sessions
	ShadowQueriesReplayed     atomic.Int64
	ComparisonsTotal          atomic.Int64
	ComparisonsMatched        atomic.Int64
	ComparisonsDiffered       atomic.Int64
	ComparisonsIgnored        atomic.Int64
	ComparisonsDigestOver     atomic.Int64 // new digests dropped because cap hit
	ComparisonsDigestCount    atomic.Int64 // current unique digests tracked (gauge)
}

// Global is the singleton counter set. Components increment fields on it
// directly; the HTTP server reads them for /metrics responses.
var Global = &Counters{}

// Server is an HTTP server for /healthz and /metrics.
type Server struct {
	srv *http.Server
}

// NewServer constructs (but does not start) an HTTP server on addr.
// Pass addr="" to disable metrics entirely.
func NewServer(addr string) *Server {
	if addr == "" {
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handleHealthz)
	// /metrics serves Prometheus/OpenMetrics text format — the industry
	// convention, and what Datadog's openmetrics check and Prometheus
	// scrapers expect by default.
	mux.HandleFunc("/metrics", handlePrometheus)
	// /metrics.json keeps the JSON view for human debugging or scrapers
	// that prefer structured data.
	mux.HandleFunc("/metrics.json", handleJSONMetrics)
	// expvar's default handler exposes Go runtime stats at /debug/vars
	mux.Handle("/debug/vars", expvar.Handler())

	return &Server{
		srv: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
}

// Start launches the server in a goroutine. Errors other than
// http.ErrServerClosed are logged.
func (s *Server) Start() {
	if s == nil {
		return
	}
	go func() {
		slog.Info("metrics server listening", "addr", s.srv.Addr)
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "err", err)
		}
	}()
}

// Shutdown stops the server with a short timeout.
func (s *Server) Shutdown() {
	if s == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.srv.Shutdown(ctx)
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// metric holds one gauge/counter value with the metadata needed to
// render it in Prometheus exposition format.
type metric struct {
	name  string
	typ   string // "counter" or "gauge"
	help  string
	value float64
}

// snapshot returns the current values of every exposed metric. Runtime
// gauges are read live via runtime.ReadMemStats each call.
func snapshot() []metric {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	ng := runtime.NumGoroutine()

	return []metric{
		// Sessions
		{"active_sessions", "gauge", "Current number of active client sessions", float64(Global.ActiveSessions.Load())},
		{"total_sessions", "counter", "Total client sessions accepted since start", float64(Global.TotalSessions.Load())},
		{"sessions_closed_max_lifetime", "counter", "Sessions closed because proxy.max_session_lifetime elapsed", float64(Global.SessionsClosedMaxLifetime.Load())},
		{"sessions_lifetime_postponed", "counter", "Expiry checks deferred because the backend was mid-transaction", float64(Global.SessionsLifetimePostponed.Load())},

		// Query pipeline
		{"queries_handled", "counter", "Total queries forwarded to the backend", float64(Global.QueriesHandled.Load())},
		{"query_errors", "counter", "Queries that returned an error from the backend", float64(Global.QueryErrors.Load())},

		// Logger
		{"logger_dropped", "counter", "SQL log entries dropped because the async buffer was full", float64(Global.LoggerDropped.Load())},

		// Shadow
		{"shadow_enabled", "gauge", "1 when shadow traffic is enabled, 0 when paused via config toggle", float64(Global.ShadowEnabledGauge.Load())},
		{"shadow_active_sessions", "gauge", "Current count of pinned shadow sessions (one per active primary session)", float64(Global.ShadowActiveSessions.Load())},
		{"shadow_queries_replayed", "counter", "Shadow queries successfully executed against the shadow server", float64(Global.ShadowQueriesReplayed.Load())},
		{"shadow_disabled", "counter", "Sends rejected because shadow.enabled=false", float64(Global.ShadowDisabled.Load())},
		{"shadow_sampled_out", "counter", "Sends dropped by shadow.sample_rate", float64(Global.ShadowSampledOut.Load())},
		{"shadow_filtered_by_cidr", "counter", "Sends rejected by the CIDR allow/exclude filter", float64(Global.ShadowFilteredByCIDR.Load())},
		{"shadow_skipped", "counter", "Sends skipped because the query was not read-only", float64(Global.ShadowSkipped.Load())},
		{"shadow_dropped", "counter", "Sends dropped because the queue was full or the connection timed out", float64(Global.ShadowDropped.Load())},

		// Comparisons
		{"comparisons_total", "counter", "Total comparison results recorded", float64(Global.ComparisonsTotal.Load())},
		{"comparisons_matched", "counter", "Comparisons where primary and shadow agreed", float64(Global.ComparisonsMatched.Load())},
		{"comparisons_differed", "counter", "Comparisons with a content or error divergence", float64(Global.ComparisonsDiffered.Load())},
		{"comparisons_ignored", "counter", "Comparisons that matched a configured ignore pattern", float64(Global.ComparisonsIgnored.Load())},
		{"comparisons_digest_count", "gauge", "Current number of unique query digests being tracked", float64(Global.ComparisonsDigestCount.Load())},
		{"comparisons_digest_overflow", "counter", "New digests dropped because the max_unique_digests cap was reached", float64(Global.ComparisonsDigestOver.Load())},

		// Runtime
		{"heap_alloc_bytes", "gauge", "Bytes currently allocated on the Go heap", float64(ms.HeapAlloc)},
		{"heap_inuse_bytes", "gauge", "Bytes in heap in-use spans", float64(ms.HeapInuse)},
		{"heap_idle_bytes", "gauge", "Bytes in heap idle spans (may be returned to OS)", float64(ms.HeapIdle)},
		{"heap_sys_bytes", "gauge", "Bytes of heap memory obtained from the OS", float64(ms.HeapSys)},
		{"heap_objects", "gauge", "Live heap object count", float64(ms.HeapObjects)},
		{"stack_inuse_bytes", "gauge", "Bytes in stack memory currently in use", float64(ms.StackInuse)},
		{"sys_bytes", "gauge", "Total bytes obtained from the OS for the Go runtime", float64(ms.Sys)},
		{"num_goroutines", "gauge", "Number of live goroutines", float64(ng)},
		{"gc_cycles_total", "counter", "Completed GC cycles since process start", float64(ms.NumGC)},
		{"gc_pause_ns_total", "counter", "Cumulative STW pause time in nanoseconds", float64(ms.PauseTotalNs)},
	}
}

// handlePrometheus serves the /metrics endpoint in Prometheus/OpenMetrics
// text exposition format. Compatible with Datadog's openmetrics check and
// any Prometheus scraper.
func handlePrometheus(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	writePrometheus(w, snapshot())
}

func writePrometheus(w io.Writer, metrics []metric) {
	// Sort by name for a stable output order — helpful when diffing
	// scrape output across versions.
	sort.Slice(metrics, func(i, j int) bool { return metrics[i].name < metrics[j].name })
	for _, m := range metrics {
		fmt.Fprintf(w, "# HELP %s %s\n", m.name, m.help)
		fmt.Fprintf(w, "# TYPE %s %s\n", m.name, m.typ)
		// %g for floats, %d for integer-valued gauges would be nicer but
		// complicates the type table; Prometheus accepts scientific notation.
		fmt.Fprintf(w, "%s %g\n", m.name, m.value)
	}
}

// handleJSONMetrics serves /metrics.json for human debugging or scrapers
// that prefer structured data.
func handleJSONMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	snap := make(map[string]float64, 40)
	for _, m := range snapshot() {
		snap[m.name] = m.value
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(snap)
}
