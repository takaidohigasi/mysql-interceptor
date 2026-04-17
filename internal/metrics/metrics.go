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
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// Counters holds the metrics the server exposes. Fields are pointers to
// atomic Int64 so handlers can observe them without a mutex.
type Counters struct {
	ActiveSessions        atomic.Int64
	TotalSessions         atomic.Int64
	QueriesHandled        atomic.Int64
	QueryErrors           atomic.Int64
	LoggerDropped         atomic.Int64
	ShadowDropped         atomic.Int64
	ShadowSkipped         atomic.Int64
	ShadowQueriesReplayed atomic.Int64
	ComparisonsTotal      atomic.Int64
	ComparisonsMatched    atomic.Int64
	ComparisonsDiffered   atomic.Int64
	ComparisonsIgnored    atomic.Int64
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
	mux.HandleFunc("/metrics", handleMetrics)
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

func handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	snap := map[string]int64{
		"active_sessions":         Global.ActiveSessions.Load(),
		"total_sessions":          Global.TotalSessions.Load(),
		"queries_handled":         Global.QueriesHandled.Load(),
		"query_errors":            Global.QueryErrors.Load(),
		"logger_dropped":          Global.LoggerDropped.Load(),
		"shadow_dropped":          Global.ShadowDropped.Load(),
		"shadow_skipped":          Global.ShadowSkipped.Load(),
		"shadow_queries_replayed": Global.ShadowQueriesReplayed.Load(),
		"comparisons_total":       Global.ComparisonsTotal.Load(),
		"comparisons_matched":     Global.ComparisonsMatched.Load(),
		"comparisons_differed":    Global.ComparisonsDiffered.Load(),
		"comparisons_ignored":     Global.ComparisonsIgnored.Load(),
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(snap)
}
