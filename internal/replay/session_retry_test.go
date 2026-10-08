package replay

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/takaidohigasi/mysql-interceptor/internal/compare"
	"github.com/takaidohigasi/mysql-interceptor/internal/config"
	"github.com/takaidohigasi/mysql-interceptor/internal/metrics"
)

// retryHarness is a ShadowSession wired to a real Engine and a Reporter
// writing to a temp file, with comparison.retry enabled and a stubbed
// execFn, so the retry state machine can be driven without a backend.
type retryHarness struct {
	ss         *ShadowSession
	engine     *compare.Engine
	reporter   *compare.Reporter
	reportPath string
}

func newRetryHarness(t *testing.T, attempts int, delay time.Duration, mode string) *retryHarness {
	t.Helper()
	path := filepath.Join(t.TempDir(), "diff.jsonl")
	reporter, err := compare.NewReporterFromOptions(compare.ReporterOptions{OutputFile: path})
	if err != nil {
		t.Fatalf("NewReporterFromOptions: %v", err)
	}
	t.Cleanup(func() { _ = reporter.Close() })

	sender := &ShadowSender{
		engine:        compare.NewEngine(compare.EngineConfig{}),
		reporter:      reporter,
		retryAttempts: attempts,
		retryDelay:    delay,
		retryMode:     mode,
		timeout:       time.Second,
	}
	sender.enabled.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	ss := &ShadowSession{
		sessionID:  7,
		sender:     sender,
		queryCh:    make(chan ShadowQuery, 8),
		tempTables: make(map[string]struct{}),
		done:       make(chan struct{}),
		ctx:        ctx,
		cancel:     cancel,
	}
	t.Cleanup(cancel)
	return &retryHarness{ss: ss, engine: sender.engine, reporter: reporter, reportPath: path}
}

// records closes the reporter and returns the emitted JSONL lines.
func (h *retryHarness) records(t *testing.T) []string {
	t.Helper()
	if err := h.reporter.Close(); err != nil {
		t.Fatalf("reporter close: %v", err)
	}
	data, err := os.ReadFile(h.reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var lines []string
	for _, l := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

func rows(v string) *compare.CapturedResult {
	return &compare.CapturedResult{Columns: []string{"c"}, Rows: [][]string{{v}}, Duration: time.Millisecond}
}

// sequenceExec returns an execFn that answers the n-th shadow execution
// with results[min(n, len-1)] and counts the calls.
func sequenceExec(calls *atomic.Int32, results ...*compare.CapturedResult) func(*client.Conn, string, ...interface{}) (*compare.CapturedResult, error) {
	return func(_ *client.Conn, _ string, _ ...interface{}) (*compare.CapturedResult, error) {
		n := int(calls.Add(1)) - 1
		if n >= len(results) {
			n = len(results) - 1
		}
		return results[n], nil
	}
}

func selectQuery(orig *compare.CapturedResult) ShadowQuery {
	return ShadowQuery{SessionID: 7, Query: "SELECT c FROM t WHERE id = 1", OrigResult: orig}
}

// TestRetry_ShadowResolvesOnRetry: the first shadow execution lags
// behind, the re-execution after the delay matches the primary's
// original result, and the record is a match flagged resolved_by_retry.
func TestRetry_ShadowResolvesOnRetry(t *testing.T) {
	h := newRetryHarness(t, 2, 20*time.Millisecond, "shadow")
	var calls atomic.Int32
	h.ss.execFn = sequenceExec(&calls, rows("stale"), rows("fresh"))

	h.ss.processQuery(selectQuery(rows("fresh")), h.engine, h.reporter)
	if got := len(h.ss.pendingRetries); got != 1 {
		t.Fatalf("expected the diff to be parked, pending=%d", got)
	}
	if h.ss.retryTimer == nil {
		t.Fatal("retry timer must be armed")
	}
	if !strings.Contains(h.reporter.Summary(), "total=0") {
		t.Fatalf("nothing must be recorded before the retry: %s", h.reporter.Summary())
	}

	time.Sleep(30 * time.Millisecond)
	h.ss.runDueRetries(h.engine, h.reporter)

	if calls.Load() != 2 {
		t.Errorf("expected 2 shadow executions, got %d", calls.Load())
	}
	if len(h.ss.pendingRetries) != 0 || h.ss.retryTimer != nil {
		t.Errorf("retry queue must be empty after a match: pending=%d timer=%v", len(h.ss.pendingRetries), h.ss.retryTimer != nil)
	}
	sum := h.reporter.Summary()
	for _, want := range []string{"total=1", "matched=1", "different=0", "resolved_by_retry=1"} {
		if !strings.Contains(sum, want) {
			t.Errorf("summary missing %q: %s", want, sum)
		}
	}
	recs := h.records(t)
	if len(recs) != 1 {
		t.Fatalf("expected the resolved record to be emitted, got %d lines", len(recs))
	}
	for _, want := range []string{`"match":true`, `"retries":1`, `"resolved_by_retry":true`, `"retry_mode":"shadow"`} {
		if !strings.Contains(recs[0], want) {
			t.Errorf("record missing %s: %s", want, recs[0])
		}
	}
}

// TestRetry_ExhaustsAttempts: the shadow keeps differing, so after
// max_attempts re-executions the diff is reported once, carrying the
// retry count.
func TestRetry_ExhaustsAttempts(t *testing.T) {
	h := newRetryHarness(t, 2, time.Millisecond, "shadow")
	var calls atomic.Int32
	h.ss.execFn = sequenceExec(&calls, rows("stale"))

	h.ss.processQuery(selectQuery(rows("fresh")), h.engine, h.reporter)
	for i := 0; i < 2; i++ {
		time.Sleep(5 * time.Millisecond)
		h.ss.runDueRetries(h.engine, h.reporter)
	}
	if calls.Load() != 3 {
		t.Errorf("expected 1 + 2 executions, got %d", calls.Load())
	}
	sum := h.reporter.Summary()
	for _, want := range []string{"total=1", "matched=0", "different=1", "resolved_by_retry=0"} {
		if !strings.Contains(sum, want) {
			t.Errorf("summary missing %q: %s", want, sum)
		}
	}
	recs := h.records(t)
	if len(recs) != 1 || !strings.Contains(recs[0], `"retries":2`) || strings.Contains(recs[0], "resolved_by_retry") {
		t.Errorf("expected one diff record with retries=2: %v", recs)
	}
}

// TestRetry_NotForNonSelect: statements other than SELECT are never
// parked, even when they differ.
func TestRetry_NotForNonSelect(t *testing.T) {
	h := newRetryHarness(t, 2, time.Millisecond, "shadow")
	var calls atomic.Int32
	h.ss.execFn = sequenceExec(&calls, &compare.CapturedResult{AffectedRows: 1})

	sq := ShadowQuery{SessionID: 7, Query: "SET NAMES utf8mb4", OrigResult: &compare.CapturedResult{AffectedRows: 0}}
	h.ss.processQuery(sq, h.engine, h.reporter)
	if len(h.ss.pendingRetries) != 0 {
		t.Fatal("non-SELECT must not be retried")
	}
	if !strings.Contains(h.reporter.Summary(), "different=1") {
		t.Errorf("diff must be recorded immediately: %s", h.reporter.Summary())
	}
}

// TestRetry_Disabled: with max_attempts 0 the diff is recorded at once
// and no retry bookkeeping appears on the record.
func TestRetry_Disabled(t *testing.T) {
	h := newRetryHarness(t, 0, time.Second, "shadow")
	var calls atomic.Int32
	h.ss.execFn = sequenceExec(&calls, rows("stale"))
	h.ss.processQuery(selectQuery(rows("fresh")), h.engine, h.reporter)
	if len(h.ss.pendingRetries) != 0 {
		t.Fatal("retry must be disabled")
	}
	recs := h.records(t)
	if len(recs) != 1 || strings.Contains(recs[0], "retries") || strings.Contains(recs[0], "retry_mode") {
		t.Errorf("expected a plain diff record: %v", recs)
	}
}

// TestRetry_SettledOnShutdown: a parked retry is re-executed immediately
// when the session ends, so no comparison is lost.
func TestRetry_SettledOnShutdown(t *testing.T) {
	h := newRetryHarness(t, 2, time.Hour, "shadow")
	var calls atomic.Int32
	h.ss.execFn = sequenceExec(&calls, rows("stale"), rows("fresh"))

	h.ss.processQuery(selectQuery(rows("fresh")), h.engine, h.reporter)
	if len(h.ss.pendingRetries) != 1 {
		t.Fatal("expected a parked retry")
	}
	h.ss.cancel()
	finished := make(chan struct{})
	go func() { h.ss.run(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return after cancel")
	}
	if calls.Load() != 2 {
		t.Errorf("expected the parked query to be re-executed on shutdown, calls=%d", calls.Load())
	}
	if !strings.Contains(h.reporter.Summary(), "resolved_by_retry=1") {
		t.Errorf("expected the settled retry to be recorded: %s", h.reporter.Summary())
	}
}

// TestRetry_BrokenConnectionReportsLastResult: when the shadow
// connection is already gone at teardown, the parked diff is reported
// from the last execution instead of being re-run.
func TestRetry_BrokenConnectionReportsLastResult(t *testing.T) {
	h := newRetryHarness(t, 2, time.Hour, "shadow")
	var calls atomic.Int32
	h.ss.execFn = sequenceExec(&calls, rows("stale"))

	h.ss.processQuery(selectQuery(rows("fresh")), h.engine, h.reporter)
	h.ss.connBroken = true
	h.ss.settlePendingRetries(h.engine, h.reporter)
	if calls.Load() != 1 {
		t.Errorf("must not execute on a broken connection, calls=%d", calls.Load())
	}
	recs := h.records(t)
	if len(recs) != 1 || !strings.Contains(recs[0], `"match":false`) || !strings.Contains(recs[0], `"retries":1`) {
		t.Errorf("expected the original diff reported with retries=1: %v", recs)
	}
}

// TestRetry_BothReexecutesPrimary: in mode "both" the retry refreshes
// the primary result over the verification connection, so a row the
// primary changed after the original execution no longer counts as a
// shadow diff once both sides agree.
func TestRetry_BothReexecutesPrimary(t *testing.T) {
	h := newRetryHarness(t, 1, time.Millisecond, "both")
	primary := &client.Conn{}
	var connects atomic.Int32
	h.ss.sender.connectPrimary = func(cfg config.BackendConfig, _ config.BackendSideTLSConfig) (*client.Conn, error) {
		connects.Add(1)
		if cfg.User != "alice" {
			t.Errorf("verification connection must use the session user, got %q", cfg.User)
		}
		return primary, nil
	}
	h.ss.primaryCfg = &config.BackendConfig{User: "alice"}

	var shadowCalls atomic.Int32
	h.ss.execFn = func(conn *client.Conn, _ string, _ ...interface{}) (*compare.CapturedResult, error) {
		if conn == primary {
			return rows("v2"), nil // the primary moved on
		}
		shadowCalls.Add(1)
		return rows("v2"), nil // the shadow has caught up to v2
	}
	before := metrics.Global.ShadowRetryPrimaryQueries.Load()

	// Original comparison: primary saw v1, shadow already shows v2.
	h.ss.processQuery(selectQuery(rows("v1")), h.engine, h.reporter)
	time.Sleep(5 * time.Millisecond)
	h.ss.runDueRetries(h.engine, h.reporter)

	if connects.Load() != 1 || h.ss.primaryConn != primary {
		t.Errorf("verification connection must be opened once, connects=%d", connects.Load())
	}
	if metrics.Global.ShadowRetryPrimaryQueries.Load() != before+1 {
		t.Error("primary re-execution must be counted")
	}
	recs := h.records(t)
	if len(recs) != 1 {
		t.Fatalf("expected one record, got %v", recs)
	}
	for _, want := range []string{`"match":true`, `"resolved_by_retry":true`, `"retry_mode":"both"`} {
		if !strings.Contains(recs[0], want) {
			t.Errorf("record missing %s: %s", want, recs[0])
		}
	}
}

// TestRetry_BothFallsBackToShadow covers the cases where the primary
// side cannot be used: session state the verification connection cannot
// reproduce, and a failing connect. Both degrade to mode "shadow".
func TestRetry_BothFallsBackToShadow(t *testing.T) {
	t.Run("session state touched", func(t *testing.T) {
		h := newRetryHarness(t, 1, time.Millisecond, "both")
		h.ss.primaryCfg = &config.BackendConfig{}
		h.ss.sender.connectPrimary = func(config.BackendConfig, config.BackendSideTLSConfig) (*client.Conn, error) {
			t.Fatal("must not connect to the primary after SET")
			return nil, nil
		}
		var calls atomic.Int32
		h.ss.execFn = sequenceExec(&calls, rows("stale"), rows("fresh"))
		if !h.ss.passesCategoryCheck("SET time_zone = '+09:00'") {
			t.Fatal("SET must pass the category check")
		}
		h.ss.processQuery(selectQuery(rows("fresh")), h.engine, h.reporter)
		time.Sleep(5 * time.Millisecond)
		h.ss.runDueRetries(h.engine, h.reporter)
		recs := h.records(t)
		if len(recs) != 1 || !strings.Contains(recs[0], `"retry_mode":"shadow"`) || !strings.Contains(recs[0], `"resolved_by_retry":true`) {
			t.Errorf("expected a shadow-mode resolved record: %v", recs)
		}
	})
	t.Run("primary connect fails", func(t *testing.T) {
		h := newRetryHarness(t, 2, time.Millisecond, "both")
		h.ss.primaryCfg = &config.BackendConfig{}
		var connects atomic.Int32
		h.ss.sender.connectPrimary = func(config.BackendConfig, config.BackendSideTLSConfig) (*client.Conn, error) {
			connects.Add(1)
			return nil, errors.New("dial refused")
		}
		var calls atomic.Int32
		h.ss.execFn = sequenceExec(&calls, rows("stale"))
		h.ss.processQuery(selectQuery(rows("fresh")), h.engine, h.reporter)
		for i := 0; i < 2; i++ {
			time.Sleep(5 * time.Millisecond)
			h.ss.runDueRetries(h.engine, h.reporter)
		}
		if connects.Load() != 1 || !h.ss.primaryConnFailed {
			t.Errorf("connect must be attempted once per session, connects=%d failed=%v", connects.Load(), h.ss.primaryConnFailed)
		}
		recs := h.records(t)
		if len(recs) != 1 || !strings.Contains(recs[0], `"retry_mode":"shadow"`) || !strings.Contains(recs[0], `"retries":2`) {
			t.Errorf("expected a shadow-mode diff with retries=2: %v", recs)
		}
	})
}
