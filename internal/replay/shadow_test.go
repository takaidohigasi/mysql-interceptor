package replay

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/takaidohigasi/mysql-interceptor/internal/config"
)

// captureSlog redirects the default slog logger into the returned buffer
// for the duration of the test. Restoring is the caller's responsibility
// (use t.Cleanup or the returned restore func).
func captureSlog(t *testing.T) (*bytes.Buffer, *sync.Mutex) {
	t.Helper()
	buf := &bytes.Buffer{}
	mu := &sync.Mutex{}
	h := slog.NewTextHandler(&lockedWriter{buf: buf, mu: mu}, nil)
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf, mu
}

type lockedWriter struct {
	buf *bytes.Buffer
	mu  *sync.Mutex
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

// TestShadowSender_EnableDisable verifies that Send() respects the
// enabled flag and that toggling via SetEnabled flips behavior without
// requiring a restart.
//
// We don't want to actually connect to a shadow backend, so we construct
// the sender with an unreachable target and immediately disable it —
// workers never pick up a query because Send() short-circuits first.
func TestShadowSender_EnableDisable(t *testing.T) {
	falseVal := false
	s, err := NewShadowSender(
		config.ShadowConfig{
			TargetAddr:    "127.0.0.1:1", // unreachable on purpose
			TargetUser:    "root",
			Enabled:       &falseVal, // start disabled
			MaxConcurrent: 1,
		},
		config.ComparisonConfig{},
	)
	if err != nil {
		t.Fatalf("NewShadowSender: %v", err)
	}
	defer s.Close()

	if s.IsEnabled() {
		t.Fatal("expected IsEnabled=false after construction with enabled=false")
	}

	// Sending while disabled — counted as disabled, not dropped.
	s.Send(ShadowQuery{Query: "SELECT 1"})
	if got := s.disabled.Load(); got != 1 {
		t.Errorf("expected disabled count=1, got %d", got)
	}
	if got := s.dropped.Load(); got != 0 {
		t.Errorf("expected dropped=0 while disabled, got %d", got)
	}

	// Re-enable. New sends should pass through the Send() gate.
	s.SetEnabled(true)
	if !s.IsEnabled() {
		t.Fatal("expected IsEnabled=true after SetEnabled(true)")
	}

	// SELECT-style query: passes the readonly filter. Will try to enqueue.
	// The worker will eventually try to connect to 127.0.0.1:1 and fail,
	// but the enqueue itself succeeds (buffer is 10000).
	s.Send(ShadowQuery{Query: "SELECT 1"})
	// disabled count must not have changed.
	if got := s.disabled.Load(); got != 1 {
		t.Errorf("disabled count should stay at 1 after re-enable, got %d", got)
	}

	// Disable again mid-flight.
	s.SetEnabled(false)
	s.Send(ShadowQuery{Query: "SELECT 1"})
	if got := s.disabled.Load(); got != 2 {
		t.Errorf("expected disabled count=2 after second disable, got %d", got)
	}
}

// TestShadowSender_DefaultEnabled confirms that a zero-value ShadowConfig
// (Enabled == nil) is treated as enabled, matching the documented default.
func TestShadowSender_DefaultEnabled(t *testing.T) {
	s, err := NewShadowSender(
		config.ShadowConfig{
			TargetAddr:    "127.0.0.1:1",
			TargetUser:    "root",
			MaxConcurrent: 1,
			// Enabled is nil
		},
		config.ComparisonConfig{},
	)
	if err != nil {
		t.Fatalf("NewShadowSender: %v", err)
	}
	defer s.Close()
	if !s.IsEnabled() {
		t.Error("expected default-enabled when ShadowConfig.Enabled is nil")
	}
}

// TestShadowSender_CIDRAllowlist verifies that sends are filtered when
// allowed_source_cidrs is non-empty and the source IP doesn't match.
func TestShadowSender_CIDRAllowlist(t *testing.T) {
	s, err := NewShadowSender(
		config.ShadowConfig{
			TargetAddr:         "127.0.0.1:1",
			TargetUser:         "root",
			MaxConcurrent:      1,
			AllowedSourceCIDRs: []string{"10.0.0.0/8"},
		},
		config.ComparisonConfig{},
	)
	if err != nil {
		t.Fatalf("NewShadowSender: %v", err)
	}
	defer s.Close()

	// IP inside allow list → NOT filtered.
	s.Send(ShadowQuery{SourceIP: "10.1.2.3", Query: "SELECT 1"})
	if got := s.Filtered(); got != 0 {
		t.Errorf("expected 0 filtered for allowed IP, got %d", got)
	}

	// IP outside allow list → filtered.
	s.Send(ShadowQuery{SourceIP: "192.168.0.1", Query: "SELECT 1"})
	if got := s.Filtered(); got != 1 {
		t.Errorf("expected 1 filtered for disallowed IP, got %d", got)
	}

	// Empty source IP → pass through (best-effort for non-TCP sockets).
	s.Send(ShadowQuery{SourceIP: "", Query: "SELECT 1"})
	if got := s.Filtered(); got != 1 {
		t.Errorf("expected filtered count unchanged for empty IP, got %d", got)
	}
}

// TestShadowSender_CIDRExcludeList verifies that exclude wins over allow.
func TestShadowSender_CIDRExcludeList(t *testing.T) {
	s, err := NewShadowSender(
		config.ShadowConfig{
			TargetAddr:          "127.0.0.1:1",
			TargetUser:          "root",
			MaxConcurrent:       1,
			AllowedSourceCIDRs:  []string{"10.0.0.0/8"},
			ExcludedSourceCIDRs: []string{"10.0.5.0/24"}, // DBA subnet
		},
		config.ComparisonConfig{},
	)
	if err != nil {
		t.Fatalf("NewShadowSender: %v", err)
	}
	defer s.Close()

	// Allowed subnet, not in exclude → NOT filtered.
	s.Send(ShadowQuery{SourceIP: "10.1.2.3", Query: "SELECT 1"})
	if got := s.Filtered(); got != 0 {
		t.Errorf("expected 0 filtered, got %d", got)
	}

	// In allow AND in exclude → filtered (exclude wins).
	s.Send(ShadowQuery{SourceIP: "10.0.5.42", Query: "SELECT 1"})
	if got := s.Filtered(); got != 1 {
		t.Errorf("expected 1 filtered (exclude wins), got %d", got)
	}
}

// TestShadowSender_CIDRExcludeOnly confirms that with only exclude rules,
// unlisted IPs are allowed.
func TestShadowSender_CIDRExcludeOnly(t *testing.T) {
	s, err := NewShadowSender(
		config.ShadowConfig{
			TargetAddr:          "127.0.0.1:1",
			TargetUser:          "root",
			MaxConcurrent:       1,
			ExcludedSourceCIDRs: []string{"10.0.5.0/24"},
		},
		config.ComparisonConfig{},
	)
	if err != nil {
		t.Fatalf("NewShadowSender: %v", err)
	}
	defer s.Close()

	// Random IP not in exclude → allowed.
	s.Send(ShadowQuery{SourceIP: "192.168.1.1", Query: "SELECT 1"})
	if got := s.Filtered(); got != 0 {
		t.Errorf("expected 0 filtered with exclude-only policy, got %d", got)
	}

	// In exclude → filtered.
	s.Send(ShadowQuery{SourceIP: "10.0.5.1", Query: "SELECT 1"})
	if got := s.Filtered(); got != 1 {
		t.Errorf("expected 1 filtered, got %d", got)
	}
}

// TestShadowSender_SampleRate_Zero drops every query.
func TestShadowSender_SampleRate_Zero(t *testing.T) {
	rate := 0.0
	s, err := NewShadowSender(
		config.ShadowConfig{
			TargetAddr:    "127.0.0.1:1",
			TargetUser:    "root",
			MaxConcurrent: 1,
			SampleRate:    &rate,
		},
		config.ComparisonConfig{},
	)
	if err != nil {
		t.Fatalf("NewShadowSender: %v", err)
	}
	defer s.Close()

	for i := 0; i < 100; i++ {
		s.Send(ShadowQuery{Query: "SELECT 1"})
	}
	if got := s.SampledOut(); got != 100 {
		t.Errorf("expected 100 sampled out with rate=0, got %d", got)
	}
}

// TestShadowSender_SampleRate_One samples every query (default).
func TestShadowSender_SampleRate_One(t *testing.T) {
	s, err := NewShadowSender(
		config.ShadowConfig{
			TargetAddr:    "127.0.0.1:1",
			TargetUser:    "root",
			MaxConcurrent: 1,
			// SampleRate nil → default 1.0
		},
		config.ComparisonConfig{},
	)
	if err != nil {
		t.Fatalf("NewShadowSender: %v", err)
	}
	defer s.Close()

	for i := 0; i < 100; i++ {
		s.Send(ShadowQuery{Query: "SELECT 1"})
	}
	if got := s.SampledOut(); got != 0 {
		t.Errorf("expected 0 sampled out with rate=1.0, got %d", got)
	}
	if got := s.SampleRate(); got != 1.0 {
		t.Errorf("expected SampleRate()=1.0, got %v", got)
	}
}

// TestShadowSender_SampleRate_Half hits roughly 50% under large N.
// Uses a generous tolerance to avoid flakes from the random sampler.
func TestShadowSender_SampleRate_Half(t *testing.T) {
	rate := 0.5
	s, err := NewShadowSender(
		config.ShadowConfig{
			TargetAddr:    "127.0.0.1:1",
			TargetUser:    "root",
			MaxConcurrent: 1,
			SampleRate:    &rate,
		},
		config.ComparisonConfig{},
	)
	if err != nil {
		t.Fatalf("NewShadowSender: %v", err)
	}
	defer s.Close()

	const n = 10000
	for i := 0; i < n; i++ {
		s.Send(ShadowQuery{Query: "SELECT 1"})
	}
	out := int(s.SampledOut())
	// Expected ~5000; allow ±500 (10% tolerance) to avoid flakes.
	if out < 4500 || out > 5500 {
		t.Errorf("expected ~5000 sampled out for rate=0.5 over %d sends, got %d", n, out)
	}
}

// TestShadowSender_SetSampleRate_HotReload confirms mid-flight updates.
func TestShadowSender_SetSampleRate_HotReload(t *testing.T) {
	s, err := NewShadowSender(
		config.ShadowConfig{
			TargetAddr:    "127.0.0.1:1",
			TargetUser:    "root",
			MaxConcurrent: 1,
		},
		config.ComparisonConfig{},
	)
	if err != nil {
		t.Fatalf("NewShadowSender: %v", err)
	}
	defer s.Close()

	// Drop everything.
	if err := s.SetSampleRate(0.0); err != nil {
		t.Fatalf("SetSampleRate: %v", err)
	}
	for i := 0; i < 50; i++ {
		s.Send(ShadowQuery{Query: "SELECT 1"})
	}
	if got := s.SampledOut(); got != 50 {
		t.Errorf("expected 50 sampled out after rate=0, got %d", got)
	}

	// Restore full sampling.
	if err := s.SetSampleRate(1.0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		s.Send(ShadowQuery{Query: "SELECT 1"})
	}
	if got := s.SampledOut(); got != 50 {
		t.Errorf("sampled_out should stay at 50 after rate=1.0 restored, got %d", got)
	}

	// Reject out-of-range values.
	if err := s.SetSampleRate(-0.1); err == nil {
		t.Error("expected error for rate < 0")
	}
	if err := s.SetSampleRate(1.5); err == nil {
		t.Error("expected error for rate > 1")
	}
}

// TestShadowSender_SetCIDRs_HotReload verifies runtime update of filters.
func TestShadowSender_SetCIDRs_HotReload(t *testing.T) {
	s, err := NewShadowSender(
		config.ShadowConfig{
			TargetAddr:    "127.0.0.1:1",
			TargetUser:    "root",
			MaxConcurrent: 1,
		},
		config.ComparisonConfig{},
	)
	if err != nil {
		t.Fatalf("NewShadowSender: %v", err)
	}
	defer s.Close()

	// Initially no filters — everything passes.
	s.Send(ShadowQuery{SourceIP: "192.168.1.1", Query: "SELECT 1"})
	if got := s.Filtered(); got != 0 {
		t.Errorf("expected 0 filtered with no policy, got %d", got)
	}

	// Hot-reload to a restrictive allow list.
	if err := s.SetCIDRs([]string{"10.0.0.0/8"}, nil); err != nil {
		t.Fatalf("SetCIDRs: %v", err)
	}
	s.Send(ShadowQuery{SourceIP: "192.168.1.1", Query: "SELECT 1"})
	if got := s.Filtered(); got != 1 {
		t.Errorf("expected 1 filtered after reload, got %d", got)
	}

	// Reject invalid CIDR.
	if err := s.SetCIDRs([]string{"not-a-cidr"}, nil); err == nil {
		t.Error("expected error for invalid CIDR")
	}
}

// TestShadowSender_PeriodicSummary confirms the configured interval
// causes the cumulative comparison summary to be logged at runtime,
// not just at shutdown.
func TestShadowSender_PeriodicSummary(t *testing.T) {
	buf, mu := captureSlog(t)

	s, err := NewShadowSender(
		config.ShadowConfig{
			TargetAddr:    "127.0.0.1:1",
			TargetUser:    "root",
			MaxConcurrent: 1,
		},
		config.ComparisonConfig{
			SummaryInterval: 30 * time.Millisecond,
		},
	)
	if err != nil {
		t.Fatalf("NewShadowSender: %v", err)
	}
	defer s.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		out := buf.String()
		mu.Unlock()
		if strings.Contains(out, "shadow comparison periodic summary") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	final := buf.String()
	mu.Unlock()
	t.Fatalf("expected periodic summary log line within 2s, got:\n%s", final)
}

// TestShadowSender_PeriodicSummaryDisabled confirms that a negative
// interval suppresses the periodic log entirely. We can't prove a
// negative absolutely, but a generous wait without seeing the line is
// strong enough evidence the loop never started.
func TestShadowSender_PeriodicSummaryDisabled(t *testing.T) {
	buf, mu := captureSlog(t)

	s, err := NewShadowSender(
		config.ShadowConfig{
			TargetAddr:    "127.0.0.1:1",
			TargetUser:    "root",
			MaxConcurrent: 1,
		},
		config.ComparisonConfig{
			SummaryInterval: -1, // any negative value disables
		},
	)
	if err != nil {
		t.Fatalf("NewShadowSender: %v", err)
	}
	defer s.Close()

	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	out := buf.String()
	mu.Unlock()
	if strings.Contains(out, "shadow comparison periodic summary") {
		t.Fatalf("expected no periodic summary when interval<0, got:\n%s", out)
	}
}
