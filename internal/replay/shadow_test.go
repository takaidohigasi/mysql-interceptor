package replay

import (
	"testing"

	"github.com/takaidohigasi/mysql-interceptor/internal/config"
)

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
