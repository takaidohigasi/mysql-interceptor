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
