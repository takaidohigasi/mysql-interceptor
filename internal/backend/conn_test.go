package backend

import (
	"testing"
	"time"

	"github.com/takaidohigasi/mysql-interceptor/internal/config"
)

func boolPtr(b bool) *bool { return &b }

func TestBackendDialer_KeepAliveEnabled(t *testing.T) {
	d := backendDialer(7*time.Second, config.KeepAliveConfig{
		Enabled:  boolPtr(true),
		Idle:     30 * time.Second,
		Interval: 10 * time.Second,
		Count:    3,
	})
	if d.Timeout != 7*time.Second {
		t.Errorf("expected dial timeout 7s, got %v", d.Timeout)
	}
	if !d.KeepAliveConfig.Enable {
		t.Fatal("expected KeepAliveConfig.Enable true")
	}
	if d.KeepAliveConfig.Idle != 30*time.Second ||
		d.KeepAliveConfig.Interval != 10*time.Second ||
		d.KeepAliveConfig.Count != 3 {
		t.Errorf("unexpected KeepAliveConfig: %+v", d.KeepAliveConfig)
	}
}

func TestBackendDialer_KeepAliveDisabled(t *testing.T) {
	// nil Enabled (e.g. the shadow target, never run through config.Load)
	// must leave keep-alive off — KeepAlive=-1 disables Go's default too.
	d := backendDialer(3*time.Second, config.KeepAliveConfig{})
	if d.KeepAliveConfig.Enable {
		t.Error("expected KeepAliveConfig.Enable false for nil Enabled")
	}
	if d.KeepAlive != -1 {
		t.Errorf("expected KeepAlive=-1 (disabled), got %v", d.KeepAlive)
	}

	// Explicit enabled:false behaves the same.
	d = backendDialer(3*time.Second, config.KeepAliveConfig{Enabled: boolPtr(false)})
	if d.KeepAliveConfig.Enable || d.KeepAlive != -1 {
		t.Errorf("expected keep-alive disabled for enabled:false, got Enable=%v KeepAlive=%v",
			d.KeepAliveConfig.Enable, d.KeepAlive)
	}
}

// TestBackendDialer_DialRespectsTimeout verifies the dialer actually
// enforces the connect timeout (go-mysql's ConnectWithTimeout did not —
// the reason we dial ourselves). Dialing an unroutable address must fail
// within roughly the configured timeout, not hang.
func TestBackendDialer_DialRespectsTimeout(t *testing.T) {
	d := backendDialer(150*time.Millisecond, config.KeepAliveConfig{Enabled: boolPtr(true), Idle: time.Second, Interval: time.Second, Count: 1})
	// 192.0.2.0/24 (TEST-NET-1) is reserved and unroutable → connect blocks
	// until the dial timeout fires.
	start := time.Now()
	_, err := d.Dial("tcp", "192.0.2.1:3306")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected dial to fail to an unroutable address")
	}
	if elapsed > time.Second {
		t.Fatalf("dial took %v; timeout (150ms) was not honored", elapsed)
	}
}
