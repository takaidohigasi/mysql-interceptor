//go:build unix

package backend

import (
	"context"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/takaidohigasi/mysql-interceptor/internal/config"
)

// TestBackendDialer_SetsSocketKeepAlive dials a real local TCP listener
// with the keep-alive-enabled dialer and reads SO_KEEPALIVE off the
// resulting socket, proving the option is actually applied at the OS level
// (not just present in the dialer struct). Unix-only: SO_KEEPALIVE /
// GetsockoptInt aren't portable to Windows.
func TestBackendDialer_SetsSocketKeepAlive(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			defer c.Close()
		}
	}()

	d := backendDialer(2*time.Second, config.KeepAliveConfig{
		Enabled:  boolPtr(true),
		Idle:     30 * time.Second,
		Interval: 10 * time.Second,
		Count:    3,
	})
	conn, err := d.DialContext(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatalf("expected *net.TCPConn, got %T", conn)
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn: %v", err)
	}

	var soKeepAlive int
	var ctrlErr error
	if err := raw.Control(func(fd uintptr) {
		soKeepAlive, ctrlErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_KEEPALIVE)
	}); err != nil {
		t.Fatalf("Control: %v", err)
	}
	if ctrlErr != nil {
		t.Fatalf("GetsockoptInt(SO_KEEPALIVE): %v", ctrlErr)
	}
	if soKeepAlive == 0 {
		t.Error("expected SO_KEEPALIVE enabled on the dialed socket, got 0")
	}
}
