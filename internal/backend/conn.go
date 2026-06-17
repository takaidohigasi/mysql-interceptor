package backend

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/takaidohigasi/mysql-interceptor/internal/config"
)

// DefaultConnectTimeout caps how long we wait for a backend handshake before
// giving up, so an unreachable backend can't pin sessions indefinitely.
const DefaultConnectTimeout = 10 * time.Second

// Connect opens a backend connection using DefaultConnectTimeout.
func Connect(cfg config.BackendConfig, tlsCfg config.BackendSideTLSConfig) (*client.Conn, error) {
	return ConnectWithTimeout(cfg, tlsCfg, DefaultConnectTimeout)
}

// ConnectWithTimeout opens a backend connection with an explicit connect
// timeout, applying cfg.KeepAlive to the underlying TCP socket. The shadow
// path uses a shorter timeout than the primary so a slow/unreachable
// shadow target doesn't pin a connecting goroutine (and, by extension,
// session teardown) for the full default.
//
// We go through client.ConnectWithDialer with our own net.Dialer rather
// than client.ConnectWithTimeout for two reasons: (1) the dialer is where
// TCP keep-alive is configured, and (2) go-mysql's ConnectWithTimeout
// ignores its timeout argument and always dials with a hardcoded 10s — our
// net.Dialer.Timeout makes the requested timeout actually take effect.
func ConnectWithTimeout(cfg config.BackendConfig, tlsCfg config.BackendSideTLSConfig, timeout time.Duration) (*client.Conn, error) {
	var opts []client.Option

	if tlsCfg.Enabled {
		tc, err := buildBackendTLSConfig(tlsCfg)
		if err != nil {
			return nil, fmt.Errorf("building backend TLS config: %w", err)
		}
		opts = append(opts, func(c *client.Conn) error {
			c.SetTLSConfig(tc)
			return nil
		})
	}

	dialer := backendDialer(timeout, cfg.KeepAlive)
	// network "" lets go-mysql pick tcp/unix from the address shape
	// (getNetProto); keep-alive only applies to TCP and is a no-op for
	// unix sockets.
	conn, err := client.ConnectWithDialer(
		context.Background(), "", cfg.Addr, cfg.User, cfg.Password, cfg.DB, dialer.DialContext, opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to backend %s: %w", cfg.Addr, err)
	}

	return conn, nil
}

// backendDialer builds the net.Dialer used for the backend connection,
// wiring in TCP keep-alive when enabled. A nil ka.Enabled (a BackendConfig
// built programmatically that never went through config.Load, e.g. in
// tests) means keep-alive is left off; KeepAlive=-1 explicitly disables
// Go's implicit default.
func backendDialer(timeout time.Duration, ka config.KeepAliveConfig) *net.Dialer {
	d := &net.Dialer{Timeout: timeout}
	if ka.Enabled != nil && *ka.Enabled {
		d.KeepAliveConfig = net.KeepAliveConfig{
			Enable:   true,
			Idle:     ka.Idle,
			Interval: ka.Interval,
			Count:    ka.Count,
		}
	} else {
		d.KeepAlive = -1
	}
	return d
}

func buildBackendTLSConfig(cfg config.BackendSideTLSConfig) (*tls.Config, error) {
	// InsecureSkipVerify is a deliberate opt-in escape hatch controlled by
	// the operator via config; the default is false.
	tc := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.SkipVerify, //nolint:gosec // see above
	}

	if cfg.CAFile != "" {
		caCert, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append CA certificate")
		}
		tc.RootCAs = pool
	}

	return tc, nil
}
