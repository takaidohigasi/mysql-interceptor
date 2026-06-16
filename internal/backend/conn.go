package backend

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
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

// ConnectWithTimeout opens a backend connection with an explicit handshake
// timeout. The shadow path uses a shorter timeout than the primary so a
// slow/unreachable shadow target doesn't pin a connecting goroutine (and,
// by extension, session teardown) for the full default.
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

	conn, err := client.ConnectWithTimeout(cfg.Addr, cfg.User, cfg.Password, cfg.DB, timeout, opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to backend %s: %w", cfg.Addr, err)
	}

	return conn, nil
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
