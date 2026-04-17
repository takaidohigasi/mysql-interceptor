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

func Connect(cfg config.BackendConfig, tlsCfg config.BackendSideTLSConfig) (*client.Conn, error) {
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

	conn, err := client.ConnectWithTimeout(cfg.Addr, cfg.User, cfg.Password, cfg.DB, DefaultConnectTimeout, opts...)
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
