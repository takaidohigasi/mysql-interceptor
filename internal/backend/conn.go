package backend

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/takaidohigasi/mysql-interceptor/internal/config"
)

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

	conn, err := client.Connect(cfg.Addr, cfg.User, cfg.Password, cfg.DB, opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to backend %s: %w", cfg.Addr, err)
	}

	return conn, nil
}

func buildBackendTLSConfig(cfg config.BackendSideTLSConfig) (*tls.Config, error) {
	tc := &tls.Config{
		InsecureSkipVerify: cfg.SkipVerify,
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
