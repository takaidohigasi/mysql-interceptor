package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_ValidConfig(t *testing.T) {
	content := `
proxy:
  listen_addr: "0.0.0.0:3307"
backend:
  addr: "127.0.0.1:3306"
  user: "root"
  password: "pass"
logging:
  enabled: true
  output_dir: "/tmp/logs"
`
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if cfg.Proxy.ListenAddr != "0.0.0.0:3307" {
		t.Errorf("expected listen_addr 0.0.0.0:3307, got %s", cfg.Proxy.ListenAddr)
	}
	if cfg.Backend.Addr != "127.0.0.1:3306" {
		t.Errorf("expected backend addr 127.0.0.1:3306, got %s", cfg.Backend.Addr)
	}
	// Check defaults applied
	if cfg.Proxy.MaxConnections != 1000 {
		t.Errorf("expected default max_connections 1000, got %d", cfg.Proxy.MaxConnections)
	}
	if cfg.Replay.Mode != "disabled" {
		t.Errorf("expected default replay mode 'disabled', got %s", cfg.Replay.Mode)
	}
	if cfg.Logging.FilePrefix != "queries" {
		t.Errorf("expected default file_prefix 'queries', got %s", cfg.Logging.FilePrefix)
	}
}

func TestLoad_MissingBackendAddr(t *testing.T) {
	content := `
backend:
  user: "root"
`
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(content), 0o644)

	_, err := Load(cfgPath)
	if err == nil {
		t.Error("expected validation error for missing backend.addr")
	}
}

func TestLoad_InvalidReplayMode(t *testing.T) {
	content := `
backend:
  addr: "127.0.0.1:3306"
  user: "root"
replay:
  mode: "invalid"
`
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(content), 0o644)

	_, err := Load(cfgPath)
	if err == nil {
		t.Error("expected validation error for invalid replay mode")
	}
}

func TestLoad_TLSRequiresCertAndKey(t *testing.T) {
	content := `
backend:
  addr: "127.0.0.1:3306"
  user: "root"
tls:
  client_side:
    enabled: true
`
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(content), 0o644)

	_, err := Load(cfgPath)
	if err == nil {
		t.Error("expected validation error when TLS enabled without cert/key")
	}
}
