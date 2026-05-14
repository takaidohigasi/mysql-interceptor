package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoad_ValidConfig(t *testing.T) {
	content := `
proxy:
  listen_addr: "0.0.0.0:3307"
  users:
    - username: "root"
      password: "pass"
backend:
  addr: "127.0.0.1:3306"
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
proxy:
  users:
    - username: "root"
      password: "p"
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

func TestLoad_MultiUserMode(t *testing.T) {
	// Each session opens its backend connection using the authenticated
	// user's creds; backend.user is not used.
	content := `
backend:
  addr: "127.0.0.1:3306"
proxy:
  users:
    - username: "alice"
      password: "alice-pw"
    - username: "bob"
      password: "bob-pw"
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
	if len(cfg.Proxy.Users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(cfg.Proxy.Users))
	}
	if cfg.Proxy.Users[0].Username != "alice" || cfg.Proxy.Users[0].Password != "alice-pw" {
		t.Errorf("user[0] = %+v, want {alice, alice-pw}", cfg.Proxy.Users[0])
	}
}

func TestLoad_MultiUserDuplicateRejected(t *testing.T) {
	content := `
backend:
  addr: "127.0.0.1:3306"
proxy:
  users:
    - username: "alice"
      password: "p1"
    - username: "alice"
      password: "p2"
`
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(content), 0o644)

	_, err := Load(cfgPath)
	if err == nil {
		t.Error("expected validation error for duplicate username")
	}
}

func TestLoad_MultiUserMissingUsernameRejected(t *testing.T) {
	content := `
backend:
  addr: "127.0.0.1:3306"
proxy:
  users:
    - password: "p1"
`
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(content), 0o644)

	_, err := Load(cfgPath)
	if err == nil {
		t.Error("expected validation error for missing username")
	}
}

// TestLoad_HashedPassword_Accepts confirms the MySQL standard
// "*XXXX..." 41-char form is parsed and validated cleanly. The exact
// value below is `SELECT PASSWORD('alice')` from a MySQL 5.7 instance
// — used here purely as a shape-correct fixture; no real account
// uses it.
func TestLoad_HashedPassword_Accepts(t *testing.T) {
	content := `
backend:
  addr: "127.0.0.1:3306"
proxy:
  users:
    - username: "alice"
      hashed_password: "*4DF1D66463C18D44E3B001A8FB1BBFBEA13E27FC"
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
	if cfg.Proxy.Users[0].HashedPassword != "*4DF1D66463C18D44E3B001A8FB1BBFBEA13E27FC" {
		t.Errorf("hashed_password not preserved through load: %+v", cfg.Proxy.Users[0])
	}
}

// TestLoad_HashedPassword_RejectsBoth confirms the validator catches
// the ambiguous "both password and hashed_password set" case. Without
// this gate it would be silently undefined which value is used.
func TestLoad_HashedPassword_RejectsBoth(t *testing.T) {
	content := `
backend:
  addr: "127.0.0.1:3306"
proxy:
  users:
    - username: "alice"
      password: "plain"
      hashed_password: "*4DF1D66463C18D44E3B001A8FB1BBFBEA13E27FC"
`
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(content), 0o644)

	if _, err := Load(cfgPath); err == nil {
		t.Error("expected validation error when both password and hashed_password are set")
	}
}

// TestLoad_HashedPassword_RejectsMalformed exercises the shape check
// across the most common typos: missing leading "*", wrong length,
// and non-hex characters. Any of these registered would only fail
// hours later at first login attempt — fail-fast here instead.
func TestLoad_HashedPassword_RejectsMalformed(t *testing.T) {
	// Empty string is intentionally NOT tested here: an empty
	// hashed_password is indistinguishable from "field absent" in YAML,
	// so the validator treats it as unset and falls through to the
	// (also possibly empty) Password — matching the pre-change
	// behavior where users could register with empty passwords.
	for name, hashed := range map[string]string{
		"no_leading_asterisk": "4DF1D66463C18D44E3B001A8FB1BBFBEA13E27FC", // 40 hex chars but missing "*"
		"too_short":           "*4DF1D664",
		"too_long":            "*4DF1D66463C18D44E3B001A8FB1BBFBEA13E27FCEXTRA",
		"non_hex":             "*4DF1D66463C18D44E3B001A8FB1BBFBEA13E27FZ", // trailing Z
	} {
		t.Run(name, func(t *testing.T) {
			content := `
backend:
  addr: "127.0.0.1:3306"
proxy:
  users:
    - username: "alice"
      hashed_password: "` + hashed + `"
`
			tmpDir := t.TempDir()
			cfgPath := filepath.Join(tmpDir, "config.yaml")
			os.WriteFile(cfgPath, []byte(content), 0o644)

			if _, err := Load(cfgPath); err == nil {
				t.Errorf("expected validation error for hashed_password=%q", hashed)
			}
		})
	}
}

func TestLoad_TLSRequiresCertAndKey(t *testing.T) {
	content := `
backend:
  addr: "127.0.0.1:3306"
proxy:
  users:
    - username: "root"
      password: "p"
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

func TestLoad_ExpandsEnvVars(t *testing.T) {
	t.Setenv("MI_TEST_USER", "mercari_user")
	t.Setenv("MI_TEST_PASS", "s3cr3t")

	content := `
backend:
  addr: "127.0.0.1:3306"
proxy:
  users:
    - username: "${MI_TEST_USER}"
      password: "${MI_TEST_PASS}"
replay:
  mode: "shadow"
  shadow:
    target_addr: "10.0.0.1:3306"
    target_user: "${MI_TEST_USER}"
    target_password: "${MI_TEST_PASS}"
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

	if got := cfg.Proxy.Users[0].Username; got != "mercari_user" {
		t.Errorf("expected proxy.users[0].username=mercari_user, got %q", got)
	}
	if got := cfg.Proxy.Users[0].Password; got != "s3cr3t" {
		t.Errorf("expected proxy.users[0].password=s3cr3t, got %q", got)
	}
	if cfg.Replay.Shadow.TargetUser != "mercari_user" {
		t.Errorf("expected shadow.target_user=mercari_user, got %q", cfg.Replay.Shadow.TargetUser)
	}
	if cfg.Replay.Shadow.TargetPassword != "s3cr3t" {
		t.Errorf("expected shadow.target_password=s3cr3t, got %q", cfg.Replay.Shadow.TargetPassword)
	}
}

func TestLoad_UnsetEnvVarErrors(t *testing.T) {
	// Make sure the var really is unset for this test.
	os.Unsetenv("MI_TEST_DEFINITELY_UNSET_1")
	os.Unsetenv("MI_TEST_DEFINITELY_UNSET_2")

	content := `
backend:
  addr: "127.0.0.1:3306"
proxy:
  users:
    - username: "${MI_TEST_DEFINITELY_UNSET_1}"
      password: "${MI_TEST_DEFINITELY_UNSET_2}"
`
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := Load(cfgPath)
	if err == nil {
		t.Fatal("expected error for unset env vars, got nil")
	}
	// Both missing names should appear in the error so the operator can
	// fix them together.
	for _, name := range []string{"MI_TEST_DEFINITELY_UNSET_1", "MI_TEST_DEFINITELY_UNSET_2"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("expected error to mention %q, got: %v", name, err)
		}
	}
}

func TestLoad_DoesNotExpandBareDollar(t *testing.T) {
	// SQL queries legitimately use bare $ (e.g. PostgreSQL-style placeholders
	// or MySQL session variables). Make sure they survive Load() unchanged.
	t.Setenv("MI_TEST_USER", "mercari_user")

	content := `
backend:
  addr: "127.0.0.1:3306"
proxy:
  users:
    - username: "${MI_TEST_USER}"
      password: "p"
bench:
  queries:
    - "SELECT $1, $foo FROM dual"
    - "SET @counter = @counter + 1"
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

	if cfg.Bench.Queries[0] != "SELECT $1, $foo FROM dual" {
		t.Errorf("bare $ should be preserved, got %q", cfg.Bench.Queries[0])
	}
}
