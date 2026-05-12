package config

import (
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Proxy      ProxyConfig      `yaml:"proxy"`
	Backend    BackendConfig    `yaml:"backend"`
	TLS        TLSConfig        `yaml:"tls"`
	Logging    LoggingConfig    `yaml:"logging"`
	Replay     ReplayConfig     `yaml:"replay"`
	Comparison ComparisonConfig `yaml:"comparison"`
	Bench      BenchConfig      `yaml:"bench"`
}

type BenchConfig struct {
	Queries     []string `yaml:"queries"`
	Concurrency int      `yaml:"concurrency"`
	Iterations  int      `yaml:"iterations"`
	WarmupIters int      `yaml:"warmup_iterations"`
}

type ProxyConfig struct {
	ListenAddr      string        `yaml:"listen_addr"`
	MetricsAddr     string        `yaml:"metrics_addr"` // "" to disable; e.g. "127.0.0.1:9090"
	MaxConnections  int           `yaml:"max_connections"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
	// Cluster names the database cluster this proxy fronts. When set,
	// every Prometheus metric line is rendered with `{cluster="<value>"}`,
	// so a single dashboard can break stats down per database cluster
	// (e.g. select a specific cluster from a template variable). Empty =
	// no label emitted, preserving backward compatibility for
	// single-cluster deployments.
	Cluster string `yaml:"cluster,omitempty"`
	// Users is the list of credentials the proxy will accept on the
	// inbound handshake. The matched (username, password) pair is also
	// used to authenticate the per-session outbound connection to the
	// backend (and the shadow, when shadow mode is on), so per-user
	// GRANTs on the backend apply consistently. At least one entry is
	// required; the proxy never synthesizes credentials of its own.
	Users []UserConfig `yaml:"users"`
	// MaxSessionLifetime caps how long any client session may stay open.
	// At session start the proxy snapshots a per-session jittered deadline
	// (configured value ±10%); after each command completes, if the
	// session is past its deadline AND the backend connection is not in
	// a transaction, the proxy closes both the client and backend
	// connections. The client is expected to reconnect — useful for
	// rebalancing onto new backends after autoscaling.
	//
	// Mid-query and mid-transaction sessions are never interrupted: the
	// check happens only between commands and only when the backend
	// reports SERVER_STATUS_IN_TRANS = false. 0 (the default) disables
	// the cap entirely. Hot-reloadable.
	MaxSessionLifetime time.Duration `yaml:"max_session_lifetime,omitempty"`
}

// UserConfig describes one user the proxy will accept. Exactly one of
// Password or HashedPassword must be set per entry — the validator
// rejects both empty and both set.
//
// Password is the plaintext form. When set, it's used both to validate
// the inbound client handshake AND to authenticate the proxy's outbound
// connection to the backend (and shadow, when shadow mode is on). This
// is the canonical pattern: a single source of truth for one user.
//
// HashedPassword is MySQL's standard `*XXXX...` 41-character hex form
// (i.e. the value stored in `mysql.user.authentication_string` for the
// mysql_native_password plugin: "*" + uppercase hex of SHA1(SHA1(plain))).
// It exists to support users — typically personal accounts mirrored
// from another proxy's config — whose plaintext password the
// interceptor never sees. The handshake-side verification works
// without the plaintext (see go-mysql-org/go-mysql#1129), so HashedPassword
// entries can log in. However, the proxy's *outbound* backend
// connection currently still requires plaintext; a hashed-only user
// authenticates successfully but the session terminates at backend-
// connect time with a clear error. Granting hashed-only users true
// query access is tracked as a follow-up (would need either client-side
// hash-auth in go-mysql, or a separate per-user `backend_password`
// override field).
type UserConfig struct {
	Username       string `yaml:"username"`
	Password       string `yaml:"password,omitempty"`
	HashedPassword string `yaml:"hashed_password,omitempty"`
}

// BackendConfig identifies the backend MySQL server the proxy talks to.
// Only Addr (and optionally DB) are user-configurable in YAML; User and
// Password are populated per-session from the matched ProxyConfig.Users
// entry at handshake time and never read from config.
type BackendConfig struct {
	Addr string `yaml:"addr"`
	DB   string `yaml:"db,omitempty"`

	// User and Password are not yaml-bound: they're set at runtime from
	// the matched ProxyConfig.Users entry. Kept on this struct so it can
	// be passed straight to backend.Connect.
	User     string `yaml:"-"`
	Password string `yaml:"-"`
}

type TLSConfig struct {
	ClientSide  ClientSideTLSConfig  `yaml:"client_side"`
	BackendSide BackendSideTLSConfig `yaml:"backend_side"`
}

type ClientSideTLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	CAFile   string `yaml:"ca_file"`
}

type BackendSideTLSConfig struct {
	Enabled    bool   `yaml:"enabled"`
	CAFile     string `yaml:"ca_file"`
	SkipVerify bool   `yaml:"skip_verify"`
}

type LoggingConfig struct {
	Enabled    bool   `yaml:"enabled"`
	OutputDir  string `yaml:"output_dir"`
	FilePrefix string `yaml:"file_prefix"`
	// RedactArgs replaces prepared-statement bind values in logged entries
	// with "<redacted>" so they never hit disk. Useful when queries may
	// bind passwords, tokens, or other PII. The query text (with ?
	// placeholders) is still recorded.
	RedactArgs bool `yaml:"redact_args"`
	// QueueSize bounds the async log channel. Larger = more burst tolerance
	// but higher memory ceiling. Entries beyond the buffer are dropped
	// (counted as logger_dropped). Default 10000.
	QueueSize int            `yaml:"queue_size"`
	Rotation  RotationConfig `yaml:"rotation"`
}

type RotationConfig struct {
	MaxSizeMB  int  `yaml:"max_size_mb"`
	MaxAgeDays int  `yaml:"max_age_days"`
	MaxBackups int  `yaml:"max_backups"`
	Compress   bool `yaml:"compress"`
}

type ReplayConfig struct {
	Mode    string        `yaml:"mode"` // "disabled", "shadow", "offline"
	Shadow  ShadowConfig  `yaml:"shadow"`
	Offline OfflineConfig `yaml:"offline"`
}

type ShadowConfig struct {
	TargetAddr     string               `yaml:"target_addr"`
	TargetUser     string               `yaml:"target_user"`
	TargetPassword string               `yaml:"target_password"`
	TLS            BackendSideTLSConfig `yaml:"tls"`
	// Enabled gates whether live queries are forwarded to the shadow
	// server. Hot-reloadable: set false in config to pause shadow sending
	// without restarting the proxy (e.g. during shadow-server maintenance).
	// Defaults to true when mode is "shadow".
	Enabled *bool `yaml:"enabled,omitempty"`
	// AllowedSourceCIDRs, if non-empty, restricts shadow forwarding to
	// sessions whose client IP falls within one of the listed CIDRs.
	// Example: ["10.0.0.0/8", "192.168.1.0/24"]. Leave empty to allow all.
	AllowedSourceCIDRs []string `yaml:"allowed_source_cidrs"`
	// ExcludedSourceCIDRs is evaluated first: any source IP matching one
	// of these CIDRs is never shadowed, even if it also matches
	// AllowedSourceCIDRs. Useful for excluding e.g. DBA hosts from shadow.
	ExcludedSourceCIDRs []string `yaml:"excluded_source_cidrs"`
	// ReadOnly is always enforced regardless of this flag — kept for backward
	// compatibility and to make the safety behavior explicit in config files.
	ReadOnly      bool          `yaml:"readonly"`
	Timeout       time.Duration `yaml:"timeout"`
	MaxConcurrent int           `yaml:"max_concurrent"`
	// QueueSize bounds the shadow query channel. Larger = more burst
	// tolerance but higher memory ceiling. Sends beyond the buffer are
	// counted as shadow_dropped. Default 10000.
	QueueSize int `yaml:"queue_size"`
	// SampleRate is the fraction of primary queries to forward to shadow,
	// in [0.0, 1.0]. 1.0 = shadow all queries (default). 0.1 = shadow ~10%.
	// Use to cap shadow overhead under high primary load; queries that
	// don't get sampled are counted as shadow_sampled_out. Hot-reloadable.
	// A pointer is used so "not set" (nil) can be distinguished from an
	// explicit 0.0 (shadow nothing).
	SampleRate *float64 `yaml:"sample_rate,omitempty"`
}

type OfflineConfig struct {
	InputDir            string               `yaml:"input_dir"`
	FilePattern         string               `yaml:"file_pattern"`
	TargetAddr          string               `yaml:"target_addr"`
	TargetUser          string               `yaml:"target_user"`
	TargetPassword      string               `yaml:"target_password"`
	TLS                 BackendSideTLSConfig `yaml:"tls"`
	SpeedFactor         float64              `yaml:"speed_factor"`
	Concurrency         int                  `yaml:"concurrency"`
	CheckpointFile      string               `yaml:"checkpoint_file"`
	AutoDeleteCompleted bool                 `yaml:"auto_delete_completed"`
	// ScannerBufferSizeBytes is the maximum line length the JSONL reader
	// will accept. Lines longer than this are logged as malformed and
	// skipped. Default 1 MiB.
	ScannerBufferSizeBytes int `yaml:"scanner_buffer_size_bytes"`
}

type ComparisonConfig struct {
	OutputFile      string   `yaml:"output_file"`
	IgnoreColumns   []string `yaml:"ignore_columns"`
	TimeThresholdMs float64  `yaml:"time_threshold_ms"`
	// IgnoreQueries is a list of case-insensitive regular expressions.
	// If the query text matches any pattern, the comparison result is
	// marked Ignored=true and doesn't contribute to the diff count.
	// Use this for queries that read server-local state and therefore
	// always diverge between instances:
	//   - "@@server_uuid"
	//   - "@@hostname"
	//   - "CONNECTION_ID\\s*\\("
	//   - "LAST_INSERT_ID\\s*\\("
	//   - "\\bNOW\\s*\\("
	IgnoreQueries []string `yaml:"ignore_queries"`
	// MaxUniqueDigests caps the number of distinct query patterns tracked
	// in the per-digest stats map. Once reached, new digests are dropped
	// (counted as comparisons_digest_overflow) while existing ones keep
	// updating. Each tracked digest can hold up to ~160 KB of timing
	// samples, so the worst-case memory ceiling is cap × 160 KB. Typical
	// apps have 50–500 digests; ad-hoc analytical workloads may need
	// this higher. Default 10000.
	MaxUniqueDigests int `yaml:"max_unique_digests"`
	// SummaryInterval controls how often the shadow sender logs a
	// cumulative comparison summary (totals + per-digest avg/p95/p99)
	// via slog. Only shadow mode honors this setting; offline replay
	// prints its summary at completion regardless. The shadow shutdown
	// summary always fires too. A negative value disables the periodic
	// log and relies on the shutdown summary alone. Default 1h.
	//
	// Note: under load the periodic snapshot reads atomic totals
	// independently of the per-digest map, so the top-line totals can
	// be ahead of the per-digest table by a small number of in-flight
	// records. This self-corrects on the next tick and at shutdown.
	SummaryInterval time.Duration `yaml:"summary_interval"`

	// LogMatches controls whether *every* comparison record is written
	// to OutputFile, or only the differences. Shadow mode only —
	// offline replay always writes a complete report regardless. When
	// false (default), matched and ignored comparisons are suppressed
	// inline and are represented only by the periodic heartbeat below.
	// Set true if you need a full audit trail of every comparison
	// (note: at high QPS this can be very noisy).
	LogMatches bool `yaml:"log_matches"`

	// HeartbeatInterval controls how often the reporter writes a
	// "type":"heartbeat" line to OutputFile summarizing the previous
	// window (matched / differed / ignored counts since the last
	// heartbeat). Useful as a liveness signal when LogMatches is off
	// and traffic is mostly clean — the absence of diff lines for a
	// while otherwise looks indistinguishable from a stuck proxy. Only
	// shadow mode emits heartbeats. Negative disables. Default 1m.
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`

	// RedactColumns is the per-column list of columns whose cell-value
	// diff payloads are masked. When a cell_value diff is recorded for
	// a column in this list, the diff record still carries the type,
	// column name, and row index (so operators see *that* the column
	// drifted), but `original` and `replay` are replaced with the
	// literal string "<redacted>". Use for security-relevant columns
	// where you want drift visibility without leaking the values
	// themselves into the comparison output stream — e.g. credentials,
	// PII, tokens. Sibling to logging.redact_args (which masks
	// prepared-statement bind values in the audit log).
	RedactColumns []string `yaml:"redact_columns"`

	// RedactAllValues is the global override: when true, every
	// cell_value and error diff has its `original` and `replay`
	// replaced with "<redacted>", regardless of RedactColumns. The
	// diff record itself (type, column name where applicable, row
	// index, timing) is still emitted so digest and column counters
	// still work. Use for high-sensitivity environments where any
	// value leak through divergence logs is unacceptable, or as a
	// defense-in-depth fallback against an incomplete RedactColumns
	// list.
	RedactAllValues bool `yaml:"redact_all_values"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	expanded, err := expandEnvVars(data)
	if err != nil {
		return nil, fmt.Errorf("expanding env vars in config: %w", err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(expanded, cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	applyDefaults(cfg)

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return cfg, nil
}

// envVarPattern matches ${VAR} references where VAR starts with a letter
// or underscore and contains letters, digits, or underscores.
//
// The bash-style $VAR (without braces) is intentionally not matched: SQL
// statements legitimately use $ (e.g. "SELECT $1") and we don't want to
// silently mangle them.
var envVarPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEnvVars replaces every ${VAR} in data with the value of the
// corresponding environment variable. References to unset env vars are
// collected and returned as a single error so the operator can fix them
// all in one go rather than discovering them one at a time.
func expandEnvVars(data []byte) ([]byte, error) {
	var missing []string
	seen := map[string]bool{}
	out := envVarPattern.ReplaceAllFunc(data, func(match []byte) []byte {
		name := string(match[2 : len(match)-1])
		val, ok := os.LookupEnv(name)
		if !ok {
			if !seen[name] {
				seen[name] = true
				missing = append(missing, name)
			}
			return match
		}
		return []byte(val)
	})
	if len(missing) > 0 {
		return nil, fmt.Errorf("unset env vars referenced in config: %s", strings.Join(missing, ", "))
	}
	return out, nil
}

func applyDefaults(cfg *Config) {
	if cfg.Proxy.ListenAddr == "" {
		cfg.Proxy.ListenAddr = "0.0.0.0:3307"
	}
	if cfg.Proxy.MaxConnections == 0 {
		cfg.Proxy.MaxConnections = 1000
	}
	if cfg.Proxy.ShutdownTimeout == 0 {
		cfg.Proxy.ShutdownTimeout = 30 * time.Second
	}
	if cfg.Logging.FilePrefix == "" {
		cfg.Logging.FilePrefix = "queries"
	}
	if cfg.Logging.Rotation.MaxSizeMB == 0 {
		cfg.Logging.Rotation.MaxSizeMB = 100
	}
	if cfg.Logging.Rotation.MaxAgeDays == 0 {
		cfg.Logging.Rotation.MaxAgeDays = 7
	}
	if cfg.Logging.Rotation.MaxBackups == 0 {
		cfg.Logging.Rotation.MaxBackups = 5
	}
	if cfg.Logging.QueueSize == 0 {
		cfg.Logging.QueueSize = 10000
	}
	if cfg.Replay.Mode == "" {
		cfg.Replay.Mode = "disabled"
	}
	if cfg.Replay.Shadow.Timeout == 0 {
		cfg.Replay.Shadow.Timeout = 5 * time.Second
	}
	if cfg.Replay.Shadow.MaxConcurrent == 0 {
		cfg.Replay.Shadow.MaxConcurrent = 100
	}
	if cfg.Replay.Shadow.QueueSize == 0 {
		cfg.Replay.Shadow.QueueSize = 10000
	}
	// Shadow is enabled by default; only an explicit `enabled: false`
	// disables it. We use *bool so "not set" is distinguishable from
	// "set to false".
	if cfg.Replay.Shadow.Enabled == nil {
		t := true
		cfg.Replay.Shadow.Enabled = &t
	}
	// Same rationale for sample_rate: nil → default 1.0 (shadow all).
	if cfg.Replay.Shadow.SampleRate == nil {
		r := 1.0
		cfg.Replay.Shadow.SampleRate = &r
	}
	// Read-only filter is always applied. Setting the default to true here
	// makes the behavior explicit for anyone inspecting the effective config.
	cfg.Replay.Shadow.ReadOnly = true
	if cfg.Replay.Offline.SpeedFactor == 0 {
		cfg.Replay.Offline.SpeedFactor = 1.0
	}
	if cfg.Replay.Offline.Concurrency == 0 {
		cfg.Replay.Offline.Concurrency = 10
	}
	if cfg.Replay.Offline.ScannerBufferSizeBytes == 0 {
		cfg.Replay.Offline.ScannerBufferSizeBytes = 1024 * 1024 // 1 MiB
	}
	if cfg.Bench.Concurrency == 0 {
		cfg.Bench.Concurrency = 1
	}
	if cfg.Bench.Iterations == 0 {
		cfg.Bench.Iterations = 100
	}
	if cfg.Bench.WarmupIters == 0 {
		cfg.Bench.WarmupIters = 10
	}
	if cfg.Comparison.MaxUniqueDigests == 0 {
		cfg.Comparison.MaxUniqueDigests = 10000
	}
	// 0 is treated as "unset" since we can't distinguish it from the
	// YAML default, so it falls back to 1h. Operators who want to
	// disable the periodic log can set a negative duration (e.g. -1s),
	// which the shadow sender treats as "do not start the loop".
	if cfg.Comparison.SummaryInterval == 0 {
		cfg.Comparison.SummaryInterval = time.Hour
	}
	// Same convention as SummaryInterval: 0 → default, negative →
	// disabled.
	if cfg.Comparison.HeartbeatInterval == 0 {
		cfg.Comparison.HeartbeatInterval = time.Minute
	}
}

func (c *Config) Validate() error {
	if c.Backend.Addr == "" {
		return fmt.Errorf("backend.addr is required")
	}
	// proxy.users is the only way to authenticate clients. The proxy
	// always opens its outbound backend connection using the inbound
	// user's credentials, so at least one entry is required.
	if len(c.Proxy.Users) == 0 {
		return fmt.Errorf("proxy.users must contain at least one entry")
	}
	seen := make(map[string]bool, len(c.Proxy.Users))
	for i, u := range c.Proxy.Users {
		if u.Username == "" {
			return fmt.Errorf("proxy.users[%d].username is required", i)
		}
		if seen[u.Username] {
			return fmt.Errorf("proxy.users[%d].username %q is duplicated", i, u.Username)
		}
		seen[u.Username] = true
		// Exactly one of password / hashed_password must be set.
		// Empty plaintext password is meaningful (MySQL allows empty
		// passwords), so we distinguish "field absent" from "field
		// present and empty" via the HashedPassword being non-empty.
		// HashedPassword takes precedence when set; if neither is set
		// the entry is rejected because the auth handler would have
		// nothing to register the user with.
		switch {
		case u.Password != "" && u.HashedPassword != "":
			return fmt.Errorf("proxy.users[%d] %q: set exactly one of password or hashed_password, not both", i, u.Username)
		case u.HashedPassword != "":
			// MySQL's mysql_native_password stored form is "*" followed
			// by 40 uppercase hex chars (SHA1(SHA1(plain)) → 20 bytes →
			// 40 hex chars, prefixed). Validate shape here so a typo
			// fails at config load rather than at first login attempt.
			if err := validateHashedPasswordShape(u.HashedPassword); err != nil {
				return fmt.Errorf("proxy.users[%d] %q: %w", i, u.Username, err)
			}
		default:
			// u.Password == "" && u.HashedPassword == "" — allowed
			// for the "empty password" edge case; AddUser will register
			// the user as accepting an empty password. Operators who
			// don't want this should either set a Password or remove
			// the user. We don't reject the case here because we'd
			// break the existing semantics of UserConfig{Password: ""}.
		}
	}
	switch c.Replay.Mode {
	case "disabled", "shadow", "offline":
	default:
		return fmt.Errorf("replay.mode must be one of: disabled, shadow, offline")
	}
	if c.TLS.ClientSide.Enabled {
		if c.TLS.ClientSide.CertFile == "" || c.TLS.ClientSide.KeyFile == "" {
			return fmt.Errorf("tls.client_side.cert_file and key_file are required when TLS is enabled")
		}
	}
	for i, pat := range c.Comparison.IgnoreQueries {
		if _, err := regexp.Compile("(?i)" + pat); err != nil {
			return fmt.Errorf("comparison.ignore_queries[%d] invalid regex %q: %w", i, pat, err)
		}
	}
	if c.Replay.Shadow.SampleRate != nil {
		r := *c.Replay.Shadow.SampleRate
		if r < 0.0 || r > 1.0 {
			return fmt.Errorf("replay.shadow.sample_rate must be in [0.0, 1.0], got %v", r)
		}
	}
	if c.Proxy.MaxSessionLifetime < 0 {
		return fmt.Errorf("proxy.max_session_lifetime must be non-negative, got %v", c.Proxy.MaxSessionLifetime)
	}
	for i, cidr := range c.Replay.Shadow.AllowedSourceCIDRs {
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return fmt.Errorf("replay.shadow.allowed_source_cidrs[%d] invalid CIDR %q: %w", i, cidr, err)
		}
	}
	for i, cidr := range c.Replay.Shadow.ExcludedSourceCIDRs {
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return fmt.Errorf("replay.shadow.excluded_source_cidrs[%d] invalid CIDR %q: %w", i, cidr, err)
		}
	}
	return nil
}

// validateHashedPasswordShape checks that s is in MySQL's standard
// mysql_native_password stored form: a leading "*" followed by exactly
// 40 hex digits (uppercase or lowercase). This is just a shape check —
// cryptographic validity is verified on the first login by go-mysql.
// Fail-fast at config load so a typo (missing leading "*", wrong
// length, paste error mixing in surrounding chars) doesn't only
// surface when a user tries to log in hours later.
func validateHashedPasswordShape(s string) error {
	if len(s) == 0 {
		return fmt.Errorf("hashed_password is empty")
	}
	if s[0] != '*' {
		return fmt.Errorf("hashed_password must start with '*' (MySQL mysql_native_password form: \"*XXXXXXXX...\", 41 chars total), got %q", firstN(s, 4))
	}
	if len(s) != 41 {
		return fmt.Errorf("hashed_password must be exactly 41 chars (\"*\" + 40 hex digits), got %d", len(s))
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return fmt.Errorf("hashed_password contains non-hex char %q at position %d", c, i)
		}
	}
	return nil
}

func firstN(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
