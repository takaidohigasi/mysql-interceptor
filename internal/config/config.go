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
}

type BackendConfig struct {
	Addr     string `yaml:"addr"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DB       string `yaml:"db"`
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
}

func (c *Config) Validate() error {
	if c.Backend.Addr == "" {
		return fmt.Errorf("backend.addr is required")
	}
	if c.Backend.User == "" {
		return fmt.Errorf("backend.user is required")
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
