package config

import (
	"fmt"
	"os"
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
	Enabled    bool           `yaml:"enabled"`
	OutputDir  string         `yaml:"output_dir"`
	FilePrefix string         `yaml:"file_prefix"`
	// RedactArgs replaces prepared-statement bind values in logged entries
	// with "<redacted>" so they never hit disk. Useful when queries may
	// bind passwords, tokens, or other PII. The query text (with ?
	// placeholders) is still recorded.
	RedactArgs bool           `yaml:"redact_args"`
	Rotation   RotationConfig `yaml:"rotation"`
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
	// ReadOnly is always enforced regardless of this flag — kept for backward
	// compatibility and to make the safety behavior explicit in config files.
	ReadOnly      bool          `yaml:"readonly"`
	Async         bool          `yaml:"async"`
	Timeout       time.Duration `yaml:"timeout"`
	MaxConcurrent int           `yaml:"max_concurrent"`
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
}

type ComparisonConfig struct {
	OutputFile      string   `yaml:"output_file"`
	IgnoreColumns   []string `yaml:"ignore_columns"`
	TimeThresholdMs float64  `yaml:"time_threshold_ms"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	applyDefaults(cfg)

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return cfg, nil
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
	if cfg.Replay.Mode == "" {
		cfg.Replay.Mode = "disabled"
	}
	if cfg.Replay.Shadow.Timeout == 0 {
		cfg.Replay.Shadow.Timeout = 5 * time.Second
	}
	if cfg.Replay.Shadow.MaxConcurrent == 0 {
		cfg.Replay.Shadow.MaxConcurrent = 100
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
	if cfg.Bench.Concurrency == 0 {
		cfg.Bench.Concurrency = 1
	}
	if cfg.Bench.Iterations == 0 {
		cfg.Bench.Iterations = 100
	}
	if cfg.Bench.WarmupIters == 0 {
		cfg.Bench.WarmupIters = 10
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
	return nil
}
