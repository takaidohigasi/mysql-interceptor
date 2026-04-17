package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/takaidohigasi/mysql-interceptor/internal/bench"
	"github.com/takaidohigasi/mysql-interceptor/internal/config"
	"github.com/takaidohigasi/mysql-interceptor/internal/logging"
	"github.com/takaidohigasi/mysql-interceptor/internal/metrics"
	"github.com/takaidohigasi/mysql-interceptor/internal/proxy"
	"github.com/takaidohigasi/mysql-interceptor/internal/replay"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	setupLogger()

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		runServe()
	case "replay":
		runReplay()
	case "bench":
		runBench()
	case "version":
		fmt.Printf("mysql-interceptor %s (commit: %s, built: %s)\n", version, commit, date)
	default:
		printUsage()
		os.Exit(1)
	}
}

// setupLogger configures the default slog logger. Format is controlled by
// LOG_FORMAT (text|json, default text) and level by LOG_LEVEL
// (debug|info|warn|error, default info). Output goes to stderr.
func setupLogger() {
	var level slog.Level
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if strings.ToLower(os.Getenv("LOG_FORMAT")) == "json" {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(handler))
}

// fatal logs at error level and exits. Replacement for log.Fatalf that
// preserves slog's structured output.
func fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: %s <command> [options]\n\n", os.Args[0])
	fmt.Fprintln(os.Stderr, "Commands:")
	fmt.Fprintln(os.Stderr, "  serve    Start the MySQL proxy server")
	fmt.Fprintln(os.Stderr, "  replay   Replay recorded queries from log files")
	fmt.Fprintln(os.Stderr, "  bench    Run benchmarks comparing direct vs proxy performance")
	fmt.Fprintln(os.Stderr, "  version  Print version information")
	fmt.Fprintln(os.Stderr, "\nOptions:")
	fmt.Fprintln(os.Stderr, "  --config <path>   Path to config file (default: config.yaml)")
	fmt.Fprintln(os.Stderr, "\nEnvironment:")
	fmt.Fprintln(os.Stderr, "  LOG_LEVEL   debug | info | warn | error (default: info)")
	fmt.Fprintln(os.Stderr, "  LOG_FORMAT  text | json (default: text)")
}

func runServe() {
	configPath := "config.yaml"
	for i := 2; i < len(os.Args); i++ {
		if os.Args[i] == "--config" && i+1 < len(os.Args) {
			configPath = os.Args[i+1]
			i++
		}
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fatal("failed to load config", "err", err)
	}

	var queryLogger *logging.Logger
	if cfg.Logging.OutputDir != "" {
		queryLogger, err = logging.NewLogger(logging.LoggerConfig{
			Enabled:    cfg.Logging.Enabled,
			OutputDir:  cfg.Logging.OutputDir,
			FilePrefix: cfg.Logging.FilePrefix,
			MaxSizeMB:  cfg.Logging.Rotation.MaxSizeMB,
			MaxAgeDays: cfg.Logging.Rotation.MaxAgeDays,
			MaxBackups: cfg.Logging.Rotation.MaxBackups,
			Compress:   cfg.Logging.Rotation.Compress,
		})
		if err != nil {
			fatal("failed to create query logger", "err", err)
		}
	}

	cfgWatcher, err := config.NewWatcher(configPath)
	if err != nil {
		slog.Warn("failed to watch config file", "err", err)
	} else {
		defer cfgWatcher.Close()
		cfgWatcher.OnChange(func(newCfg *config.Config) {
			if queryLogger != nil {
				queryLogger.SetEnabled(newCfg.Logging.Enabled)
			}
		})
	}

	var shadowSender *replay.ShadowSender
	if cfg.Replay.Mode == "shadow" {
		shadowSender, err = replay.NewShadowSender(cfg.Replay.Shadow, cfg.Comparison)
		if err != nil {
			fatal("failed to create shadow sender", "err", err)
		}
		slog.Info("shadow traffic enabled", "target", cfg.Replay.Shadow.TargetAddr)
	}

	srv, err := proxy.NewProxyServer(cfg, queryLogger, shadowSender)
	if err != nil {
		fatal("failed to create proxy server", "err", err)
	}

	metricsSrv := metrics.NewServer(cfg.Proxy.MetricsAddr)
	metricsSrv.Start()
	defer metricsSrv.Shutdown()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		slog.Info("received shutdown signal")
		srv.Shutdown()
	}()

	serveErr := srv.Serve()

	// Shutdown order matters: sessions must finish before the logger or
	// shadow sender are closed, otherwise a late Log() call would be dropped
	// (or, previously, panic on send-to-closed-channel).
	srv.Shutdown()
	if shadowSender != nil {
		shadowSender.Close()
	}
	if queryLogger != nil {
		queryLogger.Close()
	}

	if serveErr != nil {
		fatal("serve error", "err", serveErr)
	}
}

func runReplay() {
	configPath := "config.yaml"
	for i := 2; i < len(os.Args); i++ {
		if os.Args[i] == "--config" && i+1 < len(os.Args) {
			configPath = os.Args[i+1]
			i++
		}
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fatal("failed to load config", "err", err)
	}

	replayer, err := replay.NewOfflineReplayer(cfg.Replay.Offline, cfg.Comparison)
	if err != nil {
		fatal("failed to create replayer", "err", err)
	}

	if err := replayer.Run(); err != nil {
		fatal("replay error", "err", err)
	}
}

func runBench() {
	configPath := "config.yaml"
	markdownOut := ""
	for i := 2; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--config":
			if i+1 < len(os.Args) {
				configPath = os.Args[i+1]
				i++
			}
		case "--markdown-out":
			if i+1 < len(os.Args) {
				markdownOut = os.Args[i+1]
				i++
			}
		}
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fatal("failed to load config", "err", err)
	}

	if len(cfg.Bench.Queries) == 0 {
		fatal("no benchmark queries configured in bench.queries")
	}

	directDSN := fmt.Sprintf("%s:%s@tcp(%s)/%s",
		cfg.Backend.User, cfg.Backend.Password, cfg.Backend.Addr, cfg.Backend.DB)
	proxyDSN := fmt.Sprintf("%s:%s@tcp(%s)/%s",
		cfg.Backend.User, cfg.Backend.Password, cfg.Proxy.ListenAddr, cfg.Backend.DB)

	slog.Info("benchmarking", "direct", cfg.Backend.Addr, "proxy", cfg.Proxy.ListenAddr)

	report, err := bench.Run(bench.Config{
		DirectDSN:   directDSN,
		ProxyDSN:    proxyDSN,
		Queries:     cfg.Bench.Queries,
		Concurrency: cfg.Bench.Concurrency,
		Iterations:  cfg.Bench.Iterations,
		WarmupIters: cfg.Bench.WarmupIters,
	})
	if err != nil {
		fatal("benchmark error", "err", err)
	}

	report.Print()

	if markdownOut != "" {
		f, err := os.Create(markdownOut)
		if err != nil {
			fatal("failed to open markdown output", "path", markdownOut, "err", err)
		}
		defer f.Close()
		if err := report.WriteMarkdown(f); err != nil {
			fatal("failed to write markdown", "err", err)
		}
		slog.Info("wrote benchmark markdown", "path", markdownOut)
	}
}
