package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/takaidohigasi/mysql-interceptor/internal/bench"
	"github.com/takaidohigasi/mysql-interceptor/internal/config"
	"github.com/takaidohigasi/mysql-interceptor/internal/logging"
	"github.com/takaidohigasi/mysql-interceptor/internal/proxy"
	"github.com/takaidohigasi/mysql-interceptor/internal/replay"
)

func main() {
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
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: %s <command> [options]\n\n", os.Args[0])
	fmt.Fprintln(os.Stderr, "Commands:")
	fmt.Fprintln(os.Stderr, "  serve    Start the MySQL proxy server")
	fmt.Fprintln(os.Stderr, "  replay   Replay recorded queries from log files")
	fmt.Fprintln(os.Stderr, "  bench    Run benchmarks comparing direct vs proxy performance")
	fmt.Fprintln(os.Stderr, "\nOptions:")
	fmt.Fprintln(os.Stderr, "  --config <path>   Path to config file (default: config.yaml)")
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
		log.Fatalf("failed to load config: %v", err)
	}

	// Initialize SQL logger
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
			log.Fatalf("failed to create query logger: %v", err)
		}
		defer queryLogger.Close()
	}

	// Set up config hot-reload
	cfgWatcher, err := config.NewWatcher(configPath)
	if err != nil {
		log.Printf("warning: failed to watch config file: %v", err)
	} else {
		defer cfgWatcher.Close()
		cfgWatcher.OnChange(func(newCfg *config.Config) {
			if queryLogger != nil {
				queryLogger.SetEnabled(newCfg.Logging.Enabled)
			}
		})
	}

	// Initialize shadow sender if shadow mode is enabled
	var shadowSender *replay.ShadowSender
	if cfg.Replay.Mode == "shadow" {
		shadowSender, err = replay.NewShadowSender(cfg.Replay.Shadow, cfg.Comparison)
		if err != nil {
			log.Fatalf("failed to create shadow sender: %v", err)
		}
		defer shadowSender.Close()
		log.Printf("Shadow traffic enabled: forwarding to %s", cfg.Replay.Shadow.TargetAddr)
	}

	srv, err := proxy.NewProxyServer(cfg, queryLogger, shadowSender)
	if err != nil {
		log.Fatalf("failed to create proxy server: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("received shutdown signal")
		srv.Shutdown()
	}()

	if err := srv.Serve(); err != nil {
		log.Fatalf("serve error: %v", err)
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
		log.Fatalf("failed to load config: %v", err)
	}

	replayer, err := replay.NewOfflineReplayer(cfg.Replay.Offline, cfg.Comparison)
	if err != nil {
		log.Fatalf("failed to create replayer: %v", err)
	}

	if err := replayer.Run(); err != nil {
		log.Fatalf("replay error: %v", err)
	}
}

func runBench() {
	configPath := "config.yaml"
	for i := 2; i < len(os.Args); i++ {
		if os.Args[i] == "--config" && i+1 < len(os.Args) {
			configPath = os.Args[i+1]
			i++
		}
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	if len(cfg.Bench.Queries) == 0 {
		log.Fatal("no benchmark queries configured in bench.queries")
	}

	// Build DSNs from config
	directDSN := fmt.Sprintf("%s:%s@tcp(%s)/%s",
		cfg.Backend.User, cfg.Backend.Password, cfg.Backend.Addr, cfg.Backend.DB)
	proxyDSN := fmt.Sprintf("%s:%s@tcp(%s)/%s",
		cfg.Backend.User, cfg.Backend.Password, cfg.Proxy.ListenAddr, cfg.Backend.DB)

	log.Printf("Direct DSN: %s@tcp(%s)", cfg.Backend.User, cfg.Backend.Addr)
	log.Printf("Proxy DSN:  %s@tcp(%s)", cfg.Backend.User, cfg.Proxy.ListenAddr)

	report, err := bench.Run(bench.Config{
		DirectDSN:   directDSN,
		ProxyDSN:    proxyDSN,
		Queries:     cfg.Bench.Queries,
		Concurrency: cfg.Bench.Concurrency,
		Iterations:  cfg.Bench.Iterations,
		WarmupIters: cfg.Bench.WarmupIters,
	})
	if err != nil {
		log.Fatalf("benchmark error: %v", err)
	}

	report.Print()
}
