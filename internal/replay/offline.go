package replay

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/takaidohigasi/mysql-interceptor/internal/backend"
	"github.com/takaidohigasi/mysql-interceptor/internal/compare"
	"github.com/takaidohigasi/mysql-interceptor/internal/config"
	"github.com/takaidohigasi/mysql-interceptor/internal/logging"
)

// checkpointSaveInterval is how often the replayer persists its progress
// count. SELECT replay is idempotent, so on restart an in-progress file is
// simply restarted from the beginning — but the checkpoint still gives
// operators a live view of replay progress.
const checkpointSaveInterval = 5 * time.Second

type OfflineReplayer struct {
	cfg         config.OfflineConfig
	compareCfg  config.ComparisonConfig
	pool        *backend.Pool
	engine      *compare.Engine
	reporter    *compare.Reporter
	checkpoint  *Checkpoint
	speedFactor float64
}

func NewOfflineReplayer(cfg config.OfflineConfig, compareCfg config.ComparisonConfig) (*OfflineReplayer, error) {
	pool := backend.NewPool(
		config.BackendConfig{
			Addr:     cfg.TargetAddr,
			User:     cfg.TargetUser,
			Password: cfg.TargetPassword,
		},
		cfg.TLS,
		cfg.Concurrency,
	)

	ignoreColumns := make(map[string]bool)
	for _, col := range compareCfg.IgnoreColumns {
		ignoreColumns[col] = true
	}

	ignoreRegexes, err := compare.CompileIgnoreQueries(compareCfg.IgnoreQueries)
	if err != nil {
		return nil, fmt.Errorf("compiling ignore_queries: %w", err)
	}

	engine := compare.NewEngine(compare.EngineConfig{
		IgnoreColumns:    ignoreColumns,
		TimeThresholdMs:  compareCfg.TimeThresholdMs,
		IgnoreQueryRegex: ignoreRegexes,
	})

	// Offline mode always emits every record to the report file: the
	// file *is* the output, and operators run offline replay
	// specifically to get a full diff report (including matched and
	// ignored entries). LogMatches/HeartbeatInterval from config are
	// shadow-mode tunables and are not honored here.
	reporter, err := compare.NewReporterFromOptions(compare.ReporterOptions{
		OutputFile:       compareCfg.OutputFile,
		MaxUniqueDigests: compareCfg.MaxUniqueDigests,
		LogMatches:       true,
	})
	if err != nil {
		return nil, fmt.Errorf("creating reporter: %w", err)
	}

	checkpoint, err := LoadCheckpoint(cfg.CheckpointFile)
	if err != nil {
		return nil, fmt.Errorf("loading checkpoint: %w", err)
	}

	return &OfflineReplayer{
		cfg:         cfg,
		compareCfg:  compareCfg,
		pool:        pool,
		engine:      engine,
		reporter:    reporter,
		checkpoint:  checkpoint,
		speedFactor: cfg.SpeedFactor,
	}, nil
}

// Run replays all matching log files. On context cancellation the
// replayer stops at the next safe point (between files or between per-
// session queries), persists the current checkpoint, and returns
// context.Canceled so callers can distinguish "interrupted" from "error".
func (r *OfflineReplayer) Run(ctx context.Context) error {
	defer r.pool.Close()
	defer r.reporter.Close()

	files, err := r.findLogFiles()
	if err != nil {
		return err
	}

	if len(files) == 0 {
		slog.Info("no log files found to replay")
		return nil
	}

	slog.Info("replaying log files", "count", len(files))

	for _, file := range files {
		if err := ctx.Err(); err != nil {
			slog.Info("replay interrupted, stopping before next file",
				"file", filepath.Base(file))
			return err
		}

		basename := filepath.Base(file)
		if r.checkpoint.IsCompleted(basename) {
			slog.Info("skipping completed file", "file", basename)
			continue
		}

		slog.Info("replaying file", "file", basename)
		if err := r.replayFile(ctx, file); err != nil {
			// If the context was cancelled mid-file, the checkpoint was
			// already saved by the periodic saver; propagate cancel up.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return fmt.Errorf("replaying %s: %w", file, err)
		}

		if r.cfg.AutoDeleteCompleted {
			if err := r.checkpoint.RemoveCompleted(r.cfg.InputDir); err != nil {
				slog.Warn("failed to remove completed files", "err", err)
			}
		}
	}

	slog.Info("replay run complete", "summary", r.reporter.Summary())
	return nil
}

func (r *OfflineReplayer) findLogFiles() ([]string, error) {
	pattern := filepath.Join(r.cfg.InputDir, r.cfg.FilePattern)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("globbing log files: %w", err)
	}
	sort.Strings(files)
	return files, nil
}

func (r *OfflineReplayer) replayFile(ctx context.Context, filePath string) error {
	basename := filepath.Base(filePath)

	// If this file was in_progress from a previous run, restart from the
	// beginning. Mid-file resume is not safe under concurrent session replay
	// (queries from different sessions interleave), so we rely on SELECT
	// idempotency and re-run the whole file.
	if progress := r.checkpoint.GetProgress(basename); progress != nil && progress.Status == "in_progress" {
		slog.Info("restarting in_progress file from beginning",
			"file", basename,
			"previous_processed", progress.LinesReplayed)
	}

	// Mark file in_progress and persist immediately, so a crash before
	// completion leaves a visible marker.
	r.checkpoint.SetProgress(basename, 0)
	if err := r.checkpoint.Save(); err != nil {
		return fmt.Errorf("saving initial checkpoint: %w", err)
	}

	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}
	defer f.Close()

	type sessionEntry struct {
		entry logging.LogEntry
		line  int64
	}
	sessions := make(map[uint64][]sessionEntry)

	bufSize := r.cfg.ScannerBufferSizeBytes
	if bufSize <= 0 {
		bufSize = 1024 * 1024 // 1 MiB default
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, bufSize), bufSize)
	var lineNum int64
	for scanner.Scan() {
		lineNum++
		var entry logging.LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			slog.Warn("skipping malformed line", "line", lineNum, "err", err)
			continue
		}
		sessions[entry.SessionID] = append(sessions[entry.SessionID], sessionEntry{
			entry: entry,
			line:  lineNum,
		})
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning file: %w", err)
	}

	// Periodic checkpoint saver: every checkpointSaveInterval, persist the
	// current processed count so operators have a live view of progress
	// (and so a crash leaves meaningful state for post-mortem).
	var processed atomic.Int64
	stopSaver := make(chan struct{})
	saverDone := make(chan struct{})
	go func() {
		defer close(saverDone)
		ticker := time.NewTicker(checkpointSaveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopSaver:
				return
			case <-ticker.C:
				r.checkpoint.SetProgress(basename, processed.Load())
				if err := r.checkpoint.Save(); err != nil {
					slog.Warn("periodic checkpoint save failed", "err", err)
				}
			}
		}
	}()

	// Replay sessions concurrently.
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, r.cfg.Concurrency)

	for sessionID, entries := range sessions {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(sid uint64, entries []sessionEntry) {
			defer wg.Done()
			defer func() { <-semaphore }()

			conn, err := r.pool.Get()
			if err != nil {
				slog.Error("replay: failed to get connection",
					"session_id", sid, "err", err)
				return
			}
			defer r.pool.Put(conn)

			var prevTimestamp time.Time
			for _, se := range entries {
				if ctx.Err() != nil {
					return
				}
				// Safety: never replay DML/DDL against the target server.
				if !IsReadOnly(se.entry.Query) {
					continue
				}

				if !prevTimestamp.IsZero() && r.speedFactor > 0 {
					gap := se.entry.Timestamp.Sub(prevTimestamp)
					scaledGap := time.Duration(float64(gap) / r.speedFactor)
					if scaledGap > 0 && scaledGap < 10*time.Second {
						select {
						case <-time.After(scaledGap):
						case <-ctx.Done():
							return
						}
					}
				}
				prevTimestamp = se.entry.Timestamp

				replayResult, err := ExecuteAndCapture(conn, se.entry.Query, se.entry.Args...)
				if err != nil {
					slog.Error("replay: execution error",
						"session_id", sid, "err", err)
					continue
				}

				origResult := &compare.CapturedResult{
					AffectedRows: se.entry.RowsAffected,
					Error:        se.entry.Error,
					Duration:     time.Duration(se.entry.ResponseTime * float64(time.Millisecond)),
				}

				cmpResult := r.engine.Compare(origResult, replayResult, se.entry.Query, sid)
				r.reporter.Record(cmpResult)
				processed.Add(1)
			}
		}(sessionID, entries)
	}

	wg.Wait()
	close(stopSaver)
	<-saverDone

	r.checkpoint.MarkCompleted(basename, lineNum)
	if err := r.checkpoint.Save(); err != nil {
		return fmt.Errorf("saving checkpoint: %w", err)
	}

	slog.Info("completed replaying file",
		"file", basename,
		"lines_read", lineNum,
		"queries_replayed", processed.Load())
	return nil
}
