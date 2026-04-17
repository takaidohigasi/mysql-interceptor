package replay

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/takaidohigasi/mysql-interceptor/internal/backend"
	"github.com/takaidohigasi/mysql-interceptor/internal/compare"
	"github.com/takaidohigasi/mysql-interceptor/internal/config"
	"github.com/takaidohigasi/mysql-interceptor/internal/logging"
)

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
		config.BackendSideTLSConfig{},
		cfg.Concurrency,
	)

	ignoreColumns := make(map[string]bool)
	for _, col := range compareCfg.IgnoreColumns {
		ignoreColumns[col] = true
	}

	engine := compare.NewEngine(compare.EngineConfig{
		IgnoreColumns:   ignoreColumns,
		TimeThresholdMs: compareCfg.TimeThresholdMs,
	})

	reporter, err := compare.NewReporter(compareCfg.OutputFile)
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

func (r *OfflineReplayer) Run() error {
	defer r.pool.Close()
	defer r.reporter.Close()

	files, err := r.findLogFiles()
	if err != nil {
		return err
	}

	if len(files) == 0 {
		log.Println("no log files found to replay")
		return nil
	}

	log.Printf("found %d log files to replay", len(files))

	for _, file := range files {
		basename := filepath.Base(file)
		if r.checkpoint.IsCompleted(basename) {
			log.Printf("skipping completed file: %s", basename)
			continue
		}

		log.Printf("replaying file: %s", basename)
		if err := r.replayFile(file); err != nil {
			return fmt.Errorf("replaying %s: %w", file, err)
		}

		if r.cfg.AutoDeleteCompleted {
			if err := r.checkpoint.RemoveCompleted(r.cfg.InputDir); err != nil {
				log.Printf("warning: failed to remove completed files: %v", err)
			}
		}
	}

	log.Println(r.reporter.Summary())
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

func (r *OfflineReplayer) replayFile(filePath string) error {
	basename := filepath.Base(filePath)

	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}
	defer f.Close()

	// Resume from checkpoint if available
	var startOffset int64
	var startLine int64
	if progress := r.checkpoint.GetProgress(basename); progress != nil && progress.Status == "in_progress" {
		startOffset = progress.ByteOffset
		startLine = progress.LinesReplayed
		if _, err := f.Seek(startOffset, 0); err != nil {
			return fmt.Errorf("seeking to checkpoint offset: %w", err)
		}
		log.Printf("resuming from line %d (byte offset %d)", startLine, startOffset)
	}

	// Group entries by session for ordered replay
	type sessionEntry struct {
		entry  logging.LogEntry
		offset int64
		line   int64
	}
	sessions := make(map[uint64][]sessionEntry)

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB buffer
	lineNum := startLine
	currentOffset := startOffset

	for scanner.Scan() {
		lineNum++
		lineBytes := scanner.Bytes()
		currentOffset += int64(len(lineBytes)) + 1 // +1 for newline

		var entry logging.LogEntry
		if err := json.Unmarshal(lineBytes, &entry); err != nil {
			log.Printf("skipping malformed line %d: %v", lineNum, err)
			continue
		}

		sessions[entry.SessionID] = append(sessions[entry.SessionID], sessionEntry{
			entry:  entry,
			offset: currentOffset,
			line:   lineNum,
		})
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning file: %w", err)
	}

	// Replay sessions concurrently
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, r.cfg.Concurrency)
	var lastLine int64
	var lastOffset int64
	var mu sync.Mutex

	for sessionID, entries := range sessions {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(sid uint64, entries []sessionEntry) {
			defer wg.Done()
			defer func() { <-semaphore }()

			conn, err := r.pool.Get()
			if err != nil {
				log.Printf("[replay:session:%d] failed to get connection: %v", sid, err)
				return
			}
			defer r.pool.Put(conn)

			var prevTimestamp time.Time
			for _, se := range entries {
				// Respect timing gaps (scaled by speed factor)
				if !prevTimestamp.IsZero() && r.speedFactor > 0 {
					gap := se.entry.Timestamp.Sub(prevTimestamp)
					scaledGap := time.Duration(float64(gap) / r.speedFactor)
					if scaledGap > 0 && scaledGap < 10*time.Second {
						time.Sleep(scaledGap)
					}
				}
				prevTimestamp = se.entry.Timestamp

				replayResult, err := ExecuteAndCapture(conn, se.entry.Query)
				if err != nil {
					log.Printf("[replay:session:%d] execution error: %v", sid, err)
					continue
				}

				// Build original result from log entry for comparison
				origResult := &compare.CapturedResult{
					AffectedRows: se.entry.RowsAffected,
					Error:        se.entry.Error,
					Duration:     time.Duration(se.entry.ResponseTime * float64(time.Millisecond)),
				}

				cmpResult := r.engine.Compare(origResult, replayResult, se.entry.Query, sid)
				r.reporter.Record(cmpResult)

				mu.Lock()
				if se.line > lastLine {
					lastLine = se.line
					lastOffset = se.offset
				}
				mu.Unlock()
			}
		}(sessionID, entries)
	}

	wg.Wait()

	// Mark file as completed
	r.checkpoint.MarkCompleted(basename, lineNum)
	if err := r.checkpoint.Save(); err != nil {
		return fmt.Errorf("saving checkpoint: %w", err)
	}

	_ = lastOffset // used for intermediate checkpointing if needed in the future

	log.Printf("completed replaying %s: %d lines processed", basename, lineNum-startLine)
	return nil
}
