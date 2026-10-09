package logging

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/takaidohigasi/mysql-interceptor/internal/metrics"
	"gopkg.in/lumberjack.v2"
)

type Logger struct {
	entryCh    chan LogEntry
	stop       chan struct{}
	done       chan struct{}
	writer     *lumberjack.Logger
	enabled    atomic.Bool
	errorsOnly atomic.Bool
	closed     atomic.Bool
	dropped    atomic.Int64
	once       sync.Once
}

type LoggerConfig struct {
	Enabled bool
	// Level is "all" (default) or "error". See Logger.SetLevel.
	Level      string
	OutputDir  string
	FilePrefix string
	QueueSize  int // channel buffer size; 0 → default 10000
	MaxSizeMB  int
	MaxAgeDays int
	MaxBackups int
	Compress   bool
}

func NewLogger(cfg LoggerConfig) (*Logger, error) {
	if cfg.OutputDir != "" {
		if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
			return nil, fmt.Errorf("creating log output dir: %w", err)
		}
	}

	filename := filepath.Join(cfg.OutputDir, cfg.FilePrefix+".jsonl")

	lj := &lumberjack.Logger{
		Filename:   filename,
		MaxSize:    cfg.MaxSizeMB,
		MaxAge:     cfg.MaxAgeDays,
		MaxBackups: cfg.MaxBackups,
		Compress:   cfg.Compress,
		LocalTime:  true,
	}

	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = 10000
	}
	l := &Logger{
		entryCh: make(chan LogEntry, queueSize),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		writer:  lj,
	}
	l.enabled.Store(cfg.Enabled)
	l.errorsOnly.Store(cfg.Level == "error")

	go l.writeLoop()

	return l, nil
}

func (l *Logger) Log(entry LogEntry) {
	if l.closed.Load() || !l.enabled.Load() {
		return
	}

	// Level=error: skip entries from queries that didn't error, before
	// they ever reach the channel. Cuts volume during a noisy incident
	// while keeping every failure.
	if l.errorsOnly.Load() && entry.Error == "" {
		return
	}

	// Non-blocking send: if the writer goroutine has already exited or the
	// buffer is full, drop the entry rather than blocking the caller or
	// risking a deadlock during shutdown.
	select {
	case l.entryCh <- entry:
	case <-l.stop:
		l.dropped.Add(1)
		metrics.Global.LoggerDropped.Add(1)
	default:
		l.dropped.Add(1)
		metrics.Global.LoggerDropped.Add(1)
	}
}

func (l *Logger) SetEnabled(enabled bool) {
	l.enabled.Store(enabled)
	slog.Info("sql logging toggled", "enabled", enabled)
}

// SetLevel changes which entries Log records: "error" keeps only entries
// whose query returned a backend error, "all" (or any other value) keeps
// every entry. Hot-reloadable, same as SetEnabled.
func (l *Logger) SetLevel(level string) {
	errorsOnly := level == "error"
	l.errorsOnly.Store(errorsOnly)
	slog.Info("sql logging level changed", "level", level, "errors_only", errorsOnly)
}

func (l *Logger) Dropped() int64 {
	return l.dropped.Load()
}

func (l *Logger) Close() {
	l.once.Do(func() {
		l.closed.Store(true)
		close(l.stop)
		<-l.done
		l.writer.Close()
	})
}

func (l *Logger) writeLoop() {
	defer close(l.done)

	enc := json.NewEncoder(l.writer)
	enc.SetEscapeHTML(false)

	for {
		select {
		case entry := <-l.entryCh:
			if !l.enabled.Load() {
				continue
			}
			if err := enc.Encode(entry); err != nil {
				slog.Error("failed to write sql log entry", "err", err)
			}
		case <-l.stop:
			// Drain any remaining buffered entries, then exit.
			for {
				select {
				case entry := <-l.entryCh:
					if l.enabled.Load() {
						if err := enc.Encode(entry); err != nil {
							slog.Error("failed to write sql log entry", "err", err)
						}
					}
				default:
					return
				}
			}
		}
	}
}
