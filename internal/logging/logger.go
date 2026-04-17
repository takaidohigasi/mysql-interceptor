package logging

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"gopkg.in/lumberjack.v2"
)

type Logger struct {
	entryCh chan LogEntry
	stop    chan struct{}
	done    chan struct{}
	writer  *lumberjack.Logger
	enabled atomic.Bool
	closed  atomic.Bool
	dropped atomic.Int64
	once    sync.Once
}

type LoggerConfig struct {
	Enabled    bool
	OutputDir  string
	FilePrefix string
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

	l := &Logger{
		entryCh: make(chan LogEntry, 10000),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		writer:  lj,
	}
	l.enabled.Store(cfg.Enabled)

	go l.writeLoop()

	return l, nil
}

func (l *Logger) Log(entry LogEntry) {
	if l.closed.Load() || !l.enabled.Load() {
		return
	}

	// Non-blocking send: if the writer goroutine has already exited or the
	// buffer is full, drop the entry rather than blocking the caller or
	// risking a deadlock during shutdown.
	select {
	case l.entryCh <- entry:
	case <-l.stop:
		l.dropped.Add(1)
	default:
		l.dropped.Add(1)
	}
}

func (l *Logger) SetEnabled(enabled bool) {
	l.enabled.Store(enabled)
	log.Printf("SQL logging %s", map[bool]string{true: "enabled", false: "disabled"}[enabled])
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
				log.Printf("failed to write log entry: %v", err)
			}
		case <-l.stop:
			// Drain any remaining buffered entries, then exit.
			for {
				select {
				case entry := <-l.entryCh:
					if l.enabled.Load() {
						if err := enc.Encode(entry); err != nil {
							log.Printf("failed to write log entry: %v", err)
						}
					}
				default:
					return
				}
			}
		}
	}
}
