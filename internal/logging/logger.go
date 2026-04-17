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
	writer  *lumberjack.Logger
	enabled atomic.Bool
	dropped atomic.Int64
	done    chan struct{}
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
		writer:  lj,
		done:    make(chan struct{}),
	}
	l.enabled.Store(cfg.Enabled)

	go l.writeLoop()

	return l, nil
}

func (l *Logger) Log(entry LogEntry) {
	if !l.enabled.Load() {
		return
	}

	select {
	case l.entryCh <- entry:
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
		close(l.entryCh)
		<-l.done
		l.writer.Close()
	})
}

func (l *Logger) writeLoop() {
	defer close(l.done)

	enc := json.NewEncoder(l.writer)
	enc.SetEscapeHTML(false)

	for entry := range l.entryCh {
		if !l.enabled.Load() {
			continue
		}
		if err := enc.Encode(entry); err != nil {
			log.Printf("failed to write log entry: %v", err)
		}
	}
}
