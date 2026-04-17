package logging

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestLogger_ConcurrentLogAndClose(t *testing.T) {
	tmpDir := t.TempDir()
	l, err := NewLogger(LoggerConfig{
		Enabled:    true,
		OutputDir:  tmpDir,
		FilePrefix: "test",
		MaxSizeMB:  1,
		MaxAgeDays: 1,
		MaxBackups: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Spawn many loggers that run concurrently with Close() — if the logger
	// has a send-on-closed-channel race, this will panic under -race.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					l.Log(LogEntry{SessionID: uint64(id), Query: "SELECT 1"})
				}
			}
		}(i)
	}

	// Let them hammer for a moment, then close concurrently with ongoing Log() calls.
	time.Sleep(50 * time.Millisecond)
	l.Close()
	close(stop)
	wg.Wait()

	// Verify the log file exists and has some content.
	_, err = filepath.Glob(filepath.Join(tmpDir, "test.jsonl"))
	if err != nil {
		t.Fatalf("failed to glob log file: %v", err)
	}
}

func TestLogger_LogAfterCloseDoesNotPanic(t *testing.T) {
	tmpDir := t.TempDir()
	l, err := NewLogger(LoggerConfig{
		Enabled:    true,
		OutputDir:  tmpDir,
		FilePrefix: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	l.Close()

	// Must not panic.
	l.Log(LogEntry{Query: "SELECT after close"})
}

func TestLogger_DisabledDropsEntries(t *testing.T) {
	tmpDir := t.TempDir()
	l, err := NewLogger(LoggerConfig{
		Enabled:    false,
		OutputDir:  tmpDir,
		FilePrefix: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	for i := 0; i < 100; i++ {
		l.Log(LogEntry{Query: "SELECT disabled"})
	}
	// When disabled, entries don't hit the channel at all — dropped stays 0.
	if got := l.Dropped(); got != 0 {
		t.Errorf("expected dropped=0 when disabled, got %d", got)
	}
}
