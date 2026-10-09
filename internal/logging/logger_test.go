package logging

import (
	"bufio"
	"encoding/json"
	"os"
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

func TestLogger_LevelErrorOnlyRecordsFailures(t *testing.T) {
	tmpDir := t.TempDir()
	l, err := NewLogger(LoggerConfig{
		Enabled:    true,
		Level:      "error",
		OutputDir:  tmpDir,
		FilePrefix: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		l.Log(LogEntry{Query: "SELECT ok"})
	}
	l.Log(LogEntry{Query: "SELECT boom", Error: "syntax error"})
	l.Log(LogEntry{Query: "SELECT boom2", Error: "timeout"})

	l.Close()

	entries := readLogEntries(t, filepath.Join(tmpDir, "test.jsonl"))
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries written, got %d: %+v", len(entries), entries)
	}
	for _, e := range entries {
		if e.Error == "" {
			t.Errorf("expected only error entries to be written, got %+v", e)
		}
	}
}

func TestLogger_SetLevelHotToggle(t *testing.T) {
	tmpDir := t.TempDir()
	l, err := NewLogger(LoggerConfig{
		Enabled:    true,
		OutputDir:  tmpDir,
		FilePrefix: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Level defaults to "all" (empty string behaves the same as "all").
	l.Log(LogEntry{Query: "SELECT before toggle"})

	l.SetLevel("error")
	l.Log(LogEntry{Query: "SELECT after toggle, no error"})

	l.Close()

	entries := readLogEntries(t, filepath.Join(tmpDir, "test.jsonl"))
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry written, got %d: %+v", len(entries), entries)
	}
	if entries[0].Query != "SELECT before toggle" {
		t.Errorf("expected the pre-toggle entry to survive, got %+v", entries[0])
	}
}

// readLogEntries reads a JSONL file written by Logger and decodes each line.
func readLogEntries(t *testing.T, path string) []LogEntry {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening log file: %v", err)
	}
	defer f.Close()

	var entries []LogEntry
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e LogEntry
		if err := json.Unmarshal(line, &e); err != nil {
			t.Fatalf("decoding log entry %q: %v", line, err)
		}
		entries = append(entries, e)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scanning log file: %v", err)
	}
	return entries
}
