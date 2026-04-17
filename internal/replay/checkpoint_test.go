package replay

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpoint_LoadNonExistent(t *testing.T) {
	cp, err := LoadCheckpoint("/tmp/nonexistent-checkpoint-test.json")
	if err != nil {
		t.Fatalf("expected no error for non-existent file, got: %v", err)
	}
	if len(cp.Files) != 0 {
		t.Errorf("expected empty files map, got %d entries", len(cp.Files))
	}
}

func TestCheckpoint_SaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	cpPath := filepath.Join(tmpDir, "checkpoint.json")

	cp, err := LoadCheckpoint(cpPath)
	if err != nil {
		t.Fatal(err)
	}

	cp.SetProgress("queries-001.jsonl", 500)
	cp.MarkCompleted("queries-000.jsonl", 1000)

	if err := cp.Save(); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// Reload
	cp2, err := LoadCheckpoint(cpPath)
	if err != nil {
		t.Fatalf("reload failed: %v", err)
	}

	if !cp2.IsCompleted("queries-000.jsonl") {
		t.Error("expected queries-000.jsonl to be completed")
	}
	if cp2.IsCompleted("queries-001.jsonl") {
		t.Error("expected queries-001.jsonl to NOT be completed")
	}

	prog := cp2.GetProgress("queries-001.jsonl")
	if prog == nil {
		t.Fatal("expected progress for queries-001.jsonl")
	}
	if prog.LinesReplayed != 500 {
		t.Errorf("expected 500 lines, got %d", prog.LinesReplayed)
	}
}

func TestCheckpoint_RemoveCompleted(t *testing.T) {
	tmpDir := t.TempDir()
	cpPath := filepath.Join(tmpDir, "checkpoint.json")

	// Create a fake log file
	logFile := filepath.Join(tmpDir, "queries-done.jsonl")
	if err := os.WriteFile(logFile, []byte("test"), 0o644); err != nil {
		t.Fatal(err)
	}

	cp, _ := LoadCheckpoint(cpPath)
	cp.MarkCompleted("queries-done.jsonl", 100)
	cp.Save()

	if err := cp.RemoveCompleted(tmpDir); err != nil {
		t.Fatalf("RemoveCompleted failed: %v", err)
	}

	// File should be deleted
	if _, err := os.Stat(logFile); !os.IsNotExist(err) {
		t.Error("expected log file to be deleted after RemoveCompleted")
	}

	// Entry should be removed from checkpoint
	if cp.IsCompleted("queries-done.jsonl") {
		t.Error("expected entry to be removed from checkpoint after RemoveCompleted")
	}
}
