package replay

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Checkpoint struct {
	Files       map[string]*FileProgress `json:"files"`
	LastUpdated time.Time                `json:"last_updated"`
	path        string
}

type FileProgress struct {
	Status        string `json:"status"` // "in_progress", "completed"
	LinesReplayed int64  `json:"lines_replayed"`
}

func LoadCheckpoint(path string) (*Checkpoint, error) {
	cp := &Checkpoint{
		Files: make(map[string]*FileProgress),
		path:  path,
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cp, nil
		}
		return nil, fmt.Errorf("reading checkpoint: %w", err)
	}

	if err := json.Unmarshal(data, cp); err != nil {
		return nil, fmt.Errorf("parsing checkpoint: %w", err)
	}
	cp.path = path

	return cp, nil
}

func (cp *Checkpoint) GetProgress(filename string) *FileProgress {
	return cp.Files[filename]
}

func (cp *Checkpoint) SetProgress(filename string, linesReplayed int64) {
	cp.Files[filename] = &FileProgress{
		Status:        "in_progress",
		LinesReplayed: linesReplayed,
	}
}

func (cp *Checkpoint) MarkCompleted(filename string, totalLines int64) {
	cp.Files[filename] = &FileProgress{
		Status:        "completed",
		LinesReplayed: totalLines,
	}
}

func (cp *Checkpoint) IsCompleted(filename string) bool {
	fp := cp.Files[filename]
	return fp != nil && fp.Status == "completed"
}

func (cp *Checkpoint) Save() error {
	cp.LastUpdated = time.Now()

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling checkpoint: %w", err)
	}

	// Write atomically: write to temp file, then rename
	tmpPath := cp.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("writing checkpoint: %w", err)
	}
	if err := os.Rename(tmpPath, cp.path); err != nil {
		return fmt.Errorf("renaming checkpoint: %w", err)
	}

	return nil
}

func (cp *Checkpoint) RemoveCompleted(inputDir string) error {
	for filename, fp := range cp.Files {
		if fp.Status != "completed" {
			continue
		}
		fullPath := filepath.Join(inputDir, filename)
		if err := os.Remove(fullPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("removing completed file %s: %w", fullPath, err)
		}
		delete(cp.Files, filename)
	}
	return cp.Save()
}
