package compare

import (
	"testing"
	"time"
)

func TestCompare_BothSuccess_IdenticalResults(t *testing.T) {
	engine := NewEngine(EngineConfig{
		TimeThresholdMs: 100,
	})

	original := &CapturedResult{
		Columns:      []string{"id", "name"},
		Rows:         [][]string{{"1", "alice"}, {"2", "bob"}},
		AffectedRows: 0,
		Duration:     5 * time.Millisecond,
	}
	replay := &CapturedResult{
		Columns:      []string{"id", "name"},
		Rows:         [][]string{{"1", "alice"}, {"2", "bob"}},
		AffectedRows: 0,
		Duration:     7 * time.Millisecond,
	}

	result := engine.Compare(original, replay, "SELECT id, name FROM users", 1)

	if !result.Match {
		t.Errorf("expected match, got differences: %+v", result.Differences)
	}
	if result.TimeDiffExceed {
		t.Errorf("expected no time threshold exceeded")
	}
}

func TestCompare_OneError_OneSuccess(t *testing.T) {
	engine := NewEngine(EngineConfig{})

	original := &CapturedResult{
		Columns:      []string{"id", "name"},
		Rows:         [][]string{{"1", "alice"}},
		AffectedRows: 0,
		Duration:     5 * time.Millisecond,
	}
	replay := &CapturedResult{
		Error:    "Error 1146 (42S02): Table 'test_db.users' doesn't exist",
		Duration: 2 * time.Millisecond,
	}

	result := engine.Compare(original, replay, "SELECT id, name FROM users", 1)

	if result.Match {
		t.Error("expected mismatch when one returns error and the other succeeds")
	}
	if len(result.Differences) != 1 {
		t.Fatalf("expected 1 difference, got %d", len(result.Differences))
	}
	if result.Differences[0].Type != "error" {
		t.Errorf("expected difference type 'error', got %q", result.Differences[0].Type)
	}
}

func TestCompare_BothError_SameError(t *testing.T) {
	engine := NewEngine(EngineConfig{})

	errMsg := "Error 1146 (42S02): Table 'test_db.users' doesn't exist"
	original := &CapturedResult{Error: errMsg, Duration: 1 * time.Millisecond}
	replay := &CapturedResult{Error: errMsg, Duration: 2 * time.Millisecond}

	result := engine.Compare(original, replay, "SELECT * FROM users", 1)

	if !result.Match {
		t.Error("expected match when both return the same error")
	}
}

func TestCompare_DifferentRowCount(t *testing.T) {
	engine := NewEngine(EngineConfig{})

	original := &CapturedResult{
		Columns: []string{"id"},
		Rows:    [][]string{{"1"}, {"2"}, {"3"}},
	}
	replay := &CapturedResult{
		Columns: []string{"id"},
		Rows:    [][]string{{"1"}, {"2"}},
	}

	result := engine.Compare(original, replay, "SELECT id FROM users", 1)

	if result.Match {
		t.Error("expected mismatch for different row counts")
	}
	found := false
	for _, d := range result.Differences {
		if d.Type == "row_count" {
			found = true
			if d.Original != "3" || d.Replay != "2" {
				t.Errorf("expected row_count 3 vs 2, got %s vs %s", d.Original, d.Replay)
			}
		}
	}
	if !found {
		t.Error("expected row_count difference")
	}
}

func TestCompare_DifferentCellValues(t *testing.T) {
	engine := NewEngine(EngineConfig{})

	original := &CapturedResult{
		Columns: []string{"id", "name"},
		Rows:    [][]string{{"1", "alice"}},
	}
	replay := &CapturedResult{
		Columns: []string{"id", "name"},
		Rows:    [][]string{{"1", "bob"}},
	}

	result := engine.Compare(original, replay, "SELECT id, name FROM users WHERE id=1", 1)

	if result.Match {
		t.Error("expected mismatch for different cell values")
	}
	found := false
	for _, d := range result.Differences {
		if d.Type == "cell_value" && d.Column == "name" {
			found = true
			if d.Original != "alice" || d.Replay != "bob" {
				t.Errorf("expected alice vs bob, got %s vs %s", d.Original, d.Replay)
			}
		}
	}
	if !found {
		t.Error("expected cell_value difference for 'name' column")
	}
}

func TestCompare_IgnoreColumns(t *testing.T) {
	engine := NewEngine(EngineConfig{
		IgnoreColumns: map[string]bool{"updated_at": true},
	})

	original := &CapturedResult{
		Columns: []string{"id", "name", "updated_at"},
		Rows:    [][]string{{"1", "alice", "2026-01-01"}},
	}
	replay := &CapturedResult{
		Columns: []string{"id", "name", "updated_at"},
		Rows:    [][]string{{"1", "alice", "2026-04-17"}},
	}

	result := engine.Compare(original, replay, "SELECT * FROM users WHERE id=1", 1)

	if !result.Match {
		t.Errorf("expected match when only ignored column differs, got: %+v", result.Differences)
	}
}

func TestCompare_TimeThresholdExceeded(t *testing.T) {
	engine := NewEngine(EngineConfig{
		TimeThresholdMs: 50,
	})

	original := &CapturedResult{
		Columns:  []string{"id"},
		Rows:     [][]string{{"1"}},
		Duration: 10 * time.Millisecond,
	}
	replay := &CapturedResult{
		Columns:  []string{"id"},
		Rows:     [][]string{{"1"}},
		Duration: 200 * time.Millisecond,
	}

	result := engine.Compare(original, replay, "SELECT 1", 1)

	if !result.Match {
		t.Error("timing difference should not affect content match")
	}
	if !result.TimeDiffExceed {
		t.Error("expected time threshold to be exceeded")
	}
}

func TestCompare_DifferentAffectedRows(t *testing.T) {
	engine := NewEngine(EngineConfig{})

	original := &CapturedResult{AffectedRows: 5, Duration: 1 * time.Millisecond}
	replay := &CapturedResult{AffectedRows: 3, Duration: 1 * time.Millisecond}

	result := engine.Compare(original, replay, "UPDATE users SET active=1", 1)

	if result.Match {
		t.Error("expected mismatch for different affected rows")
	}
	found := false
	for _, d := range result.Differences {
		if d.Type == "affected_rows" {
			found = true
		}
	}
	if !found {
		t.Error("expected affected_rows difference")
	}
}

func TestCompare_DifferentColumnCount(t *testing.T) {
	engine := NewEngine(EngineConfig{})

	original := &CapturedResult{
		Columns: []string{"id", "name", "email"},
		Rows:    [][]string{{"1", "alice", "a@b.com"}},
	}
	replay := &CapturedResult{
		Columns: []string{"id", "name"},
		Rows:    [][]string{{"1", "alice"}},
	}

	result := engine.Compare(original, replay, "SELECT * FROM users", 1)

	if result.Match {
		t.Error("expected mismatch for different column counts")
	}
}
