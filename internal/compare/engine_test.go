package compare

import (
	"encoding/json"
	"strings"
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

	result := engine.Compare(original, replay, "SELECT id, name FROM users", "", 1)

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

	result := engine.Compare(original, replay, "SELECT id, name FROM users", "", 1)

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

	result := engine.Compare(original, replay, "SELECT * FROM users", "", 1)

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

	result := engine.Compare(original, replay, "SELECT id FROM users", "", 1)

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

	result := engine.Compare(original, replay, "SELECT id, name FROM users WHERE id=1", "", 1)

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

	result := engine.Compare(original, replay, "SELECT * FROM users WHERE id=1", "", 1)

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

	result := engine.Compare(original, replay, "SELECT 1", "", 1)

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

	result := engine.Compare(original, replay, "UPDATE users SET active=1", "", 1)

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

func TestCompare_IgnoreQueryPattern(t *testing.T) {
	regexes, err := CompileIgnoreQueries([]string{
		"@@server_uuid",
		"\\bNOW\\s*\\(",
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	engine := NewEngine(EngineConfig{IgnoreQueryRegex: regexes})

	original := &CapturedResult{
		Columns: []string{"uuid"},
		Rows:    [][]string{{"aaa-bbb"}},
	}
	replay := &CapturedResult{
		Columns: []string{"uuid"},
		Rows:    [][]string{{"ccc-ddd"}},
	}

	// Different cell values, but query matches ignore pattern — still
	// reported with the diff details, but Ignored=true.
	result := engine.Compare(original, replay, "SELECT @@server_uuid", "", 1)
	if !result.Ignored {
		t.Error("expected Ignored=true for @@server_uuid query")
	}
	// The diff is still captured for audit purposes.
	foundCellDiff := false
	for _, d := range result.Differences {
		if d.Type == "cell_value" {
			foundCellDiff = true
		}
	}
	if !foundCellDiff {
		t.Error("expected cell_value diff to still be recorded on ignored results")
	}

	// NOW() with whitespace — regex boundary check.
	result2 := engine.Compare(original, replay, "select now()", "", 2)
	if !result2.Ignored {
		t.Error("expected Ignored=true for NOW() query")
	}

	// A query NOT in the ignore list — normal mismatch behavior.
	result3 := engine.Compare(original, replay, "SELECT name FROM users", "", 3)
	if result3.Ignored {
		t.Error("expected Ignored=false for non-matching query")
	}
	if result3.Match {
		t.Error("expected Match=false for non-matching query with cell diff")
	}
}

func TestCompileIgnoreQueries_InvalidRegex(t *testing.T) {
	_, err := CompileIgnoreQueries([]string{"[unclosed"})
	if err == nil {
		t.Error("expected error for invalid regex")
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

	result := engine.Compare(original, replay, "SELECT * FROM users", "", 1)

	if result.Match {
		t.Error("expected mismatch for different column counts")
	}
}

// TestCompare_UserOnlyOnDiverging verifies the User field is set only
// when the comparison represents an actual divergence (Match=false and
// not in the ignore list). Matched results omit the user; ignored
// results omit it too — operators only need user identity on the lines
// they're going to investigate.
func TestCompare_UserOnlyOnDiverging(t *testing.T) {
	t.Run("diff: user is set", func(t *testing.T) {
		engine := NewEngine(EngineConfig{})
		original := &CapturedResult{Columns: []string{"id"}, Rows: [][]string{{"1"}}}
		replay := &CapturedResult{Columns: []string{"id"}, Rows: [][]string{{"2"}}}

		result := engine.Compare(original, replay, "SELECT id FROM users", "alice", 1)
		if result.Match {
			t.Fatal("expected Match=false for diverging cells")
		}
		if result.User != "alice" {
			t.Errorf("expected User=alice on diverging result, got %q", result.User)
		}
		b, _ := json.Marshal(result)
		if !strings.Contains(string(b), `"user":"alice"`) {
			t.Errorf("expected JSON to contain user field, got: %s", b)
		}
	})

	t.Run("match: user is omitted", func(t *testing.T) {
		engine := NewEngine(EngineConfig{})
		original := &CapturedResult{Columns: []string{"id"}, Rows: [][]string{{"1"}}}
		replay := &CapturedResult{Columns: []string{"id"}, Rows: [][]string{{"1"}}}

		result := engine.Compare(original, replay, "SELECT id FROM users", "alice", 1)
		if !result.Match {
			t.Fatal("expected Match=true for identical results")
		}
		if result.User != "" {
			t.Errorf("expected User empty on matching result, got %q", result.User)
		}
		b, _ := json.Marshal(result)
		if strings.Contains(string(b), `"user"`) {
			t.Errorf("expected JSON to omit user on match, got: %s", b)
		}
	})

	t.Run("ignored: user is omitted even when results diverge", func(t *testing.T) {
		// "@@server_uuid" matches the configured ignore pattern, so the
		// engine flags the result as Ignored. Even though the cells differ,
		// User must not be attached.
		ignoreRegexes, err := CompileIgnoreQueries([]string{"@@server_uuid"})
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		engine := NewEngine(EngineConfig{IgnoreQueryRegex: ignoreRegexes})
		original := &CapturedResult{Columns: []string{"v"}, Rows: [][]string{{"a-1"}}}
		replay := &CapturedResult{Columns: []string{"v"}, Rows: [][]string{{"a-2"}}}

		result := engine.Compare(original, replay, "SELECT @@server_uuid", "alice", 1)
		if !result.Ignored {
			t.Fatal("expected Ignored=true for matching ignore pattern")
		}
		if result.User != "" {
			t.Errorf("expected User empty on ignored result, got %q", result.User)
		}
	})
}

// findDiff returns the first Difference whose Type matches typ, or
// (Difference{}, false) if none. Helper for the redaction tests.
func findDiff(diffs []Difference, typ string) (Difference, bool) {
	for _, d := range diffs {
		if d.Type == typ {
			return d, true
		}
	}
	return Difference{}, false
}

// TestCompare_RedactColumns covers the per-column cell_value
// redaction path: a cell whose column is in RedactColumns has its
// Original / Replay values replaced with "<redacted>", but the diff
// is still recorded with the column name and row index intact.
func TestCompare_RedactColumns(t *testing.T) {
	engine := NewEngine(EngineConfig{
		RedactColumns: map[string]bool{"hashed_password": true},
	})
	original := &CapturedResult{
		Columns: []string{"id", "hashed_password", "email"},
		Rows:    [][]string{{"1", "old-hash-AAA", "alice@example.com"}},
	}
	replay := &CapturedResult{
		Columns: []string{"id", "hashed_password", "email"},
		Rows:    [][]string{{"1", "new-hash-BBB", "alice2@example.com"}},
	}

	result := engine.Compare(original, replay, "SELECT * FROM users WHERE id=1", "", 1)
	if result.Match {
		t.Fatal("expected Match=false for differing rows")
	}

	hp, ok := findDiff(result.Differences, "cell_value")
	if !ok {
		t.Fatal("expected at least one cell_value diff")
	}
	// Walk all cell_value diffs; hashed_password values must be redacted,
	// email values must not (column not in the redact list).
	hpCount, emailCount, redactedHP, leakedEmail := 0, 0, false, false
	for _, d := range result.Differences {
		if d.Type != "cell_value" {
			continue
		}
		switch d.Column {
		case "hashed_password":
			hpCount++
			if d.Original == "<redacted>" && d.Replay == "<redacted>" {
				redactedHP = true
			}
			if d.Original == "old-hash-AAA" || d.Replay == "new-hash-BBB" {
				t.Errorf("hashed_password values leaked through redaction: orig=%q replay=%q", d.Original, d.Replay)
			}
		case "email":
			emailCount++
			if d.Original == "alice@example.com" && d.Replay == "alice2@example.com" {
				leakedEmail = false // expected, not redacted
			} else if d.Original == "<redacted>" || d.Replay == "<redacted>" {
				leakedEmail = true
				t.Errorf("email was redacted but column not in RedactColumns; orig=%q replay=%q", d.Original, d.Replay)
			}
		}
	}
	if hpCount != 1 || !redactedHP {
		t.Errorf("expected exactly one redacted hashed_password diff, got count=%d redacted=%v", hpCount, redactedHP)
	}
	if emailCount != 1 || leakedEmail {
		t.Errorf("expected unredacted email diff, got count=%d leaked=%v", emailCount, leakedEmail)
	}
	// The diff record itself (column, row, type) must survive redaction
	// — that's the point of redact vs ignore.
	if hp.Column != "hashed_password" || hp.Type != "cell_value" {
		t.Errorf("redacted diff dropped its metadata: %+v", hp)
	}
}

// TestCompare_RedactAllValues confirms the global override redacts
// every cell_value AND error diff regardless of column.
func TestCompare_RedactAllValues(t *testing.T) {
	t.Run("cell_value", func(t *testing.T) {
		engine := NewEngine(EngineConfig{RedactAllValues: true})
		original := &CapturedResult{
			Columns: []string{"id", "name"},
			Rows:    [][]string{{"1", "alice"}},
		}
		replay := &CapturedResult{
			Columns: []string{"id", "name"},
			Rows:    [][]string{{"2", "bob"}},
		}
		result := engine.Compare(original, replay, "SELECT * FROM users", "", 1)
		if result.Match {
			t.Fatal("expected Match=false")
		}
		for _, d := range result.Differences {
			if d.Type != "cell_value" {
				continue
			}
			if d.Original != "<redacted>" || d.Replay != "<redacted>" {
				t.Errorf("cell_value not redacted under RedactAllValues: %+v", d)
			}
		}
	})

	t.Run("error", func(t *testing.T) {
		engine := NewEngine(EngineConfig{RedactAllValues: true})
		original := &CapturedResult{
			Error: "Duplicate entry 'foo@bar.com' for key 'email'",
		}
		replay := &CapturedResult{
			Error: "Duplicate entry 'baz@qux.com' for key 'email'",
		}
		result := engine.Compare(original, replay, "INSERT INTO users (email) VALUES (?)", "", 1)
		if result.Match {
			t.Fatal("expected Match=false on differing error strings")
		}
		err, ok := findDiff(result.Differences, "error")
		if !ok {
			t.Fatal("expected an error diff")
		}
		if err.Original != "<redacted>" || err.Replay != "<redacted>" {
			t.Errorf("error values not redacted under RedactAllValues: orig=%q replay=%q", err.Original, err.Replay)
		}
	})
}

// TestCompare_RedactErrorOnlyUnderGlobalSwitch confirms RedactColumns
// alone does NOT redact error diffs (errors aren't tied to a column).
func TestCompare_RedactErrorOnlyUnderGlobalSwitch(t *testing.T) {
	engine := NewEngine(EngineConfig{
		RedactColumns: map[string]bool{"email": true},
	})
	original := &CapturedResult{Error: "Duplicate entry 'foo@bar.com' for key 'email'"}
	replay := &CapturedResult{Error: "Duplicate entry 'baz@qux.com' for key 'email'"}

	result := engine.Compare(original, replay, "INSERT INTO users (email) VALUES (?)", "", 1)
	err, ok := findDiff(result.Differences, "error")
	if !ok {
		t.Fatal("expected an error diff")
	}
	if err.Original != "Duplicate entry 'foo@bar.com' for key 'email'" {
		t.Errorf("error.Original was unexpectedly redacted: %q", err.Original)
	}
}

// TestCompare_RedactColumnsAndAllValues confirms that when both
// switches are on, RedactAllValues wins (everything is redacted).
func TestCompare_RedactColumnsAndAllValues(t *testing.T) {
	engine := NewEngine(EngineConfig{
		RedactColumns:   map[string]bool{"hashed_password": true},
		RedactAllValues: true,
	})
	original := &CapturedResult{
		Columns: []string{"id", "name", "hashed_password"},
		Rows:    [][]string{{"1", "alice", "AAA"}},
	}
	replay := &CapturedResult{
		Columns: []string{"id", "name", "hashed_password"},
		Rows:    [][]string{{"2", "bob", "BBB"}},
	}
	result := engine.Compare(original, replay, "SELECT * FROM users", "", 1)
	for _, d := range result.Differences {
		if d.Type != "cell_value" {
			continue
		}
		if d.Original != "<redacted>" || d.Replay != "<redacted>" {
			t.Errorf("expected all cell_value diffs redacted under RedactAllValues, got column=%q orig=%q replay=%q", d.Column, d.Original, d.Replay)
		}
	}
}
