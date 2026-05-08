package compare

import (
	"fmt"
	"math"
	"regexp"
	"time"
)

type EngineConfig struct {
	IgnoreColumns    map[string]bool
	TimeThresholdMs  float64
	IgnoreQueryRegex []*regexp.Regexp // if any match, result is marked Ignored

	// RedactColumns is the set of column names whose cell_value diff
	// payloads are masked. The diff record is still emitted (so
	// operators see *that* the column drifted, with type / column /
	// row index / timing intact) but Original and Replay are replaced
	// with redactedPlaceholder. Sibling to logging.RedactArgs.
	RedactColumns map[string]bool

	// RedactAllValues, when true, replaces Original and Replay on
	// every cell_value AND error Difference with redactedPlaceholder
	// regardless of column. Defense-in-depth fallback for cases where
	// RedactColumns might be incomplete.
	RedactAllValues bool
}

// redactedPlaceholder is the string substituted for cell or error
// values when redaction applies. Same wording the audit logger uses
// for prepared-statement bind values (logging.RedactArgs).
const redactedPlaceholder = "<redacted>"

type Engine struct {
	cfg EngineConfig
}

func NewEngine(cfg EngineConfig) *Engine {
	return &Engine{cfg: cfg}
}

// CompileIgnoreQueries compiles a list of string patterns into case-
// insensitive regular expressions. Returns an error if any pattern is
// invalid. Helper for EngineConfig construction from string config.
func CompileIgnoreQueries(patterns []string) ([]*regexp.Regexp, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	out := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile("(?i)" + p)
		if err != nil {
			return nil, fmt.Errorf("compiling ignore pattern %q: %w", p, err)
		}
		out = append(out, re)
	}
	return out, nil
}

// matchesIgnore reports whether the query matches any ignore pattern.
func (e *Engine) matchesIgnore(query string) bool {
	for _, re := range e.cfg.IgnoreQueryRegex {
		if re.MatchString(query) {
			return true
		}
	}
	return false
}

// redactCell returns the value to record for a cell_value diff,
// applying RedactAllValues (global) or RedactColumns (per-column)
// when appropriate. The returned string is what lands in the JSON.
func (e *Engine) redactCell(column, value string) string {
	if e.cfg.RedactAllValues {
		return redactedPlaceholder
	}
	if e.cfg.RedactColumns[column] {
		return redactedPlaceholder
	}
	return value
}

// redactError returns the value to record for an error diff. Only
// RedactAllValues applies — error messages aren't tied to a single
// column, so per-column redaction would be ambiguous; the global
// switch is the only knob that suppresses them.
func (e *Engine) redactError(value string) string {
	if e.cfg.RedactAllValues {
		return redactedPlaceholder
	}
	return value
}

func (e *Engine) Compare(original, replay *CapturedResult, query, user string, sessionID uint64) *CompareResult {
	result := &CompareResult{
		Query:          query,
		QueryDigest:    Digest(query),
		SessionID:      sessionID,
		Timestamp:      time.Now(),
		Match:          true,
		Ignored:        e.matchesIgnore(query),
		OriginalTimeMs: float64(original.Duration.Microseconds()) / 1000.0,
		ReplayTimeMs:   float64(replay.Duration.Microseconds()) / 1000.0,
	}
	// User is attached only when the result is a real divergence
	// (Match=false and not in the ignore list). Matched and ignored
	// records aren't worth carrying per-query identity for, and this
	// keeps the diff report focused on actionable lines. Defer over
	// the multiple early-return paths so every branch shares the
	// same rule.
	defer func() {
		if !result.Match && !result.Ignored {
			result.User = user
		}
	}()
	result.TimeDiffMs = result.ReplayTimeMs - result.OriginalTimeMs

	if e.cfg.TimeThresholdMs > 0 && math.Abs(result.TimeDiffMs) > e.cfg.TimeThresholdMs {
		result.TimeDiffExceed = true
	}

	// Compare errors
	if original.Error != replay.Error {
		result.Match = false
		result.Differences = append(result.Differences, Difference{
			Type:     "error",
			Original: e.redactError(original.Error),
			Replay:   e.redactError(replay.Error),
		})
		return result
	}

	// If both errored, no further comparison needed
	if original.Error != "" {
		return result
	}

	// Compare affected rows (for DML)
	if original.AffectedRows != replay.AffectedRows {
		result.Match = false
		result.Differences = append(result.Differences, Difference{
			Type:     "affected_rows",
			Original: fmt.Sprintf("%d", original.AffectedRows),
			Replay:   fmt.Sprintf("%d", replay.AffectedRows),
		})
	}

	// Compare column names
	if len(original.Columns) != len(replay.Columns) {
		result.Match = false
		result.Differences = append(result.Differences, Difference{
			Type:     "column_count",
			Original: fmt.Sprintf("%d", len(original.Columns)),
			Replay:   fmt.Sprintf("%d", len(replay.Columns)),
		})
		return result
	}

	for i, col := range original.Columns {
		if col != replay.Columns[i] {
			result.Match = false
			result.Differences = append(result.Differences, Difference{
				Type:     "column_name",
				Column:   fmt.Sprintf("index_%d", i),
				Original: col,
				Replay:   replay.Columns[i],
			})
		}
	}

	// Compare row count
	if len(original.Rows) != len(replay.Rows) {
		result.Match = false
		result.Differences = append(result.Differences, Difference{
			Type:     "row_count",
			Original: fmt.Sprintf("%d", len(original.Rows)),
			Replay:   fmt.Sprintf("%d", len(replay.Rows)),
		})
		return result
	}

	// Compare cell values
	for rowIdx := range original.Rows {
		origRow := original.Rows[rowIdx]
		replayRow := replay.Rows[rowIdx]
		for colIdx := range origRow {
			colName := ""
			if colIdx < len(original.Columns) {
				colName = original.Columns[colIdx]
			}
			if e.cfg.IgnoreColumns[colName] {
				continue
			}
			if origRow[colIdx] != replayRow[colIdx] {
				result.Match = false
				result.Differences = append(result.Differences, Difference{
					Type:     "cell_value",
					Row:      rowIdx,
					Column:   colName,
					Original: e.redactCell(colName, origRow[colIdx]),
					Replay:   e.redactCell(colName, replayRow[colIdx]),
				})
			}
		}
	}

	return result
}
