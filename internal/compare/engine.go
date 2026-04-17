package compare

import (
	"fmt"
	"math"
	"time"
)

type EngineConfig struct {
	IgnoreColumns   map[string]bool
	TimeThresholdMs float64
}

type Engine struct {
	cfg EngineConfig
}

func NewEngine(cfg EngineConfig) *Engine {
	return &Engine{cfg: cfg}
}

func (e *Engine) Compare(original, replay *CapturedResult, query string, sessionID uint64) *CompareResult {
	result := &CompareResult{
		Query:          query,
		SessionID:      sessionID,
		Timestamp:      time.Now(),
		Match:          true,
		OriginalTimeMs: float64(original.Duration.Microseconds()) / 1000.0,
		ReplayTimeMs:   float64(replay.Duration.Microseconds()) / 1000.0,
	}
	result.TimeDiffMs = result.ReplayTimeMs - result.OriginalTimeMs

	if e.cfg.TimeThresholdMs > 0 && math.Abs(result.TimeDiffMs) > e.cfg.TimeThresholdMs {
		result.TimeDiffExceed = true
	}

	// Compare errors
	if original.Error != replay.Error {
		result.Match = false
		result.Differences = append(result.Differences, Difference{
			Type:     "error",
			Original: original.Error,
			Replay:   replay.Error,
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
					Original: origRow[colIdx],
					Replay:   replayRow[colIdx],
				})
			}
		}
	}

	return result
}
