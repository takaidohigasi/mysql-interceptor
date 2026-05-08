package replay

import (
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/takaidohigasi/mysql-interceptor/internal/compare"
)

func ExecuteAndCapture(conn *client.Conn, query string, args ...interface{}) (*compare.CapturedResult, error) {
	start := time.Now()
	result, err := conn.Execute(query, args...)
	duration := time.Since(start)

	captured := &compare.CapturedResult{
		Duration: duration,
	}

	if err != nil {
		captured.Error = err.Error()
		return captured, nil
	}

	captured.AffectedRows = result.AffectedRows

	if result.Resultset != nil {
		// Pre-allocate to the known final length so append doesn't
		// grow-double the underlying array 2-3 times for wider /
		// longer resultsets.
		captured.Columns = make([]string, len(result.Fields))
		for i, field := range result.Fields {
			captured.Columns[i] = string(field.Name)
		}
		captured.Rows = make([][]string, len(result.Values))
		for rowIdx := range result.Values {
			row := make([]string, len(result.Values[rowIdx]))
			for colIdx := range result.Values[rowIdx] {
				row[colIdx] = compare.FormatCellValue(result.Values[rowIdx][colIdx].Value())
			}
			captured.Rows[rowIdx] = row
		}
	}

	return captured, nil
}
