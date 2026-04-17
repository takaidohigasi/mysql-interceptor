package replay

import (
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/takaidohigasi/mysql-interceptor/internal/compare"
)

func ExecuteAndCapture(conn *client.Conn, query string) (*compare.CapturedResult, error) {
	start := time.Now()
	result, err := conn.Execute(query)
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
		for _, field := range result.Fields {
			captured.Columns = append(captured.Columns, string(field.Name))
		}

		for rowIdx := 0; rowIdx < len(result.Values); rowIdx++ {
			row := make([]string, len(result.Values[rowIdx]))
			for colIdx := range result.Values[rowIdx] {
				row[colIdx] = compare.FormatCellValue(result.Values[rowIdx][colIdx].Value())
			}
			captured.Rows = append(captured.Rows, row)
		}
	}

	return captured, nil
}
