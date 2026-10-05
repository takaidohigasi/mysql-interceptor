package replay

import (
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/takaidohigasi/mysql-interceptor/internal/compare"
)

// ExecuteAndCapture runs query on conn and returns both the captured
// result and the raw error from go-mysql. Callers should expect:
//
//   - On Execute success: (captured, nil). captured has Columns, Rows,
//     AffectedRows populated; captured.Error is "".
//
//   - On Execute failure: (captured, err). captured.Error carries the
//     error message string (so engine.Compare can still produce a
//     comparison record using primary's Error vs replay's Error),
//     AND err is returned non-nil. Callers that need to distinguish
//     transport-level failures (i/o timeout, broken pipe, server
//     RST) from server-returned SQL errors (Table doesn't exist,
//     syntax error, dup key) should `errors.As(err, &*mysql.MyError{})`
//     — see replay.isTransportError.
//
// Prior to this change ExecuteAndCapture silently dropped err and
// returned (captured, nil) on Execute failure. That made it
// impossible for ShadowSession.recordResult to decide whether the
// underlying connection was poisoned and needed teardown — PR #28's
// transport-error teardown branch was effectively dead code because
// `res.err` was always nil. Returning err alongside captured fixes
// that without changing the diff-report shape: engine.Compare still
// sees captured.Error and still emits the same "error"-type
// Difference, and callers that want error-as-control-flow now have
// it. Empirical post-mortem in kouzoh/microservices#29641.
func ExecuteAndCapture(conn *client.Conn, query string, args ...interface{}) (*compare.CapturedResult, error) {
	start := time.Now()
	result, err := conn.Execute(query, args...)
	duration := time.Since(start)

	captured := &compare.CapturedResult{
		Duration: duration,
	}

	if err != nil {
		captured.Error = err.Error()
		return captured, err
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
