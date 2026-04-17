package proxy

import (
	"log/slog"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/takaidohigasi/mysql-interceptor/internal/compare"
	"github.com/takaidohigasi/mysql-interceptor/internal/replay"
)

type ProxyHandler struct {
	sessionID    uint64
	backend      *client.Conn
	currentDB    string
	logQuery     func(entry QueryEvent)
	shadowSender *replay.ShadowSender
}

type QueryEvent struct {
	Timestamp    time.Time
	SessionID    uint64
	QueryType    string
	Query        string
	Args         []interface{}
	Duration     time.Duration
	AffectedRows uint64
	RowsReturned int
	Err          error
}

func (h *ProxyHandler) UseDB(dbName string) error {
	_, err := h.backend.Execute("USE " + dbName)
	if err != nil {
		return err
	}
	h.currentDB = dbName
	return nil
}

func (h *ProxyHandler) HandleQuery(query string) (*mysql.Result, error) {
	start := time.Now()
	result, err := h.backend.Execute(query)
	duration := time.Since(start)

	if h.logQuery != nil {
		evt := QueryEvent{
			Timestamp: start,
			SessionID: h.sessionID,
			QueryType: "query",
			Query:     query,
			Duration:  duration,
			Err:       err,
		}
		if result != nil {
			evt.AffectedRows = result.AffectedRows
			if result.Resultset != nil {
				evt.RowsReturned = len(result.Values)
			}
		}
		h.logQuery(evt)
	}

	if h.shadowSender != nil {
		captured := captureResult(result, err, duration)
		h.shadowSender.Send(replay.ShadowQuery{
			SessionID:    h.sessionID,
			Database:     h.currentDB,
			Query:        query,
			OrigDuration: duration,
			OrigResult:   captured,
		})
	}

	return result, err
}

func captureResult(result *mysql.Result, err error, duration time.Duration) *compare.CapturedResult {
	captured := &compare.CapturedResult{
		Duration: duration,
	}
	if err != nil {
		captured.Error = err.Error()
		return captured
	}
	if result != nil {
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
	}
	return captured
}

func (h *ProxyHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, mysql.NewError(mysql.ER_UNKNOWN_ERROR, "not supported")
}

func (h *ProxyHandler) HandleStmtPrepare(query string) (int, int, interface{}, error) {
	// Phase 4 will implement full prepared statement support.
	// For now, return an error so clients fall back to text protocol.
	return 0, 0, nil, mysql.NewError(mysql.ER_UNKNOWN_ERROR, "prepared statements not yet supported")
}

func (h *ProxyHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	start := time.Now()
	result, err := h.backend.Execute(query, args...)
	duration := time.Since(start)

	if h.logQuery != nil {
		evt := QueryEvent{
			Timestamp: start,
			SessionID: h.sessionID,
			QueryType: "execute",
			Query:     query,
			Args:      args,
			Duration:  duration,
			Err:       err,
		}
		if result != nil {
			evt.AffectedRows = result.AffectedRows
			if result.Resultset != nil {
				evt.RowsReturned = len(result.Values)
			}
		}
		h.logQuery(evt)
	}

	return result, err
}

func (h *ProxyHandler) HandleStmtClose(context interface{}) error {
	return nil
}

func (h *ProxyHandler) HandleOtherCommand(cmd byte, data []byte) error {
	switch cmd {
	case mysql.COM_PING:
		return h.backend.Ping()
	case mysql.COM_QUIT:
		h.backend.Close()
		return nil
	default:
		slog.Warn("unsupported command",
			"session_id", h.sessionID,
			"cmd", cmd)
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, "command not supported")
	}
}
