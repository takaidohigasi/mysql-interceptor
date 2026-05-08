package proxy

import (
	"log/slog"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/takaidohigasi/mysql-interceptor/internal/compare"
	"github.com/takaidohigasi/mysql-interceptor/internal/metrics"
	"github.com/takaidohigasi/mysql-interceptor/internal/replay"
)

type ProxyHandler struct {
	sessionID     uint64
	sourceIP      string // client IP without port; passed to shadow for CIDR filtering
	user          string // authenticated MySQL user from the inbound handshake
	backend       *client.Conn
	currentDB     string
	logQuery      func(entry QueryEvent)
	shadowSession *replay.ShadowSession // nil if shadow is disabled or failed to start
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

// preparedStmt is the context value passed between HandleStmtPrepare,
// HandleStmtExecute, and HandleStmtClose by the go-mysql server. It holds
// the backend-side prepared statement handle plus the original query text
// (useful for logging and shadow forwarding).
type preparedStmt struct {
	backend *client.Stmt
	query   string
}

func (h *ProxyHandler) UseDB(dbName string) error {
	// Called during the inbound handshake when the client connects with
	// CONNECT_WITH_DB; at that point the backend connection has not yet
	// been opened (we need the authenticated user first). Record the
	// requested database so handleConnection can pass it to
	// backend.Connect, which brings the backend up already on this DB.
	if h.backend == nil {
		h.currentDB = dbName
		return nil
	}

	start := time.Now()
	_, err := h.backend.Execute("USE " + dbName)
	if err != nil {
		return err
	}
	h.currentDB = dbName

	// Forward the USE to shadow so its pinned connection follows the
	// primary's current database. Uses the regular afterExecute path so
	// log/shadow/metrics bookkeeping is consistent with other commands.
	h.afterExecute("use_db", "USE `"+dbName+"`", nil, start, time.Since(start), nil, nil)
	return nil
}

func (h *ProxyHandler) HandleQuery(query string) (*mysql.Result, error) {
	start := time.Now()
	result, err := h.backend.Execute(query)
	duration := time.Since(start)

	h.afterExecute("query", query, nil, start, duration, result, err)
	return result, err
}

// afterExecute handles the post-execution bookkeeping (logging + shadow
// forwarding) common to HandleQuery and HandleStmtExecute.
func (h *ProxyHandler) afterExecute(queryType, query string, args []interface{}, start time.Time, duration time.Duration, result *mysql.Result, err error) {
	metrics.Global.QueriesHandled.Add(1)
	if err != nil {
		metrics.Global.QueryErrors.Add(1)
	}

	if h.logQuery != nil {
		evt := QueryEvent{
			Timestamp: start,
			SessionID: h.sessionID,
			QueryType: queryType,
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

	if h.shadowSession != nil {
		captured := captureResult(result, err, duration)
		h.shadowSession.Send(replay.ShadowQuery{
			SessionID:    h.sessionID,
			SourceIP:     h.sourceIP,
			User:         h.user,
			Database:     h.currentDB,
			Query:        query,
			Args:         args,
			OrigDuration: duration,
			OrigResult:   captured,
		})
	}
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
			// Pre-allocate so append doesn't grow-double 2-3 times for
			// wider/longer resultsets — both sides know the exact final
			// size up front.
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
	}
	return captured
}

func (h *ProxyHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return h.backend.FieldList(table, fieldWildcard)
}

// HandleStmtPrepare forwards COM_STMT_PREPARE to the backend by calling
// client.Conn.Prepare, then returns the backend's param and column counts
// along with the *Stmt wrapped in a context value.
func (h *ProxyHandler) HandleStmtPrepare(query string) (int, int, interface{}, error) {
	stmt, err := h.backend.Prepare(query)
	if err != nil {
		return 0, 0, nil, err
	}
	return stmt.ParamNum(), stmt.ColumnNum(), &preparedStmt{backend: stmt, query: query}, nil
}

func (h *ProxyHandler) HandleStmtExecute(ctx interface{}, query string, args []interface{}) (*mysql.Result, error) {
	ps, ok := ctx.(*preparedStmt)
	if !ok || ps.backend == nil {
		return nil, mysql.NewError(mysql.ER_UNKNOWN_ERROR, "invalid prepared statement context")
	}

	start := time.Now()
	result, err := ps.backend.Execute(args...)
	duration := time.Since(start)

	h.afterExecute("execute", ps.query, args, start, duration, result, err)
	return result, err
}

func (h *ProxyHandler) HandleStmtClose(ctx interface{}) error {
	ps, ok := ctx.(*preparedStmt)
	if !ok || ps.backend == nil {
		return nil
	}
	return ps.backend.Close()
}

func (h *ProxyHandler) HandleOtherCommand(cmd byte, data []byte) error {
	switch cmd {
	case mysql.COM_PING:
		return h.backend.Ping()
	case mysql.COM_QUIT:
		h.backend.Close()
		return nil
	case mysql.COM_STATISTICS,
		mysql.COM_PROCESS_INFO,
		mysql.COM_DEBUG,
		mysql.COM_REFRESH,
		mysql.COM_TIME,
		mysql.COM_SLEEP,
		mysql.COM_CONNECT:
		// Best-effort no-op: these are informational or deprecated commands
		// that rarely carry data and can be safely ignored without breaking
		// the client connection. Return nil so the session continues.
		slog.Debug("informational command accepted",
			"session_id", h.sessionID,
			"cmd", cmd)
		return nil
	default:
		slog.Warn("unsupported command",
			"session_id", h.sessionID,
			"cmd", cmd)
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, "command not supported")
	}
}
