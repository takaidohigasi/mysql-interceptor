package replay

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/takaidohigasi/mysql-interceptor/internal/compare"
	"github.com/takaidohigasi/mysql-interceptor/internal/metrics"
)

// ShadowSession is the shadow-side counterpart of a single primary MySQL
// session. It holds one dedicated backend connection, processes queries
// serially in FIFO order from a bounded per-session queue, and tracks
// temp tables created on the primary so DML against those tables can be
// forwarded safely.
//
// The ShadowSession is not goroutine-safe for Send: it assumes queries
// arrive serially, which matches the go-mysql server's command loop (one
// command at a time per client connection).
type ShadowSession struct {
	sessionID uint64
	sender    *ShadowSender
	conn      *client.Conn
	queryCh   chan ShadowQuery

	// tempTables is the lowercase set of temp tables this session has
	// created on the shadow connection. Accessed only from the handler
	// goroutine that calls Send, so no mutex is needed.
	//
	// Updates are optimistic (at Send time, before the shadow goroutine
	// has actually executed the CREATE). A CREATE failure on the shadow
	// would leave a phantom entry — the cost is one or two subsequent
	// forwarded DMLs that error on shadow, which is not dangerous (the
	// comparison report surfaces the error divergence).
	tempTables map[string]struct{}

	closed atomic.Bool
	done   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
}

// Send applies the session-level filter — which includes the sender's
// global gates (enabled/sample/CIDR) plus a session-aware category
// check — then enqueues for execution on this session's pinned connection.
// Non-blocking: if the per-session queue is full, the query is dropped
// and counted as shadow_dropped.
func (ss *ShadowSession) Send(sq ShadowQuery) {
	if ss.closed.Load() {
		ss.sender.dropped.Add(1)
		metrics.Global.ShadowDropped.Add(1)
		return
	}

	// Global gates: enabled, sample rate, CIDR.
	if !ss.sender.shouldSendPreCategory(sq) {
		return
	}

	// Session-aware category check. Update temp-table tracking
	// optimistically so subsequent DML against the new temp will pass.
	if !ss.passesCategoryCheck(sq.Query) {
		ss.sender.skipped.Add(1)
		metrics.Global.ShadowSkipped.Add(1)
		return
	}

	select {
	case ss.queryCh <- sq:
	case <-ss.ctx.Done():
		ss.sender.dropped.Add(1)
		metrics.Global.ShadowDropped.Add(1)
	default:
		ss.sender.dropped.Add(1)
		metrics.Global.ShadowDropped.Add(1)
	}
}

// passesCategoryCheck decides whether the query is safe to run on the
// session's pinned connection. Accepts:
//   - everything IsSafeForShadowSession accepts (SELECT, session state,
//     temp-table DDL, transactions)
//   - DML/DDL whose target is a temp table this session created
//
// Also updates ss.tempTables on CREATE/DROP of a temp table so the
// next query in the same session sees the update.
func (ss *ShadowSession) passesCategoryCheck(query string) bool {
	cat := Classify(query)
	if IsSafeForShadowSession(cat) {
		// Track temp-table lifecycle as we pass these through.
		switch cat {
		case CategoryTempTable:
			if name := ExtractTempTableName(query); name != "" {
				switch {
				case startsWithKeyword(query, "CREATE"):
					ss.tempTables[name] = struct{}{}
				case startsWithKeyword(query, "DROP"):
					delete(ss.tempTables, name)
				}
			}
		}
		return true
	}

	// DML or DDL against a known temp table is safe — mutations are
	// confined to this shadow connection's own temp state.
	if cat == CategoryDML || cat == CategoryDDL {
		name := ExtractDMLTargetTable(query)
		if name == "" {
			return false
		}
		if _, ok := ss.tempTables[name]; ok {
			// Plain DROP TABLE on a tracked temp removes it — MySQL
			// resolves DROP TABLE against the temp list first. TRUNCATE
			// leaves the table intact (just empties it), so we keep the
			// tracking entry.
			if startsWithKeyword(query, "DROP") {
				delete(ss.tempTables, name)
			}
			return true
		}
	}
	return false
}

// Close signals the session goroutine to exit, waits for it to drain
// the queue and close the connection, and unregisters the session from
// its sender. Idempotent.
func (ss *ShadowSession) Close() {
	if !ss.closed.CompareAndSwap(false, true) {
		return
	}
	ss.cancel()
	<-ss.done
	ss.conn.Close()
	ss.sender.unregisterSession(ss.sessionID)
}

// run drains queryCh serially on ss.conn. Exits on ctx cancel.
func (ss *ShadowSession) run() {
	defer close(ss.done)

	engine := ss.sender.engine
	reporter := ss.sender.reporter

	for {
		select {
		case <-ss.ctx.Done():
			return
		case sq := <-ss.queryCh:
			ss.processQuery(sq, engine, reporter)
		}
	}
}

func (ss *ShadowSession) processQuery(sq ShadowQuery, engine *compare.Engine, reporter *compare.Reporter) {
	// Follow the primary's current database if it diverges. This covers
	// the case where the primary issues USE <db> as a protocol command
	// (COM_INIT_DB) rather than as a query, and also handles the first
	// query after an initial_db-less connect.
	if sq.Database != "" && sq.Database != ss.conn.GetDB() {
		if _, err := ss.conn.Execute("USE `" + sq.Database + "`"); err != nil {
			slog.Error("shadow: USE failed",
				"session_id", ss.sessionID, "db", sq.Database, "err", err)
			return
		}
	}

	// Enforce per-query timeout. go-mysql's Execute has no native ctx
	// parameter, so we race it against a timer. On timeout we abort the
	// in-flight Execute by setting a past deadline on the underlying
	// net.Conn, then drain the goroutine before closing — see the
	// abortInFlightExec helper for why Close() can't race Execute
	// directly.
	done := make(chan execResult, 1)
	go func() {
		r, e := ExecuteAndCapture(ss.conn, sq.Query, sq.Args...)
		done <- execResult{r, e}
	}()

	timeout := ss.sender.timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	select {
	case res := <-done:
		if res.err != nil {
			slog.Debug("shadow: execution error",
				"session_id", ss.sessionID, "err", res.err)
			// Still record the error in the comparison report so it
			// shows up as a divergence the operator can audit.
			if sq.OrigResult != nil {
				replayRes := &compare.CapturedResult{Error: res.err.Error()}
				cmpResult := engine.Compare(sq.OrigResult, replayRes, sq.Query, sq.User, sq.SessionID)
				reporter.Record(cmpResult)
			}
			return
		}
		metrics.Global.ShadowQueriesReplayed.Add(1)
		if sq.OrigResult != nil {
			cmpResult := engine.Compare(sq.OrigResult, res.result, sq.Query, sq.User, sq.SessionID)
			reporter.Record(cmpResult)
		}
	case <-time.After(timeout):
		slog.Warn("shadow: query timeout exceeded, tearing down session",
			"session_id", ss.sessionID, "timeout", timeout, "query", sq.Query)
		ss.abortInFlightExec(done)
		ss.conn.Close()
		metrics.Global.ShadowDropped.Add(1)
		ss.cancel()
	case <-ss.ctx.Done():
		ss.abortInFlightExec(done)
		ss.conn.Close()
	}
}

// execResult carries the (result, err) pair from the per-query
// Execute goroutine back to processQuery via a buffered channel.
// Lifted out of processQuery so abortInFlightExec can name it in its
// receiver-method signature.
type execResult struct {
	result *compare.CapturedResult
	err    error
}

// abortInFlightExec poisons the shadow connection so the in-flight
// Execute returns with an i/o-deadline error, then waits for the
// Execute goroutine to drain `done`. After this returns it is safe
// to call ss.conn.Close() without racing the Execute goroutine on
// packet.Conn's buffered writer or Sequence field.
//
// We use net.Conn.SetDeadline (goroutine-safe per stdlib) instead of
// ss.conn.Close() because go-mysql's *client.Conn isn't safe for
// concurrent Close-while-Execute: Close clears packet.Conn.Sequence
// at the same time Execute's writeCommand mutates it, which the race
// detector flags. SetDeadline only touches the underlying net.Conn,
// not Sequence, so the in-flight Execute returns cleanly.
func (ss *ShadowSession) abortInFlightExec(done <-chan execResult) {
	// A past deadline aborts both reads and writes on the underlying
	// net.Conn. Reachable via method promotion: client.Conn embeds
	// *packet.Conn, which embeds net.Conn.
	_ = ss.conn.SetDeadline(time.Now().Add(-time.Second))
	<-done
}

// startsWithKeyword is a case-insensitive prefix check after stripping
// leading comments/whitespace. Used by passesCategoryCheck to decide
// whether to track or untrack a temp table.
func startsWithKeyword(query, kw string) bool {
	q := stripLeadingCommentsAndWS(query)
	if len(q) < len(kw) {
		return false
	}
	for i := 0; i < len(kw); i++ {
		a := q[i]
		if a >= 'a' && a <= 'z' {
			a -= 'a' - 'A'
		}
		if a != kw[i] {
			return false
		}
	}
	if len(q) == len(kw) {
		return true
	}
	c := q[len(kw)]
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ';' || c == '('
}
