package replay

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/takaidohigasi/mysql-interceptor/internal/backend"
	"github.com/takaidohigasi/mysql-interceptor/internal/compare"
	"github.com/takaidohigasi/mysql-interceptor/internal/config"
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

	// execFn runs one query on conn and captures the result. Defaults to
	// ExecuteAndCapture; overridable in tests to drive processQuery's
	// timeout / teardown races deterministically without a real backend.
	execFn func(conn *client.Conn, query string, args ...interface{}) (*compare.CapturedResult, error)

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

	// pendingRetries holds diverging queries waiting to be re-executed
	// (comparison.retry), in due-time order. Owned by the run goroutine.
	// retryTimer fires when pendingRetries[0] is due; nil when empty.
	pendingRetries []pendingRetry
	retryTimer     *time.Timer

	// primaryCfg is set when comparison.retry.mode is "both": the primary
	// backend with this session's credentials. primaryConn is the
	// verification connection opened lazily from it by the run goroutine
	// on the first retry that needs it; primaryConnFailed remembers a
	// failed connect so it is attempted once per session.
	primaryCfg        *config.BackendConfig
	primaryConn       *client.Conn
	primaryConnFailed bool

	// sessionStateTouched is set (from the Send goroutine) once the
	// session ran a session-state statement such as SET; tempTableCount
	// mirrors len(tempTables) for the run goroutine. Either makes the
	// primary verification connection unable to reproduce the session,
	// so retries fall back to re-executing on the shadow only.
	sessionStateTouched atomic.Bool
	tempTableCount      atomic.Int64

	// connBroken is set once the shadow connection has been closed
	// because of a transport error or query timeout. Pending retries are
	// then settled from their last result instead of being re-executed
	// on a dead connection.
	connBroken bool

	closed atomic.Bool
	done   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
}

// pendingRetry is a diverging query parked until its re-execution is due.
type pendingRetry struct {
	sq  ShadowQuery
	due time.Time
	// lastReplay is the shadow result of the most recent execution, kept
	// so the diff can still be reported if the session ends before the
	// retry runs.
	lastReplay *compare.CapturedResult
}

// maxPendingRetriesPerSession bounds the memory held by parked results:
// beyond this many outstanding retries a new diff is reported at once.
const maxPendingRetriesPerSession = 64

// Send applies the session-level filter — which includes the sender's
// global gates (enabled/sample/CIDR) plus a session-aware category
// check — then enqueues for execution on this session's pinned connection.
// Non-blocking: if the per-session queue is full, the query is dropped
// and counted as shadow_dropped.
//
// If sq.OrigResult is nil and sq.Capture is non-nil, Capture is invoked
// AFTER the gate checks pass to materialize the primary's result lazily.
// This lets the proxy hot path skip captureResult work for queries that
// will be rejected by sample_rate / CIDR / category filters — measured
// at ~20% of sends in dev (DML against persistent tables fails the
// category check).
func (ss *ShadowSession) Send(sq ShadowQuery) {
	if ss.closed.Load() {
		ss.sender.dropped.Add(1)
		metrics.Global.ShadowDropped.Add(1)
		return
	}

	// Shadow connect failed (or the session is being torn down): the
	// session context is cancelled. Drop without doing the gate /
	// category / capture work. During the connecting window the context
	// is still live, so queries are buffered in queryCh until the shadow
	// connection becomes ready (see connectAndRun).
	if ss.ctx.Err() != nil {
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

	// All gates passed. Lazily materialize OrigResult if the producer
	// deferred capture via the Capture callback. Nil out the closure
	// after invocation so the queued ShadowQuery doesn't pin captured
	// variables (the *mysql.Result, etc.) for the entire queue
	// lifetime.
	if sq.OrigResult == nil && sq.Capture != nil {
		sq.OrigResult = sq.Capture()
	}
	sq.Capture = nil

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
				ss.tempTableCount.Store(int64(len(ss.tempTables)))
			}
		case CategorySessionState:
			ss.sessionStateTouched.Store(true)
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
				ss.tempTableCount.Store(int64(len(ss.tempTables)))
			}
			return true
		}
	}
	return false
}

// Close signals the session goroutine to exit, waits for it to drain the
// queue, and unregisters the session from its sender. Idempotent. The
// shadow connection is owned and closed by connectAndRun (its deferred
// conn.Close runs after run() returns), so Close does not touch ss.conn —
// it only needs to wait for ss.done, which connectAndRun always closes
// (even when the connect failed and run() never started).
func (ss *ShadowSession) Close() {
	if !ss.closed.CompareAndSwap(false, true) {
		return
	}
	ss.cancel()
	<-ss.done
	ss.sender.unregisterSession(ss.sessionID)
}

// run drains queryCh serially on ss.conn. On ctx cancel it makes a
// best-effort pass to drain any already-enqueued queries before
// exiting, so audit records aren't silently lost when the primary
// session ends with queries still buffered. New queries arriving
// after ctx is cancelled are dropped by the producer side
// (ShadowSession.Send checks ss.closed first). Called only from
// connectAndRun once the connection is established; connectAndRun owns
// closing ss.done.
func (ss *ShadowSession) run() {
	engine := ss.sender.engine
	reporter := ss.sender.reporter

	for {
		// Nil channel when nothing is parked: the select never picks it.
		var retryC <-chan time.Time
		if ss.retryTimer != nil {
			retryC = ss.retryTimer.C
		}
		select {
		case <-ss.ctx.Done():
			ss.drainOnShutdown(engine, reporter)
			return
		case sq := <-ss.queryCh:
			ss.processQuery(sq, engine, reporter)
		case <-retryC:
			ss.runDueRetries(engine, reporter)
		}
	}
}

// canRetry reports whether a diverging execution of sq should be parked
// for re-execution instead of being reported now. Only SELECTs qualify:
// re-running session-state, transaction or temp-table statements would
// change the shadow session, and DML never reaches the shadow anyway.
func (ss *ShadowSession) canRetry(sq ShadowQuery) bool {
	if ss.sender.retryAttempts <= 0 || sq.retryAttempt >= ss.sender.retryAttempts {
		return false
	}
	if ss.connBroken || ss.ctx.Err() != nil {
		return false
	}
	if len(ss.pendingRetries) >= maxPendingRetriesPerSession {
		slog.Debug("shadow: retry queue full, reporting diff without retry",
			"session_id", ss.sessionID, "pending", len(ss.pendingRetries))
		return false
	}
	return Classify(sq.Query) == CategorySelect
}

// scheduleRetry parks sq for re-execution after the configured delay.
// Entries are appended in arrival order; because every entry uses the
// same delay the slice stays sorted by due time, so the timer only ever
// needs to track the head.
func (ss *ShadowSession) scheduleRetry(sq ShadowQuery, lastReplay *compare.CapturedResult) {
	sq.retryAttempt++
	ss.pendingRetries = append(ss.pendingRetries, pendingRetry{
		sq:         sq,
		due:        time.Now().Add(ss.sender.retryDelay),
		lastReplay: lastReplay,
	})
	if ss.retryTimer == nil {
		ss.retryTimer = time.NewTimer(ss.sender.retryDelay)
	}
}

// runDueRetries re-executes every parked query whose delay has elapsed
// and re-arms the timer for the next one, if any.
func (ss *ShadowSession) runDueRetries(engine *compare.Engine, reporter *compare.Reporter) {
	// The timer that brought us here (if any) is spent; re-armed below
	// when something is still parked.
	if ss.retryTimer != nil {
		ss.retryTimer.Stop()
		ss.retryTimer = nil
	}
	now := time.Now()
	for len(ss.pendingRetries) > 0 && !ss.pendingRetries[0].due.After(now) {
		pr := ss.pendingRetries[0]
		ss.pendingRetries[0] = pendingRetry{} // release the parked results
		ss.pendingRetries = ss.pendingRetries[1:]
		ss.reexecute(pr.sq, engine, reporter)
	}
	if len(ss.pendingRetries) == 0 {
		ss.pendingRetries = nil
		return
	}
	ss.retryTimer = time.NewTimer(time.Until(ss.pendingRetries[0].due))
}

// settlePendingRetries is called on teardown. Parked queries are
// re-executed immediately (their delay may not have elapsed, so a
// still-lagging shadow is reported as a diff — the conservative
// outcome) while the connection is usable; once it is broken they are
// reported from the result of their last execution.
func (ss *ShadowSession) settlePendingRetries(engine *compare.Engine, reporter *compare.Reporter) {
	pending := ss.pendingRetries
	ss.pendingRetries = nil
	if ss.retryTimer != nil {
		ss.retryTimer.Stop()
		ss.retryTimer = nil
	}
	for _, pr := range pending {
		if ss.connBroken {
			ss.reportComparison(pr.sq, pr.lastReplay, engine, reporter)
			continue
		}
		ss.reexecute(pr.sq, engine, reporter)
	}
}

// reexecute runs one parked retry. In mode "both" the query is first
// re-executed on the primary verification connection and the fresh
// primary result replaces sq.OrigResult, so the following shadow
// execution compares two results taken at the same time; if the
// primary side cannot be used (no connection, session state the
// verification connection cannot reproduce, execution failure) the
// retry degrades to the "shadow" mode for this attempt.
func (ss *ShadowSession) reexecute(sq ShadowQuery, engine *compare.Engine, reporter *compare.Reporter) {
	sq.retryMode = "shadow"
	if ss.canReexecuteOnPrimary() {
		if fresh := ss.executeOnPrimary(sq); fresh != nil {
			sq.OrigResult = fresh
			sq.retryMode = "both"
		}
	}
	ss.processQuery(sq, engine, reporter)
}

// canReexecuteOnPrimary reports whether this retry may re-run the query
// on the primary: mode "both" is configured, the session has not changed
// session state or created temp tables (the verification connection has
// neither), and a previous connect attempt did not fail.
func (ss *ShadowSession) canReexecuteOnPrimary() bool {
	return ss.primaryCfg != nil && !ss.primaryConnFailed &&
		!ss.sessionStateTouched.Load() && ss.tempTableCount.Load() == 0
}

// executeOnPrimary runs sq on the primary verification connection,
// opening it on first use, and returns the captured result or nil when
// the connection could not be opened, the query failed, or it exceeded
// the per-query timeout (the connection is then dropped and reopened on
// the next retry).
func (ss *ShadowSession) executeOnPrimary(sq ShadowQuery) *compare.CapturedResult {
	if ss.primaryConn == nil {
		conn, err := ss.sender.connectPrimary(*ss.primaryCfg, ss.sender.primaryTLS)
		if err != nil {
			slog.Info("shadow: primary verification connect failed; retries fall back to shadow-only for this session",
				"session_id", ss.sessionID, "err", err)
			ss.primaryConnFailed = true
			return nil
		}
		ss.primaryConn = conn
	}
	if sq.Database != "" && sq.Database != ss.primaryConn.GetDB() {
		if _, err := ss.primaryConn.Execute("USE `" + sq.Database + "`"); err != nil {
			slog.Debug("shadow: primary verification USE failed", "session_id", ss.sessionID, "db", sq.Database, "err", err)
			return nil
		}
	}

	timeout := ss.sender.timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	done := make(chan execResult, 1)
	conn := ss.primaryConn
	go func() {
		r, e := ss.execFn(conn, sq.Query, sq.Args...)
		done <- execResult{r, e}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case res := <-done:
		metrics.Global.ShadowRetryPrimaryQueries.Add(1)
		if res.err != nil {
			slog.Debug("shadow: primary verification query failed", "session_id", ss.sessionID, "err", res.err)
			if isTransportError(res.err) {
				ss.closePrimaryConn()
			}
			return nil
		}
		return res.result
	case <-timer.C:
		slog.Warn("shadow: primary verification query timed out; dropping the verification connection",
			"session_id", ss.sessionID, "timeout", timeout)
		// Closing the connection makes the in-flight Execute return.
		ss.closePrimaryConn()
		return nil
	}
}

// closePrimaryConn releases the primary verification connection, if any.
// A later retry reopens it.
func (ss *ShadowSession) closePrimaryConn() {
	if ss.primaryConn != nil {
		backend.Quit(ss.primaryConn)
		ss.primaryConn = nil
	}
}

// drainOnShutdown processes queries already buffered in queryCh after
// ctx cancellation. We need this because Go's select picks among
// ready cases pseudo-randomly: with ctx cancelled and queryCh
// holding items, the outer loop in run() can exit via ctx.Done() and
// silently lose every remaining query — operators tailing the diff
// report would see audit records vanish at session boundaries.
//
// processQuery itself observes ctx.Done() inside its own select, but
// it does NOT abort a query merely because ctx is cancelled: it gives
// the in-flight Execute up to the per-query timeout to finish and
// records the result, so drained queries are compared instead of
// being killed and mislabeled as i/o timeouts (see processQuery's
// ctx-cancel arm). Only a genuinely hung query is aborted.
func (ss *ShadowSession) drainOnShutdown(engine *compare.Engine, reporter *compare.Reporter) {
	for {
		select {
		case sq := <-ss.queryCh:
			ss.processQuery(sq, engine, reporter)
		default:
			ss.settlePendingRetries(engine, reporter)
			return
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
		r, e := ss.execFn(ss.conn, sq.Query, sq.Args...)
		done <- execResult{r, e}
	}()

	timeout := ss.sender.timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	// time.NewTimer + Stop is preferred over time.After here: most queries
	// finish well under the 5s default, so the timer is unused 99%+ of the
	// time. time.After can't be cancelled — the underlying timer survives
	// until expiry, holding a reference in the runtime's timer heap. With
	// 200k+ queries/sec each holding a 5s timer for ~1ms of actual usage,
	// that's significant heap pressure. NewTimer + defer Stop releases the
	// timer immediately on the success / ctx.Done paths.
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case res := <-done:
		ss.recordResult(sq, res, engine, reporter)
	case <-timer.C:
		slog.Warn("shadow: query timeout exceeded, tearing down session",
			"session_id", ss.sessionID, "timeout", timeout, "query", sq.Query)
		ss.abortInFlightExec(done)
		ss.conn.Close()
		ss.connBroken = true
		metrics.Global.ShadowDropped.Add(1)
		ss.cancel()
	case <-ss.ctx.Done():
		// Session is being torn down (primary disconnected, or sender
		// shutdown). Do NOT abort an in-flight query just because ctx is
		// cancelled: during drainOnShutdown ctx is *already* cancelled
		// when processQuery starts, so a non-blocking peek at done would
		// (almost) always miss the freshly-launched Execute and abort it
		// — turning a fast, successful shadow query into a self-inflicted
		// i/o-timeout error-diff. Instead give the query up to the
		// per-query timeout (the timer started above) to complete; real
		// queries finish in ~ms, so done wins and the result is recorded
		// normally. Only a genuinely hung shadow query falls through to
		// the abort path. We do NOT close ss.conn here: ShadowSession.Close()
		// will close it once run() and drainOnShutdown have processed
		// any queries the primary already enqueued.
		select {
		case res := <-done:
			ss.recordResult(sq, res, engine, reporter)
		case <-timer.C:
			res := ss.abortInFlightExec(done)
			ss.recordResult(sq, res, engine, reporter)
		}
	}
}

// recordResult writes the comparison record for one query's
// (primary, shadow) result pair. Bumps ShadowQueriesReplayed on
// success and turns shadow-side errors into "error" diff records so
// operators see them in the audit log.
func (ss *ShadowSession) recordResult(sq ShadowQuery, res execResult, engine *compare.Engine, reporter *compare.Reporter) {
	if res.err != nil {
		slog.Debug("shadow: execution error",
			"session_id", ss.sessionID, "err", res.err)
		// Use res.result (which ExecuteAndCapture populates with both
		// .Error and .Duration even on Execute failure) so the diff
		// record carries the shadow-side latency. Before #28's fix
		// landed end-to-end this branch was unreachable (see
		// kouzoh/microservices#29641 post-mortem); a guard against
		// res.result == nil is kept for defensiveness in case a
		// future ExecuteAndCapture variant returns a nil captured.
		transport := isTransportError(res.err)
		if sq.OrigResult != nil {
			replayRes := res.result
			if replayRes == nil {
				replayRes = &compare.CapturedResult{Error: res.err.Error()}
			}
			// A server-side SQL error (e.g. a table that the changefeed
			// has not created yet) may be lag too, so it goes through
			// the retry path; a transport error means the connection
			// is about to be torn down, so report it as-is.
			if transport {
				ss.reportComparison(sq, replayRes, engine, reporter)
			} else {
				ss.compareOrRetry(sq, replayRes, engine, reporter)
			}
		}
		// Transport-level errors poison go-mysql's *client.Conn: once
		// the underlying net.Conn returns "i/o timeout" or "connection
		// was bad", every subsequent Execute on the same connection
		// short-circuits to the same error. Tear the session down so
		// the next primary query opens a fresh shadow connection.
		// Server-returned SQL errors arrive as *mysql.MyError and
		// don't break the connection, so we keep the session alive
		// for those (see isTransportError).
		if transport {
			slog.Info("shadow: transport error, tearing down session",
				"session_id", ss.sessionID, "err", res.err)
			ss.conn.Close()
			ss.connBroken = true
			ss.cancel()
		}
		return
	}
	metrics.Global.ShadowQueriesReplayed.Add(1)
	if sq.OrigResult != nil {
		ss.compareOrRetry(sq, res.result, engine, reporter)
	}
}

// compareOrRetry compares the shadow result with the primary's original
// result. A non-ignored divergence on a retryable query is parked for
// re-execution (comparison.retry) instead of being reported now; every
// other outcome is recorded immediately.
func (ss *ShadowSession) compareOrRetry(sq ShadowQuery, replay *compare.CapturedResult, engine *compare.Engine, reporter *compare.Reporter) {
	cmpResult := engine.Compare(sq.OrigResult, replay, sq.Query, sq.User, sq.SessionID)
	if !cmpResult.Match && !cmpResult.Ignored && ss.canRetry(sq) {
		compare.ReleaseCompareResult(cmpResult)
		ss.scheduleRetry(sq, replay)
		return
	}
	ss.recordComparison(sq, cmpResult, reporter)
}

// reportComparison records the comparison of sq against replay without
// considering a retry. Used for transport errors and for parked queries
// settled at teardown.
func (ss *ShadowSession) reportComparison(sq ShadowQuery, replay *compare.CapturedResult, engine *compare.Engine, reporter *compare.Reporter) {
	ss.recordComparison(sq, engine.Compare(sq.OrigResult, replay, sq.Query, sq.User, sq.SessionID), reporter)
}

// recordComparison stamps the retry bookkeeping on cmpResult, records it
// and releases it to the pool.
func (ss *ShadowSession) recordComparison(sq ShadowQuery, cmpResult *compare.CompareResult, reporter *compare.Reporter) {
	if sq.retryAttempt > 0 {
		cmpResult.Retries = sq.retryAttempt
		cmpResult.ResolvedByRetry = cmpResult.Match && !cmpResult.Ignored
		cmpResult.RetryMode = sq.retryMode
	}
	reporter.Record(cmpResult)
	compare.ReleaseCompareResult(cmpResult)
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
// Execute goroutine to drain `done`. Returns the (poisoned) result
// so the caller can record it for audit instead of silently dropping
// the query. After this returns it is safe to call ss.conn.Close()
// without racing the Execute goroutine on packet.Conn's buffered
// writer or Sequence field.
//
// We use net.Conn.SetDeadline (goroutine-safe per stdlib) instead of
// ss.conn.Close() because go-mysql's *client.Conn isn't safe for
// concurrent Close-while-Execute: Close clears packet.Conn.Sequence
// at the same time Execute's writeCommand mutates it, which the race
// detector flags. SetDeadline only touches the underlying net.Conn,
// not Sequence, so the in-flight Execute returns cleanly.
func (ss *ShadowSession) abortInFlightExec(done <-chan execResult) execResult {
	// A past deadline aborts both reads and writes on the underlying
	// net.Conn. Reachable via method promotion: client.Conn embeds
	// *packet.Conn, which embeds net.Conn.
	_ = ss.conn.SetDeadline(time.Now().Add(-time.Second))
	return <-done
}

// isTransportError reports whether err looks like a broken-connection
// (TCP-level / packet.Conn) failure as opposed to a server-returned
// SQL error.
//
// go-mysql wraps the MySQL server's ERR_Packet in *mysql.MyError —
// "Table doesn't exist", "syntax error", "duplicate key", etc. Those
// are query-specific and don't poison the underlying connection;
// recordResult should keep the session alive for the next query.
//
// Anything that ISN'T *mysql.MyError is a transport-level failure:
// i/o timeout, broken pipe, RST from the server's tcp_keepalive
// reaper, packet.Conn's "connection was bad" cached state, etc. Once
// the underlying net.Conn is dead, go-mysql will return the same
// error for every subsequent Execute on this *client.Conn — the
// session must be torn down so the next primary query opens a fresh
// shadow session with a fresh connection.
func isTransportError(err error) bool {
	if err == nil {
		return false
	}
	var myErr *gomysql.MyError
	return !errors.As(err, &myErr)
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
