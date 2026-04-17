package replay

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/takaidohigasi/mysql-interceptor/internal/backend"
	"github.com/takaidohigasi/mysql-interceptor/internal/compare"
	"github.com/takaidohigasi/mysql-interceptor/internal/config"
	"github.com/takaidohigasi/mysql-interceptor/internal/metrics"
)

type ShadowQuery struct {
	SessionID    uint64
	Database     string // current DB at the time of the query (empty if none)
	Query        string
	Args         []interface{} // non-nil for prepared statement executions
	OrigDuration time.Duration
	OrigResult   *compare.CapturedResult
}

type ShadowSender struct {
	pool     *backend.Pool
	queryCh  chan ShadowQuery
	engine   *compare.Engine
	reporter *compare.Reporter
	timeout  time.Duration
	dropped  atomic.Int64
	skipped  atomic.Int64
	closed   atomic.Bool
	wg       sync.WaitGroup
	once     sync.Once
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewShadowSender(cfg config.ShadowConfig, compareCfg config.ComparisonConfig) (*ShadowSender, error) {
	pool := backend.NewPool(
		config.BackendConfig{
			Addr:     cfg.TargetAddr,
			User:     cfg.TargetUser,
			Password: cfg.TargetPassword,
		},
		cfg.TLS,
		cfg.MaxConcurrent,
	)

	ignoreColumns := make(map[string]bool)
	for _, col := range compareCfg.IgnoreColumns {
		ignoreColumns[col] = true
	}

	ignoreRegexes, err := compare.CompileIgnoreQueries(compareCfg.IgnoreQueries)
	if err != nil {
		return nil, fmt.Errorf("compiling ignore_queries: %w", err)
	}

	engine := compare.NewEngine(compare.EngineConfig{
		IgnoreColumns:    ignoreColumns,
		TimeThresholdMs:  compareCfg.TimeThresholdMs,
		IgnoreQueryRegex: ignoreRegexes,
	})

	reporter, err := compare.NewReporter(compareCfg.OutputFile)
	if err != nil {
		return nil, fmt.Errorf("creating shadow reporter: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &ShadowSender{
		pool:     pool,
		queryCh:  make(chan ShadowQuery, 10000),
		engine:   engine,
		reporter: reporter,
		timeout:  cfg.Timeout,
		ctx:      ctx,
		cancel:   cancel,
	}

	for i := 0; i < cfg.MaxConcurrent; i++ {
		s.wg.Add(1)
		go s.worker()
	}

	return s, nil
}

func (s *ShadowSender) Send(sq ShadowQuery) {
	// Always enforce read-only: never replay DML/DDL to shadow server.
	if !IsReadOnly(sq.Query) {
		s.skipped.Add(1)
		metrics.Global.ShadowSkipped.Add(1)
		return
	}

	// Short-circuit if closed to avoid a send-on-closed-channel race.
	if s.closed.Load() {
		s.dropped.Add(1)
		metrics.Global.ShadowDropped.Add(1)
		return
	}

	select {
	case s.queryCh <- sq:
	case <-s.ctx.Done():
		s.dropped.Add(1)
		metrics.Global.ShadowDropped.Add(1)
	default:
		s.dropped.Add(1)
		metrics.Global.ShadowDropped.Add(1)
	}
}

func (s *ShadowSender) Dropped() int64 {
	return s.dropped.Load()
}

func (s *ShadowSender) Skipped() int64 {
	return s.skipped.Load()
}

func (s *ShadowSender) Close() {
	s.once.Do(func() {
		// Mark closed first so Send() short-circuits. Then cancel the context
		// to wake workers. We do NOT close the queryCh — that would race with
		// in-flight Send() calls; workers exit via ctx.Done() instead.
		s.closed.Store(true)
		s.cancel()
		s.wg.Wait()
		s.reporter.Close()
		s.pool.Close()
		slog.Info("shadow sender closed",
			"skipped_non_select", s.skipped.Load(),
			"dropped_queue_full", s.dropped.Load(),
			"summary", s.reporter.Summary())
	})
}

func (s *ShadowSender) worker() {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		case sq := <-s.queryCh:
			s.processQuery(sq)
		}
	}
}

// processQuery handles one shadow query with a timeout. If the shadow
// backend is slow, we abandon the result rather than pin the worker.
// Note: go-mysql's client.Execute has no native context support, so we
// enforce the timeout by running Execute in a goroutine and dropping
// the connection on timeout so the worker can move on.
func (s *ShadowSender) processQuery(sq ShadowQuery) {
	conn, err := s.pool.Get()
	if err != nil {
		slog.Error("shadow: failed to get connection", "err", err)
		return
	}

	// Make sure the shadow connection is on the same database as the
	// primary was when the query ran. Without this, queries with
	// unqualified table references would hit the wrong schema (or
	// fail on a fresh shadow connection with no default DB).
	if sq.Database != "" && sq.Database != conn.GetDB() {
		if _, err := conn.Execute("USE `" + sq.Database + "`"); err != nil {
			slog.Error("shadow: USE failed", "db", sq.Database, "err", err)
			s.pool.Put(conn)
			return
		}
	}

	type execResult struct {
		result *compare.CapturedResult
		err    error
	}
	done := make(chan execResult, 1)
	go func() {
		r, e := ExecuteAndCapture(conn, sq.Query, sq.Args...)
		done <- execResult{r, e}
	}()

	timeout := s.timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	select {
	case res := <-done:
		s.pool.Put(conn)
		if res.err != nil {
			slog.Error("shadow: execution error", "err", res.err)
			return
		}
		metrics.Global.ShadowQueriesReplayed.Add(1)
		if sq.OrigResult != nil {
			cmpResult := s.engine.Compare(sq.OrigResult, res.result, sq.Query, sq.SessionID)
			s.reporter.Record(cmpResult)
		}
	case <-time.After(timeout):
		// Close the connection instead of returning it to the pool: the
		// background Execute is still running on it. Close causes the
		// Execute to error out, and the goroutine will GC once it returns.
		slog.Warn("shadow: query timeout exceeded, dropping connection",
			"timeout", timeout, "query", sq.Query)
		conn.Close()
		metrics.Global.ShadowDropped.Add(1)
	case <-s.ctx.Done():
		conn.Close()
		return
	}
}
