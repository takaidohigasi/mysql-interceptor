package replay

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/takaidohigasi/mysql-interceptor/internal/backend"
	"github.com/takaidohigasi/mysql-interceptor/internal/compare"
	"github.com/takaidohigasi/mysql-interceptor/internal/config"
)

type ShadowQuery struct {
	SessionID    uint64
	Query        string
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
		config.BackendSideTLSConfig{},
		cfg.MaxConcurrent,
	)

	ignoreColumns := make(map[string]bool)
	for _, col := range compareCfg.IgnoreColumns {
		ignoreColumns[col] = true
	}

	engine := compare.NewEngine(compare.EngineConfig{
		IgnoreColumns:   ignoreColumns,
		TimeThresholdMs: compareCfg.TimeThresholdMs,
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
		return
	}

	// Short-circuit if closed to avoid a send-on-closed-channel race.
	if s.closed.Load() {
		s.dropped.Add(1)
		return
	}

	select {
	case s.queryCh <- sq:
	case <-s.ctx.Done():
		s.dropped.Add(1)
	default:
		s.dropped.Add(1)
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
		log.Printf("Shadow sender closed. Skipped non-SELECT queries: %d. Dropped (queue full): %d. %s",
			s.skipped.Load(), s.dropped.Load(), s.reporter.Summary())
	})
}

func (s *ShadowSender) worker() {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		case sq := <-s.queryCh:
			conn, err := s.pool.Get()
			if err != nil {
				log.Printf("[shadow] failed to get connection: %v", err)
				continue
			}

			replayResult, err := ExecuteAndCapture(conn, sq.Query)
			s.pool.Put(conn)

			if err != nil {
				log.Printf("[shadow] execution error: %v", err)
				continue
			}

			if sq.OrigResult != nil {
				cmpResult := s.engine.Compare(sq.OrigResult, replayResult, sq.Query, sq.SessionID)
				s.reporter.Record(cmpResult)
			}
		}
	}
}

