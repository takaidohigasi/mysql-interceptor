package replay

import (
	"context"
	"fmt"
	"log"
	"strings"
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
	readonly bool
	timeout  time.Duration
	dropped  atomic.Int64
	wg       sync.WaitGroup
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
		readonly: cfg.ReadOnly,
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
	if s.readonly && !isReadOnly(sq.Query) {
		return
	}

	select {
	case s.queryCh <- sq:
	default:
		s.dropped.Add(1)
	}
}

func (s *ShadowSender) Dropped() int64 {
	return s.dropped.Load()
}

func (s *ShadowSender) Close() {
	s.cancel()
	close(s.queryCh)
	s.wg.Wait()
	s.reporter.Close()
	s.pool.Close()
	log.Printf("Shadow sender closed. %s", s.reporter.Summary())
}

func (s *ShadowSender) worker() {
	defer s.wg.Done()

	for sq := range s.queryCh {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

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

func isReadOnly(query string) bool {
	q := strings.TrimSpace(strings.ToUpper(query))
	return strings.HasPrefix(q, "SELECT") ||
		strings.HasPrefix(q, "SHOW") ||
		strings.HasPrefix(q, "DESCRIBE") ||
		strings.HasPrefix(q, "DESC") ||
		strings.HasPrefix(q, "EXPLAIN")
}
