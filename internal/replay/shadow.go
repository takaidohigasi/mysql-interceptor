package replay

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
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
	SourceIP     string // client IP (without port) for CIDR filtering
	Database     string // current DB at the time of the query (empty if none)
	Query        string
	Args         []interface{} // non-nil for prepared statement executions
	OrigDuration time.Duration
	OrigResult   *compare.CapturedResult
}

// ShadowSender is the configuration and lifecycle root for shadow traffic.
// Actual query forwarding happens via per-primary-session ShadowSession
// objects opened via StartSession — each holds its own dedicated backend
// connection so temp tables, session variables, and transactions on the
// primary are faithfully mirrored on the shadow.
type ShadowSender struct {
	// Shared dependencies
	backendCfg      config.BackendConfig
	tlsCfg          config.BackendSideTLSConfig
	engine          *compare.Engine
	reporter        *compare.Reporter
	timeout         time.Duration
	sessionQueueSz  int
	summaryInterval time.Duration // 0 falls back to 1h; negative disables periodic logging

	// Aggregate counters (across all sessions)
	dropped    atomic.Int64
	skipped    atomic.Int64
	disabled   atomic.Int64
	filtered   atomic.Int64
	sampledOut atomic.Int64

	// Runtime-tunable policy (atomics for lock-free reads)
	enabled        atomic.Bool
	sampleRateBits atomic.Uint64
	allowedCIDRs   atomic.Pointer[[]*net.IPNet]
	excludedCIDRs  atomic.Pointer[[]*net.IPNet]

	// Active sessions registry for shutdown
	sessionsMu sync.Mutex
	sessions   map[uint64]*ShadowSession

	// Background goroutines started by NewShadowSender (currently just
	// the periodic summary loop). Close() waits on this so any final
	// log lines come before "shadow sender closed".
	bgWG sync.WaitGroup

	closed atomic.Bool
	once   sync.Once
	ctx    context.Context
	cancel context.CancelFunc
}

func NewShadowSender(cfg config.ShadowConfig, compareCfg config.ComparisonConfig) (*ShadowSender, error) {
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

	reporter, err := compare.NewReporterWithDigestCap(compareCfg.OutputFile, compareCfg.MaxUniqueDigests)
	if err != nil {
		return nil, fmt.Errorf("creating shadow reporter: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		// Per-session default: small, since each active primary session
		// now has its own queue and a dedicated goroutine draining it.
		queueSize = 64
	}

	s := &ShadowSender{
		backendCfg: config.BackendConfig{
			Addr:     cfg.TargetAddr,
			User:     cfg.TargetUser,
			Password: cfg.TargetPassword,
		},
		tlsCfg:          cfg.TLS,
		engine:          engine,
		reporter:        reporter,
		timeout:         cfg.Timeout,
		sessionQueueSz:  queueSize,
		summaryInterval: compareCfg.SummaryInterval,
		sessions:        make(map[uint64]*ShadowSession),
		ctx:             ctx,
		cancel:          cancel,
	}

	initiallyEnabled := true
	if cfg.Enabled != nil {
		initiallyEnabled = *cfg.Enabled
	}
	s.enabled.Store(initiallyEnabled)

	allowed, err := parseCIDRs(cfg.AllowedSourceCIDRs)
	if err != nil {
		return nil, fmt.Errorf("allowed_source_cidrs: %w", err)
	}
	excluded, err := parseCIDRs(cfg.ExcludedSourceCIDRs)
	if err != nil {
		return nil, fmt.Errorf("excluded_source_cidrs: %w", err)
	}
	s.allowedCIDRs.Store(&allowed)
	s.excludedCIDRs.Store(&excluded)

	initialRate := 1.0
	if cfg.SampleRate != nil {
		initialRate = *cfg.SampleRate
	}
	s.sampleRateBits.Store(math.Float64bits(initialRate))

	// applyDefaults already converts 0 → 1h, but tests construct
	// ShadowSender via NewShadowSender directly with a hand-built
	// ComparisonConfig that may leave SummaryInterval zero. Mirror the
	// same fallback here so behavior matches the documented contract
	// regardless of whether config.Load() ran.
	interval := s.summaryInterval
	if interval == 0 {
		interval = time.Hour
	}
	if interval > 0 {
		s.bgWG.Add(1)
		go s.runPeriodicSummary(interval)
	}

	return s, nil
}

// runPeriodicSummary logs the cumulative comparison summary on each
// tick. The same summary is also logged once at Close(); the periodic
// log is for long-running pods where waiting for shutdown isn't
// practical. The loop exits when s.ctx is cancelled, and Close() waits
// on s.bgWG so a tick that fired just before cancellation cannot emit
// after the "shadow sender closed" line.
func (s *ShadowSender) runPeriodicSummary(interval time.Duration) {
	defer s.bgWG.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			slog.Info("shadow comparison periodic summary",
				"interval", interval,
				"summary", s.reporter.Summary())
		}
	}
}

// StartSession opens a dedicated shadow connection for the given primary
// session. Returns (nil, nil) if shadow is currently disabled — callers
// should skip shadowing in that case. If the backend connection fails,
// returns the error so the caller can decide whether to proceed without
// shadow (recommended) or surface it.
//
// If user is non-empty, the shadow connection is opened with that user
// and password instead of the configured shadow.target_user / target_password.
// This is used in multi-user proxy mode so the shadow connection mirrors
// the same identity the primary session used, preserving per-user GRANTs
// on the shadow target.
func (s *ShadowSender) StartSession(sessionID uint64, initialDB, user, password string) (*ShadowSession, error) {
	if s.closed.Load() {
		return nil, fmt.Errorf("shadow sender is closed")
	}
	if !s.enabled.Load() {
		return nil, nil
	}

	backendCfg := s.backendCfg
	if user != "" {
		backendCfg.User = user
		backendCfg.Password = password
	}
	conn, err := backend.Connect(backendCfg, s.tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to shadow server: %w", err)
	}
	if initialDB != "" && initialDB != conn.GetDB() {
		if _, err := conn.Execute("USE `" + initialDB + "`"); err != nil {
			conn.Close()
			return nil, fmt.Errorf("USE %q on shadow: %w", initialDB, err)
		}
	}

	ctx, cancel := context.WithCancel(s.ctx)
	ss := &ShadowSession{
		sessionID:  sessionID,
		sender:     s,
		conn:       conn,
		queryCh:    make(chan ShadowQuery, s.sessionQueueSz),
		tempTables: make(map[string]struct{}),
		done:       make(chan struct{}),
		ctx:        ctx,
		cancel:     cancel,
	}

	s.sessionsMu.Lock()
	s.sessions[sessionID] = ss
	s.sessionsMu.Unlock()
	metrics.Global.ShadowActiveSessions.Add(1)

	go ss.run()
	return ss, nil
}

// Send applies only the non-session-state filter gates (enabled /
// sample_rate / CIDR) and increments counters. Session-aware checks
// (category + temp-table tracking) are the responsibility of a
// ShadowSession. Returns true if the query would pass the global gates
// — mostly useful as a test hook; real forwarding goes through
// StartSession + ShadowSession.Send.
func (s *ShadowSender) Send(sq ShadowQuery) bool {
	return s.shouldSendPreCategory(sq)
}

// shouldSendPreCategory applies configurable gates that don't require
// session state: enabled, sample_rate, CIDR. Used by ShadowSession.Send
// before the session-aware category check.
func (s *ShadowSender) shouldSendPreCategory(sq ShadowQuery) bool {
	if !s.enabled.Load() {
		s.disabled.Add(1)
		metrics.Global.ShadowDisabled.Add(1)
		return false
	}

	rate := math.Float64frombits(s.sampleRateBits.Load())
	if rate < 1.0 && (rate <= 0.0 || rand.Float64() >= rate) {
		s.sampledOut.Add(1)
		metrics.Global.ShadowSampledOut.Add(1)
		return false
	}

	if !s.isAllowedByCIDR(sq.SourceIP) {
		s.filtered.Add(1)
		metrics.Global.ShadowFilteredByCIDR.Add(1)
		return false
	}

	return true
}

// --- counter accessors ---------------------------------------------------

func (s *ShadowSender) Dropped() int64    { return s.dropped.Load() }
func (s *ShadowSender) Skipped() int64    { return s.skipped.Load() }
func (s *ShadowSender) Filtered() int64   { return s.filtered.Load() }
func (s *ShadowSender) SampledOut() int64 { return s.sampledOut.Load() }

// --- runtime controls ---------------------------------------------------

func (s *ShadowSender) IsEnabled() bool { return s.enabled.Load() }

func (s *ShadowSender) SetEnabled(enabled bool) {
	prev := s.enabled.Swap(enabled)
	if prev != enabled {
		slog.Info("shadow traffic toggled", "enabled", enabled)
		if enabled {
			metrics.Global.ShadowEnabledGauge.Store(1)
		} else {
			metrics.Global.ShadowEnabledGauge.Store(0)
		}
	}
}

func (s *ShadowSender) SampleRate() float64 {
	return math.Float64frombits(s.sampleRateBits.Load())
}

func (s *ShadowSender) SetSampleRate(rate float64) error {
	if rate < 0.0 || rate > 1.0 {
		return fmt.Errorf("sample_rate must be in [0.0, 1.0], got %v", rate)
	}
	prev := math.Float64frombits(s.sampleRateBits.Swap(math.Float64bits(rate)))
	if prev != rate {
		slog.Info("shadow sample rate updated", "rate", rate)
	}
	return nil
}

func (s *ShadowSender) SetCIDRs(allowed, excluded []string) error {
	a, err := parseCIDRs(allowed)
	if err != nil {
		return fmt.Errorf("allowed_source_cidrs: %w", err)
	}
	e, err := parseCIDRs(excluded)
	if err != nil {
		return fmt.Errorf("excluded_source_cidrs: %w", err)
	}
	s.allowedCIDRs.Store(&a)
	s.excludedCIDRs.Store(&e)
	slog.Info("shadow CIDR filters updated",
		"allowed", len(a), "excluded", len(e))
	return nil
}

// isAllowedByCIDR reports whether the source IP satisfies the current
// allow/exclude CIDR policy. Returns true (allow) when no filters apply.
// Empty/unparseable IPs pass — CIDR filtering is best-effort and shouldn't
// block shadow for Unix sockets or other non-TCP transports.
func (s *ShadowSender) isAllowedByCIDR(sourceIP string) bool {
	allowed := *s.allowedCIDRs.Load()
	excluded := *s.excludedCIDRs.Load()
	if len(allowed) == 0 && len(excluded) == 0 {
		return true
	}
	ip := net.ParseIP(sourceIP)
	if ip == nil {
		return true
	}
	for _, n := range excluded {
		if n.Contains(ip) {
			return false
		}
	}
	if len(allowed) == 0 {
		return true
	}
	for _, n := range allowed {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

func parseCIDRs(patterns []string) ([]*net.IPNet, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	out := make([]*net.IPNet, 0, len(patterns))
	for _, p := range patterns {
		_, n, err := net.ParseCIDR(p)
		if err != nil {
			return nil, fmt.Errorf("invalid CIDR %q: %w", p, err)
		}
		out = append(out, n)
	}
	return out, nil
}

// Close stops the sender, force-closes all active ShadowSessions, and
// flushes the reporter. Idempotent.
func (s *ShadowSender) Close() {
	s.once.Do(func() {
		s.closed.Store(true)
		s.cancel()

		// Snapshot sessions, then close each. Close() on a session
		// unregisters itself from s.sessions, so we can't hold the mutex
		// while iterating.
		s.sessionsMu.Lock()
		sessions := make([]*ShadowSession, 0, len(s.sessions))
		for _, ss := range s.sessions {
			sessions = append(sessions, ss)
		}
		s.sessionsMu.Unlock()

		for _, ss := range sessions {
			ss.Close()
		}

		// Wait for the periodic summary loop (if running) to observe
		// the cancellation and exit. Without this, a tick that already
		// fired could log a periodic summary after the "shadow sender
		// closed" line below.
		s.bgWG.Wait()

		s.reporter.Close()
		slog.Info("shadow sender closed",
			"skipped_unsafe", s.skipped.Load(),
			"dropped_queue_full", s.dropped.Load(),
			"summary", s.reporter.Summary())
	})
}

// unregisterSession is called by ShadowSession.Close to clear itself
// from the active registry.
func (s *ShadowSender) unregisterSession(sessionID uint64) {
	s.sessionsMu.Lock()
	if _, ok := s.sessions[sessionID]; ok {
		delete(s.sessions, sessionID)
		metrics.Global.ShadowActiveSessions.Add(-1)
	}
	s.sessionsMu.Unlock()
}
