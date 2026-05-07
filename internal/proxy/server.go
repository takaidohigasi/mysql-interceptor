package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/takaidohigasi/mysql-interceptor/internal/backend"
	"github.com/takaidohigasi/mysql-interceptor/internal/config"
	"github.com/takaidohigasi/mysql-interceptor/internal/logging"
	"github.com/takaidohigasi/mysql-interceptor/internal/metrics"
	"github.com/takaidohigasi/mysql-interceptor/internal/replay"
)

type ProxyServer struct {
	cfg          *config.Config
	listener     net.Listener
	serverConf   *server.Server
	logger       *logging.Logger
	shadowSender *replay.ShadowSender

	// authHandler validates the inbound MySQL handshake against
	// cfg.Proxy.Users. The same map keys are mirrored in userPasswords
	// so we can recover the plaintext password after the handshake and
	// use it to authenticate the session's outbound backend connection.
	authHandler   *server.InMemoryAuthenticationHandler
	userPasswords map[string]string

	// maxSessionLifetime holds the configured cap. The underlying
	// time.Duration is stored as int64 only because atomic.Int64 is the
	// type the stdlib provides; the YAML accepts human-readable durations
	// (e.g. "30m", "1h"). 0 means disabled. SetMaxSessionLifetime updates
	// it without restarting the proxy; sessions read it on every loop
	// iteration and apply per-session ±10% jitter.
	maxSessionLifetime atomic.Int64

	// sessions tracks active client connections so they can be closed on
	// shutdown. The mutex guards insertion/deletion and final drain iteration.
	sessionsMu sync.Mutex
	sessions   map[uint64]net.Conn
	sessionsWg sync.WaitGroup
	sessionSeq atomic.Uint64

	// connSlots bounds concurrent client sessions to Proxy.MaxConnections.
	// Empty slot receive = permission to accept; send back on release.
	connSlots chan struct{}

	shutdownOnce sync.Once
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewProxyServer(cfg *config.Config, logger *logging.Logger, shadowSender *replay.ShadowSender) (*ProxyServer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	maxConns := cfg.Proxy.MaxConnections
	if maxConns <= 0 {
		maxConns = 1000
	}

	ps := &ProxyServer{
		cfg:          cfg,
		logger:       logger,
		shadowSender: shadowSender,
		sessions:     make(map[uint64]net.Conn),
		connSlots:    make(chan struct{}, maxConns),
		ctx:          ctx,
		cancel:       cancel,
	}

	var svr *server.Server
	if cfg.TLS.ClientSide.Enabled {
		tc, err := buildClientSideTLSConfig(cfg.TLS.ClientSide)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("building client-side TLS config: %w", err)
		}
		svr = server.NewServer("8.0.30", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, tc)
	} else {
		svr = server.NewDefaultServer()
	}

	ps.serverConf = svr
	ps.maxSessionLifetime.Store(int64(cfg.Proxy.MaxSessionLifetime))

	if len(cfg.Proxy.Users) == 0 {
		// config.Validate already enforces this, so the only way to hit
		// this branch is constructing a Config struct directly in a test.
		// Return an error rather than crashing later in handleConnection.
		cancel()
		return nil, fmt.Errorf("proxy.users must contain at least one entry")
	}
	ah := server.NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	passwords := make(map[string]string, len(cfg.Proxy.Users))
	for _, u := range cfg.Proxy.Users {
		if err := ah.AddUser(u.Username, u.Password); err != nil {
			cancel()
			return nil, fmt.Errorf("adding user %q: %w", u.Username, err)
		}
		passwords[u.Username] = u.Password
	}
	ps.authHandler = ah
	ps.userPasswords = passwords

	return ps, nil
}

// SetMaxSessionLifetime atomically updates the cap. 0 (or negative)
// disables the cap entirely. The change applies to the per-iteration
// check in handleConnection, so existing sessions pick it up after their
// next command completes — no restart needed.
func (ps *ProxyServer) SetMaxSessionLifetime(d time.Duration) {
	if d < 0 {
		d = 0
	}
	prev := ps.maxSessionLifetime.Swap(int64(d))
	if prev != int64(d) {
		slog.Info("max_session_lifetime updated", "lifetime", d)
	}
}

// MaxSessionLifetime returns the currently configured cap (mostly for tests).
func (ps *ProxyServer) MaxSessionLifetime() time.Duration {
	return time.Duration(ps.maxSessionLifetime.Load())
}

func (ps *ProxyServer) Serve() error {
	ln, err := net.Listen("tcp", ps.cfg.Proxy.ListenAddr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", ps.cfg.Proxy.ListenAddr, err)
	}
	ps.listener = ln
	slog.Info("proxy listening",
		"listen_addr", ps.cfg.Proxy.ListenAddr,
		"backend_addr", ps.cfg.Backend.Addr,
		"max_connections", cap(ps.connSlots))

	backoff := time.Duration(0)
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ps.ctx.Done():
				return nil
			default:
			}
			// Temporary errors (e.g., EMFILE under fd pressure) shouldn't
			// turn into a busy loop. Exponential backoff up to 1s.
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				slog.Debug("accept timed out", "err", err)
				continue
			}
			if backoff == 0 {
				backoff = 5 * time.Millisecond
			} else if backoff < time.Second {
				backoff *= 2
			}
			slog.Error("accept error, backing off",
				"err", err, "backoff", backoff)
			select {
			case <-time.After(backoff):
			case <-ps.ctx.Done():
				return nil
			}
			continue
		}
		backoff = 0

		// Enforce MaxConnections: block here until a slot frees up, or
		// reject the connection if shutdown is in progress.
		select {
		case ps.connSlots <- struct{}{}:
		case <-ps.ctx.Done():
			conn.Close()
			return nil
		}

		sessionID := ps.sessionSeq.Add(1)
		ps.sessionsWg.Add(1)
		metrics.Global.TotalSessions.Add(1)
		metrics.Global.ActiveSessions.Add(1)
		go ps.handleConnection(sessionID, conn)
	}
}

func (ps *ProxyServer) handleConnection(sessionID uint64, conn net.Conn) {
	defer ps.sessionsWg.Done()
	defer metrics.Global.ActiveSessions.Add(-1)
	defer func() { <-ps.connSlots }()
	defer conn.Close()

	// Register the connection for shutdown to close it.
	ps.sessionsMu.Lock()
	ps.sessions[sessionID] = conn
	ps.sessionsMu.Unlock()
	defer func() {
		ps.sessionsMu.Lock()
		delete(ps.sessions, sessionID)
		ps.sessionsMu.Unlock()
	}()

	remoteAddr := conn.RemoteAddr().String()
	// Strip the port so CIDR filtering can match on host IP alone.
	// Unix sockets and malformed addrs fall through to "" which the
	// shadow sender treats as "no filter applies".
	sourceIP := remoteAddr
	if h, _, err := net.SplitHostPort(remoteAddr); err == nil {
		sourceIP = h
	}
	sessionLog := slog.With("session_id", sessionID, "remote", remoteAddr)
	sessionLog.Info("new connection")

	handler := &ProxyHandler{
		sessionID: sessionID,
		sourceIP:  sourceIP,
	}

	// Run the inbound handshake first; we need the authenticated user
	// before we can open the per-session outbound backend connection.
	serverConn, err := ps.serverConf.NewCustomizedConn(conn, ps.authHandler, handler)
	if err != nil {
		sessionLog.Error("handshake error", "err", err)
		return
	}
	backendUser := serverConn.GetUser()
	backendPass, ok := ps.userPasswords[backendUser]
	if !ok {
		// Should not happen: authHandler approved a user we don't have a
		// plaintext password for. Fail loudly so the missing entry gets
		// noticed instead of silently falling back.
		sessionLog.Error("authenticated user has no backend password mapping", "user", backendUser)
		return
	}

	handler.user = backendUser

	backendCfg := ps.cfg.Backend
	backendCfg.User = backendUser
	backendCfg.Password = backendPass
	// If the client sent CONNECT_WITH_DB during the handshake,
	// ProxyHandler.UseDB has already recorded it in handler.currentDB.
	// Override the configured default so the backend connection comes up
	// on the same DB the client expects.
	if handler.currentDB != "" {
		backendCfg.DB = handler.currentDB
	}
	backendConn, err := backend.Connect(backendCfg, ps.cfg.TLS.BackendSide)
	if err != nil {
		sessionLog.Error("backend connect error", "err", err, "user", backendUser)
		return
	}
	defer backendConn.Close()

	// Start a dedicated shadow session if shadow is configured. A failure
	// here must never fail the primary — we just proceed without shadow
	// for this session. The shadow connection uses the same credentials
	// so per-user GRANTs apply consistently.
	var shadowSession *replay.ShadowSession
	if ps.shadowSender != nil {
		ss, sErr := ps.shadowSender.StartSession(sessionID, backendConn.GetDB(), backendUser, backendPass)
		if sErr != nil {
			sessionLog.Warn("shadow session start failed; continuing without shadow for this session",
				"err", sErr)
		} else if ss != nil {
			shadowSession = ss
			defer shadowSession.Close()
		}
	}

	handler.backend = backendConn
	handler.currentDB = backendConn.GetDB()
	handler.shadowSession = shadowSession

	if ps.logger != nil {
		redactArgs := ps.cfg.Logging.RedactArgs
		handler.logQuery = func(evt QueryEvent) {
			errStr := ""
			if evt.Err != nil {
				errStr = evt.Err.Error()
			}
			args := evt.Args
			if redactArgs && len(args) > 0 {
				args = redact(args)
			}
			ps.logger.Log(logging.LogEntry{
				Timestamp:    evt.Timestamp,
				SessionID:    evt.SessionID,
				SourceIP:     remoteAddr,
				User:         backendUser,
				Database:     handler.currentDB,
				QueryType:    evt.QueryType,
				Query:        evt.Query,
				Args:         args,
				ResponseTime: float64(evt.Duration.Microseconds()) / 1000.0,
				RowsAffected: evt.AffectedRows,
				RowsReturned: evt.RowsReturned,
				Error:        errStr,
			})
		}
	}

	// Per-session lifetime jitter, computed once at session start. The
	// cap itself is read atomically every iteration so SetMaxSessionLifetime
	// takes effect on the next loop turn for existing sessions; the jitter
	// factor stays fixed so a session can't oscillate around the deadline.
	sessionStart := time.Now()
	jitterFactor := 1.0 + (rand.Float64()*0.2 - 0.1) // [-10%, +10%]

	for {
		if err := serverConn.HandleCommand(); err != nil {
			sessionLog.Debug("session closed", "err", err)
			return
		}
		if ps.shouldCloseForLifetime(backendConn, sessionStart, jitterFactor, sessionLog) {
			return
		}
	}
}

// shouldCloseForLifetime returns true when the session has exceeded its
// per-session deadline AND the backend is not in a transaction. When the
// deadline has passed but a transaction is open, the check is postponed
// (counted in metrics) and the session continues.
func (ps *ProxyServer) shouldCloseForLifetime(
	backendConn interface{ IsInTransaction() bool },
	start time.Time,
	jitter float64,
	sessionLog *slog.Logger,
) bool {
	cap := time.Duration(ps.maxSessionLifetime.Load())
	if cap <= 0 {
		return false
	}
	deadline := time.Duration(float64(cap) * jitter)
	if time.Since(start) < deadline {
		return false
	}
	if backendConn.IsInTransaction() {
		metrics.Global.SessionsLifetimePostponed.Add(1)
		sessionLog.Debug("session past max_lifetime but in transaction; postponing close",
			"age", time.Since(start), "deadline", deadline)
		return false
	}
	metrics.Global.SessionsClosedMaxLifetime.Add(1)
	sessionLog.Info("closing session for max_session_lifetime",
		"age", time.Since(start), "deadline", deadline)
	return true
}

// Shutdown gracefully stops the proxy: stops accepting new connections,
// waits up to ShutdownTimeout for active sessions to drain, then forcibly
// closes any remaining sessions. Idempotent — safe to call multiple times.
func (ps *ProxyServer) Shutdown() {
	ps.shutdownOnce.Do(ps.doShutdown)
}

func (ps *ProxyServer) doShutdown() {
	slog.Info("shutting down proxy server")
	ps.cancel()
	if ps.listener != nil {
		ps.listener.Close()
	}

	timeout := ps.cfg.Proxy.ShutdownTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	drained := make(chan struct{})
	go func() {
		ps.sessionsWg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		slog.Info("all sessions drained cleanly")
	case <-time.After(timeout):
		slog.Warn("shutdown timeout reached; force-closing active sessions",
			"timeout", timeout)
		ps.sessionsMu.Lock()
		for id, c := range ps.sessions {
			slog.Info("force-closing session", "session_id", id)
			c.Close()
		}
		ps.sessionsMu.Unlock()
		<-drained
	}
}

// redact returns a new slice with every element replaced by "<redacted>".
// Used when logging.redact_args is true so bind values never persist.
func redact(args []interface{}) []interface{} {
	out := make([]interface{}, len(args))
	for i := range args {
		out[i] = "<redacted>"
	}
	return out
}

func buildClientSideTLSConfig(cfg config.ClientSideTLSConfig) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("loading cert/key: %w", err)
	}

	tc := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}

	if cfg.CAFile != "" {
		caCert, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append CA certificate")
		}
		tc.ClientCAs = pool
		tc.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tc, nil
}
