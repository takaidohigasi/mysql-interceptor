package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
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
	"github.com/takaidohigasi/mysql-interceptor/internal/replay"
)

type ProxyServer struct {
	cfg          *config.Config
	listener     net.Listener
	serverConf   *server.Server
	logger       *logging.Logger
	shadowSender *replay.ShadowSender

	// sessions tracks active client connections so they can be closed on
	// shutdown. The mutex guards insertion/deletion and final drain iteration.
	sessionsMu sync.Mutex
	sessions   map[uint64]net.Conn
	sessionsWg sync.WaitGroup
	sessionSeq atomic.Uint64

	shutdownOnce sync.Once
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewProxyServer(cfg *config.Config, logger *logging.Logger, shadowSender *replay.ShadowSender) (*ProxyServer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	ps := &ProxyServer{
		cfg:          cfg,
		logger:       logger,
		shadowSender: shadowSender,
		sessions:     make(map[uint64]net.Conn),
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

	return ps, nil
}

func (ps *ProxyServer) Serve() error {
	ln, err := net.Listen("tcp", ps.cfg.Proxy.ListenAddr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", ps.cfg.Proxy.ListenAddr, err)
	}
	ps.listener = ln
	log.Printf("MySQL Interceptor listening on %s, forwarding to %s", ps.cfg.Proxy.ListenAddr, ps.cfg.Backend.Addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ps.ctx.Done():
				return nil
			default:
				log.Printf("accept error: %v", err)
				continue
			}
		}

		sessionID := ps.sessionSeq.Add(1)
		ps.sessionsWg.Add(1)
		go ps.handleConnection(sessionID, conn)
	}
}

func (ps *ProxyServer) handleConnection(sessionID uint64, conn net.Conn) {
	defer ps.sessionsWg.Done()
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
	log.Printf("[session:%d] new connection from %s", sessionID, remoteAddr)

	backendConn, err := backend.Connect(ps.cfg.Backend, ps.cfg.TLS.BackendSide)
	if err != nil {
		log.Printf("[session:%d] backend connect error: %v", sessionID, err)
		return
	}
	defer backendConn.Close()

	handler := &ProxyHandler{
		sessionID:    sessionID,
		backend:      backendConn,
		shadowSender: ps.shadowSender,
	}

	if ps.logger != nil {
		handler.logQuery = func(evt QueryEvent) {
			errStr := ""
			if evt.Err != nil {
				errStr = evt.Err.Error()
			}
			ps.logger.Log(logging.LogEntry{
				Timestamp:    evt.Timestamp,
				SessionID:    evt.SessionID,
				SourceIP:     remoteAddr,
				User:         ps.cfg.Backend.User,
				Database:     handler.currentDB,
				QueryType:    evt.QueryType,
				Query:        evt.Query,
				Args:         evt.Args,
				ResponseTime: float64(evt.Duration.Microseconds()) / 1000.0,
				RowsAffected: evt.AffectedRows,
				RowsReturned: evt.RowsReturned,
				Error:        errStr,
			})
		}
	}

	serverConn, err := ps.serverConf.NewConn(conn, ps.cfg.Backend.User, ps.cfg.Backend.Password, handler)
	if err != nil {
		log.Printf("[session:%d] handshake error: %v", sessionID, err)
		return
	}

	for {
		if err := serverConn.HandleCommand(); err != nil {
			log.Printf("[session:%d] closed: %v", sessionID, err)
			return
		}
	}
}

// Shutdown gracefully stops the proxy: stops accepting new connections,
// waits up to ShutdownTimeout for active sessions to drain, then forcibly
// closes any remaining sessions. Idempotent — safe to call multiple times.
func (ps *ProxyServer) Shutdown() {
	ps.shutdownOnce.Do(ps.doShutdown)
}

func (ps *ProxyServer) doShutdown() {
	log.Println("shutting down proxy server...")
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
		log.Println("all sessions drained cleanly")
	case <-time.After(timeout):
		log.Printf("shutdown timeout reached; force-closing active sessions")
		ps.sessionsMu.Lock()
		for id, c := range ps.sessions {
			log.Printf("[session:%d] force-closing", id)
			c.Close()
		}
		ps.sessionsMu.Unlock()
		<-drained
	}
}

func buildClientSideTLSConfig(cfg config.ClientSideTLSConfig) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("loading cert/key: %w", err)
	}

	tc := &tls.Config{
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
