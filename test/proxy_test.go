package test

import (
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/takaidohigasi/mysql-interceptor/internal/config"
	"github.com/takaidohigasi/mysql-interceptor/internal/proxy"
)

// startInProcessProxy spins up a ProxyServer listening on a free port and
// forwarding to the configured MYSQL1_ADDR. Returns the proxy's listen
// address and a cleanup func. Skips if no MySQL is reachable.
func startInProcessProxy(t *testing.T) (string, func()) {
	t.Helper()
	skipIfNoMySQL(t)

	// Pick a free port for the proxy listener.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("picking proxy port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()

	backendAddr := getEnvOrDefault("MYSQL1_ADDR", "127.0.0.1:3306")

	cfg := &config.Config{
		Proxy: config.ProxyConfig{
			ListenAddr:      addr,
			ShutdownTimeout: 5 * time.Second,
		},
		Backend: config.BackendConfig{
			Addr:     backendAddr,
			User:     "root",
			Password: "rootpass",
			DB:       "test_db",
		},
	}

	srv, err := proxy.NewProxyServer(cfg, nil, nil)
	if err != nil {
		t.Fatalf("creating proxy: %v", err)
	}

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- srv.Serve()
	}()

	// Wait for the proxy to accept TCP connections.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cleanup := func() {
		srv.Shutdown()
		select {
		case <-serveDone:
		case <-time.After(10 * time.Second):
			t.Errorf("proxy did not shut down within 10s")
		}
	}
	return addr, cleanup
}

// TestProxyPreparedStatementRoundTrip connects through the proxy using the
// standard database/sql driver, which uses the binary (prepared statement)
// protocol by default. This exercises HandleStmtPrepare/Execute/Close.
func TestProxyPreparedStatementRoundTrip(t *testing.T) {
	addr, cleanup := startInProcessProxy(t)
	defer cleanup()

	dsn := fmt.Sprintf("root:rootpass@tcp(%s)/test_db?timeout=10s", addr)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if err := db.Ping(); err != nil {
		t.Fatalf("ping via proxy: %v", err)
	}

	// database/sql automatically prepares this query on the backend.
	var name string
	err = db.QueryRow("SELECT name FROM users WHERE id = ?", 1).Scan(&name)
	if err != nil {
		t.Fatalf("prepared SELECT via proxy: %v", err)
	}
	if name != "alice" {
		t.Errorf("expected name=alice, got %q", name)
	}

	// Run the same prepared stmt with a different binding — confirms the
	// Stmt is reusable across executions.
	rows, err := db.Query("SELECT id, name FROM users WHERE id > ? ORDER BY id", 1)
	if err != nil {
		t.Fatalf("prepared ranged SELECT: %v", err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var id int
		var n string
		if err := rows.Scan(&id, &n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, n)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 rows (bob, charlie), got %d: %v", len(got), got)
	}
}

// TestProxyGracefulShutdown verifies the proxy drains cleanly under
// Shutdown() even with an idle client connection held open.
func TestProxyGracefulShutdown(t *testing.T) {
	addr, cleanup := startInProcessProxy(t)

	dsn := fmt.Sprintf("root:rootpass@tcp(%s)/test_db?timeout=10s", addr)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Establish a session so the proxy has an active connection to drain.
	if _, err := db.Exec("SELECT 1"); err != nil {
		t.Fatalf("warmup exec: %v", err)
	}

	// cleanup invokes Shutdown; the test fails if it exceeds the deadline.
	start := time.Now()
	cleanup()
	if elapsed := time.Since(start); elapsed > 8*time.Second {
		t.Errorf("shutdown took too long: %v", elapsed)
	}
}
