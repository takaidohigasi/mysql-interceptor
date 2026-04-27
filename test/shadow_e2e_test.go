package test

import (
	"database/sql"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	_ "github.com/go-sql-driver/mysql"
	"github.com/takaidohigasi/mysql-interceptor/internal/config"
	"github.com/takaidohigasi/mysql-interceptor/internal/metrics"
	"github.com/takaidohigasi/mysql-interceptor/internal/proxy"
	"github.com/takaidohigasi/mysql-interceptor/internal/replay"
)

// startInProcessProxyWithShadow boots the proxy with shadow traffic
// pointed at MYSQL2_ADDR. Returns the proxy listen address, a handle to
// the shadow sender (for metric/counter snapshots), and a cleanup func.
// Skips the test if either MySQL is unreachable.
func startInProcessProxyWithShadow(t *testing.T) (proxyAddr string, shadow *replay.ShadowSender, cleanup func()) {
	t.Helper()
	skipIfNoMySQL(t)

	primaryAddr := getEnvOrDefault("MYSQL1_ADDR", "127.0.0.1:3306")
	shadowAddr := getEnvOrDefault("MYSQL2_ADDR", "127.0.0.1:3307")

	// Probe the shadow so we skip instead of failing when MYSQL2 isn't running.
	sc, err := client.Connect(shadowAddr, "root", "rootpass", "test_db")
	if err != nil {
		t.Skipf("skipping: shadow MySQL at %s unreachable: %v", shadowAddr, err)
	}
	sc.Close()

	// Pick a free port for the proxy listener.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("picking proxy port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()

	cfg := &config.Config{
		Proxy: config.ProxyConfig{
			ListenAddr:      addr,
			ShutdownTimeout: 5 * time.Second,
			Users: []config.UserConfig{
				{Username: "root", Password: "rootpass"},
			},
		},
		Backend: config.BackendConfig{
			Addr: primaryAddr,
			DB:   "test_db",
		},
		Replay: config.ReplayConfig{
			Mode: "shadow",
			Shadow: config.ShadowConfig{
				TargetAddr:     shadowAddr,
				TargetUser:     "root",
				TargetPassword: "rootpass",
				Timeout:        5 * time.Second,
				MaxConcurrent:  4,
				QueueSize:      64,
			},
		},
		Comparison: config.ComparisonConfig{
			OutputFile:      filepath.Join(t.TempDir(), "diff-report.jsonl"),
			TimeThresholdMs: 1000,
		},
	}

	sender, err := replay.NewShadowSender(cfg.Replay.Shadow, cfg.Comparison)
	if err != nil {
		t.Fatalf("creating shadow sender: %v", err)
	}

	srv, err := proxy.NewProxyServer(cfg, nil, sender)
	if err != nil {
		sender.Close()
		t.Fatalf("creating proxy: %v", err)
	}

	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve() }()

	// Wait for the proxy to accept connections.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cleanup = func() {
		srv.Shutdown()
		sender.Close()
		select {
		case <-serveDone:
		case <-time.After(10 * time.Second):
			t.Errorf("proxy did not shut down within 10s")
		}
	}
	return addr, sender, cleanup
}

// TestShadowE2E_TempTableInsertForwardedPersistentInsertNot exercises the
// session-aware shadow filter end-to-end:
//
//   - CREATE TEMPORARY TABLE + INSERT into the temp are forwarded to the
//     shadow session (temp state is tracked per-session).
//   - INSERT into a persistent table is NOT forwarded (session-safe
//     filter rejects persistent DML).
//
// We verify both outcomes two ways:
//
//  1. Metric counters: shadow_skipped should increment by exactly 1
//     (the persistent INSERT), while shadow_queries_replayed grows by
//     the count of statements that did get forwarded.
//  2. Direct inspection of the shadow MySQL: the sentinel row inserted
//     into `users` must NOT exist on shadow (the INSERT was filtered),
//     but must exist on the primary (which actually ran it).
func TestShadowE2E_TempTableInsertForwardedPersistentInsertNot(t *testing.T) {
	primaryAddr := getEnvOrDefault("MYSQL1_ADDR", "127.0.0.1:3306")
	shadowAddr := getEnvOrDefault("MYSQL2_ADDR", "127.0.0.1:3307")

	const sentinelID = 9999
	const sentinelName = "shadow_e2e_sentinel"

	// Idempotent cleanup on both primary and shadow: remove any leftover
	// sentinel row from a previous failed run (and on exit).
	cleanupSentinels := func() {
		for _, addr := range []string{primaryAddr, shadowAddr} {
			c, err := client.Connect(addr, "root", "rootpass", "test_db")
			if err != nil {
				continue
			}
			_, _ = c.Execute(fmt.Sprintf("DELETE FROM users WHERE id = %d", sentinelID))
			c.Close()
		}
	}
	cleanupSentinels()
	t.Cleanup(cleanupSentinels)

	// Snapshot counters before so deltas are unambiguous regardless of
	// which other tests ran first.
	skippedBefore := metrics.Global.ShadowSkipped.Load()
	replayedBefore := metrics.Global.ShadowQueriesReplayed.Load()

	proxyAddr, _, cleanup := startInProcessProxyWithShadow(t)
	defer cleanup()

	dsn := fmt.Sprintf("root:rootpass@tcp(%s)/test_db?timeout=10s", proxyAddr)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Force all statements through a single client connection so the
	// proxy sees a single primary session, which gets a single pinned
	// shadow session + temp-table state.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer db.Close()

	// --- Step 1: CREATE TEMPORARY TABLE on the single connection ---------
	if _, err := db.Exec("CREATE TEMPORARY TABLE scratch (id INT, note VARCHAR(32))"); err != nil {
		t.Fatalf("CREATE TEMPORARY TABLE: %v", err)
	}

	// --- Step 2: INSERT into the temp table — should be forwarded -------
	if _, err := db.Exec("INSERT INTO scratch VALUES (1, 'a'), (2, 'b'), (3, 'c')"); err != nil {
		t.Fatalf("INSERT INTO scratch: %v", err)
	}

	// --- Step 3: SELECT from the temp (primary must see 3 rows) ---------
	var tmpCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM scratch").Scan(&tmpCount); err != nil {
		t.Fatalf("SELECT COUNT(*) FROM scratch: %v", err)
	}
	if tmpCount != 3 {
		t.Errorf("expected 3 rows in temp scratch, got %d", tmpCount)
	}

	// --- Step 4: INSERT into the PERSISTENT users table -----------------
	// The primary will gain the sentinel row; shadow must NOT.
	if _, err := db.Exec(
		"INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
		sentinelID, sentinelName, "sentinel@e2e.test",
	); err != nil {
		t.Fatalf("INSERT INTO users: %v", err)
	}

	// Close the client connection so the proxy tears down the session
	// (and the shadow session), draining any in-flight shadow queries.
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// The shadow pipeline is asynchronous; give it a moment to drain.
	time.Sleep(500 * time.Millisecond)

	// --- Assertion A: shadow MySQL has NO sentinel row -------------------
	shadowConn, err := client.Connect(shadowAddr, "root", "rootpass", "test_db")
	if err != nil {
		t.Fatalf("connect shadow: %v", err)
	}
	defer shadowConn.Close()

	shadowRes, err := shadowConn.Execute(
		fmt.Sprintf("SELECT COUNT(*) FROM users WHERE id = %d", sentinelID))
	if err != nil {
		t.Fatalf("SELECT COUNT(*) on shadow: %v", err)
	}
	shadowHits, _ := shadowRes.GetInt(0, 0)
	if shadowHits != 0 {
		t.Errorf("expected 0 sentinel rows on shadow (persistent INSERT must be filtered), got %d", shadowHits)
	}

	// --- Assertion B: primary MySQL DOES have the sentinel row ----------
	primaryConn, err := client.Connect(primaryAddr, "root", "rootpass", "test_db")
	if err != nil {
		t.Fatalf("connect primary: %v", err)
	}
	defer primaryConn.Close()

	primaryRes, err := primaryConn.Execute(
		fmt.Sprintf("SELECT COUNT(*) FROM users WHERE id = %d", sentinelID))
	if err != nil {
		t.Fatalf("SELECT COUNT(*) on primary: %v", err)
	}
	primaryHits, _ := primaryRes.GetInt(0, 0)
	if primaryHits != 1 {
		t.Errorf("expected 1 sentinel row on primary (proxy forwards normally), got %d", primaryHits)
	}

	// --- Assertion C: shadow_skipped incremented by exactly 1 -----------
	// The persistent INSERT is the only statement in this test that's
	// not session-safe. Everything else (CREATE TEMPORARY, INSERT into
	// the tracked temp, SELECT from temp, COM_INIT_DB from the driver
	// handshake) falls into SELECT / SessionState / TempTable categories.
	skipDelta := metrics.Global.ShadowSkipped.Load() - skippedBefore
	if skipDelta != 1 {
		t.Errorf("expected shadow_skipped to increment by 1 (the persistent INSERT), got delta=%d",
			skipDelta)
	}

	// --- Assertion D: shadow_queries_replayed incremented by >= 3 -------
	// At minimum: CREATE TEMPORARY TABLE, INSERT INTO scratch, SELECT.
	// Driver handshakes may add more (e.g. USE, SET session vars),
	// which is fine — we just require the three explicit statements
	// all produced successful shadow executions.
	replayDelta := metrics.Global.ShadowQueriesReplayed.Load() - replayedBefore
	if replayDelta < 3 {
		t.Errorf("expected shadow_queries_replayed to increment by >=3, got delta=%d",
			replayDelta)
	}
	t.Logf("shadow counters: replayed=+%d, skipped=+%d", replayDelta, skipDelta)

}
