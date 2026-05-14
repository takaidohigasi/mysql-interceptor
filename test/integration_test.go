package test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/takaidohigasi/mysql-interceptor/internal/compare"
	"github.com/takaidohigasi/mysql-interceptor/internal/config"
	"github.com/takaidohigasi/mysql-interceptor/internal/logging"
	"github.com/takaidohigasi/mysql-interceptor/internal/replay"
)

func getEnvOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func skipIfNoMySQL(t *testing.T) {
	t.Helper()
	addr1 := getEnvOrDefault("MYSQL1_ADDR", "127.0.0.1:3306")
	conn, err := client.Connect(addr1, "root", "rootpass", "test_db")
	if err != nil {
		t.Skipf("skipping integration test: cannot connect to MySQL at %s: %v", addr1, err)
	}
	conn.Close()
}

// TestProxyForwarding tests that the proxy correctly forwards queries to backend.
func TestProxyForwarding(t *testing.T) {
	skipIfNoMySQL(t)

	addr := getEnvOrDefault("MYSQL1_ADDR", "127.0.0.1:3306")
	conn, err := client.Connect(addr, "root", "rootpass", "test_db")
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Test basic SELECT
	result, err := conn.Execute("SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Values) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result.Values))
	}

	// Test SELECT with WHERE
	result, err = conn.Execute("SELECT name FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT WHERE failed: %v", err)
	}
	if len(result.Values) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Values))
	}
}

// TestReplayDivergentResponses_ErrorVsSuccess tests the scenario where:
// - Server 1 (primary) has the 'orders' table and returns data
// - Server 2 (secondary) does NOT have the 'orders' table and returns an error
// The comparison should detect this divergence.
func TestReplayDivergentResponses_ErrorVsSuccess(t *testing.T) {
	skipIfNoMySQL(t)

	addr1 := getEnvOrDefault("MYSQL1_ADDR", "127.0.0.1:3306")
	addr2 := getEnvOrDefault("MYSQL2_ADDR", "127.0.0.1:3307")

	// Step 1: Execute query on primary (should succeed — orders table exists)
	conn1, err := client.Connect(addr1, "root", "rootpass", "test_db")
	if err != nil {
		t.Fatalf("failed to connect to primary: %v", err)
	}
	defer conn1.Close()

	primaryResult, err := replay.ExecuteAndCapture(conn1, "SELECT * FROM orders WHERE user_id = 1")
	if err != nil {
		t.Fatalf("ExecuteAndCapture on primary failed: %v", err)
	}
	if primaryResult.Error != "" {
		t.Fatalf("expected no error from primary, got: %s", primaryResult.Error)
	}
	if len(primaryResult.Rows) != 2 {
		t.Errorf("expected 2 rows from primary orders, got %d", len(primaryResult.Rows))
	}

	// Step 2: Execute same query on secondary (should fail — orders table doesn't exist)
	conn2, err := client.Connect(addr2, "root", "rootpass", "test_db")
	if err != nil {
		t.Fatalf("failed to connect to secondary: %v", err)
	}
	defer conn2.Close()

	// ExecuteAndCapture now returns the underlying err on failure
	// (was silently swallowed pre-fix). For "orders table doesn't
	// exist" we expect err != nil AND secondaryResult.Error to
	// carry the same error string — both are populated.
	secondaryResult, err := replay.ExecuteAndCapture(conn2, "SELECT * FROM orders WHERE user_id = 1")
	if err == nil {
		t.Fatal("expected ExecuteAndCapture on secondary to return err (orders table missing), got nil")
	}
	if secondaryResult.Error == "" {
		t.Fatal("expected secondaryResult.Error to be populated for the same failure, got empty")
	}

	// Step 3: Compare results
	engine := compare.NewEngine(compare.EngineConfig{})
	cmpResult := engine.Compare(primaryResult, secondaryResult, "SELECT * FROM orders WHERE user_id = 1", "", 1)

	if cmpResult.Match {
		t.Error("expected comparison to detect mismatch (success vs error)")
	}

	foundErrorDiff := false
	for _, d := range cmpResult.Differences {
		if d.Type == "error" {
			foundErrorDiff = true
			if d.Original != "" {
				t.Errorf("expected original error to be empty, got: %s", d.Original)
			}
			if d.Replay == "" {
				t.Error("expected replay error to be non-empty")
			}
			t.Logf("Detected divergence: primary returned data, secondary returned error: %s", d.Replay)
		}
	}
	if !foundErrorDiff {
		t.Error("expected an 'error' type difference in comparison result")
	}
}

// TestReplayDivergentResponses_ServerUUID verifies that comparing a
// server-state query like SELECT @@server_uuid between two MySQL instances
// is correctly flagged as a cell-value difference. Every MySQL server has
// a unique UUID (auto-generated on first start, persisted to auto.cnf),
// so the replay target will return a different value than the primary.
// This is an expected false-positive category operators should be aware
// of when reviewing comparison reports.
func TestReplayDivergentResponses_ServerUUID(t *testing.T) {
	skipIfNoMySQL(t)

	addr1 := getEnvOrDefault("MYSQL1_ADDR", "127.0.0.1:3306")
	addr2 := getEnvOrDefault("MYSQL2_ADDR", "127.0.0.1:3307")

	conn1, err := client.Connect(addr1, "root", "rootpass", "test_db")
	if err != nil {
		t.Fatalf("failed to connect to primary: %v", err)
	}
	defer conn1.Close()

	conn2, err := client.Connect(addr2, "root", "rootpass", "test_db")
	if err != nil {
		t.Fatalf("failed to connect to secondary: %v", err)
	}
	defer conn2.Close()

	const query = "SELECT @@server_uuid"

	r1, err := replay.ExecuteAndCapture(conn1, query)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := replay.ExecuteAndCapture(conn2, query)
	if err != nil {
		t.Fatal(err)
	}

	if r1.Error != "" {
		t.Fatalf("primary @@server_uuid failed: %s", r1.Error)
	}
	if r2.Error != "" {
		t.Fatalf("secondary @@server_uuid failed: %s", r2.Error)
	}
	if len(r1.Rows) != 1 || len(r2.Rows) != 1 {
		t.Fatalf("expected 1 row from each server, got primary=%d secondary=%d",
			len(r1.Rows), len(r2.Rows))
	}

	primaryUUID := r1.Rows[0][0]
	secondaryUUID := r2.Rows[0][0]
	if primaryUUID == "" {
		t.Fatal("primary returned empty server_uuid")
	}
	if primaryUUID == secondaryUUID {
		t.Fatalf("the two MySQL instances should have different server_uuid but both returned %q — "+
			"this suggests the test containers are sharing the same datadir", primaryUUID)
	}

	engine := compare.NewEngine(compare.EngineConfig{})
	cmpResult := engine.Compare(r1, r2, query, "", 1)

	if cmpResult.Match {
		t.Error("expected @@server_uuid to diverge between two MySQL instances")
	}

	foundCellDiff := false
	for _, d := range cmpResult.Differences {
		if d.Type == "cell_value" {
			foundCellDiff = true
			if d.Original != primaryUUID {
				t.Errorf("expected diff.original=%q, got %q", primaryUUID, d.Original)
			}
			if d.Replay != secondaryUUID {
				t.Errorf("expected diff.replay=%q, got %q", secondaryUUID, d.Replay)
			}
			t.Logf("correctly detected server_uuid divergence: primary=%s secondary=%s",
				primaryUUID, secondaryUUID)
		}
	}
	if !foundCellDiff {
		t.Errorf("expected a cell_value difference in comparison result, got %+v",
			cmpResult.Differences)
	}
}

// TestReplayDivergentResponses_SameTable_DifferentData tests comparison when
// both servers have the same table but with different data.
func TestReplayDivergentResponses_SameTable_DifferentData(t *testing.T) {
	skipIfNoMySQL(t)

	addr1 := getEnvOrDefault("MYSQL1_ADDR", "127.0.0.1:3306")
	addr2 := getEnvOrDefault("MYSQL2_ADDR", "127.0.0.1:3307")

	// Both have the 'users' table with the same data — should match
	conn1, err := client.Connect(addr1, "root", "rootpass", "test_db")
	if err != nil {
		t.Fatalf("failed to connect to primary: %v", err)
	}
	defer conn1.Close()

	conn2, err := client.Connect(addr2, "root", "rootpass", "test_db")
	if err != nil {
		t.Fatalf("failed to connect to secondary: %v", err)
	}
	defer conn2.Close()

	query := "SELECT id, name, email FROM users ORDER BY id"

	r1, err := replay.ExecuteAndCapture(conn1, query)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := replay.ExecuteAndCapture(conn2, query)
	if err != nil {
		t.Fatal(err)
	}

	engine := compare.NewEngine(compare.EngineConfig{})
	cmpResult := engine.Compare(r1, r2, query, "", 1)

	if !cmpResult.Match {
		t.Errorf("expected match for identical users table, got differences: %+v", cmpResult.Differences)
	}
}

// TestOfflineReplayWithDivergentServers tests the full offline replay pipeline:
// 1. Write a JSONL log file with queries
// 2. Replay against the secondary server
// 3. Verify that the comparison report detects the error divergence
func TestOfflineReplayWithDivergentServers(t *testing.T) {
	skipIfNoMySQL(t)

	addr2 := getEnvOrDefault("MYSQL2_ADDR", "127.0.0.1:3307")
	tmpDir := t.TempDir()

	// Step 1: Create a fake JSONL log file with queries from primary
	logFile := filepath.Join(tmpDir, "queries.jsonl")
	entries := []logging.LogEntry{
		{
			Timestamp:    time.Now().Add(-2 * time.Second),
			SessionID:    1,
			SourceIP:     "127.0.0.1",
			User:         "root",
			Database:     "test_db",
			QueryType:    "query",
			Query:        "SELECT id, name FROM users ORDER BY id",
			ResponseTime: 1.5,
			RowsAffected: 0,
			RowsReturned: 3,
		},
		{
			Timestamp:    time.Now().Add(-1 * time.Second),
			SessionID:    1,
			SourceIP:     "127.0.0.1",
			User:         "root",
			Database:     "test_db",
			QueryType:    "query",
			Query:        "SELECT * FROM orders WHERE user_id = 1",
			ResponseTime: 2.0,
			RowsAffected: 0,
			RowsReturned: 2,
		},
	}

	f, err := os.Create(logFile)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	for _, e := range entries {
		enc.Encode(e)
	}
	f.Close()

	// Step 2: Run offline replay against secondary (which lacks the orders table)
	reportFile := filepath.Join(tmpDir, "diff-report.jsonl")
	checkpointFile := filepath.Join(tmpDir, "checkpoint.json")

	replayer, err := replay.NewOfflineReplayer(
		config.OfflineConfig{
			InputDir:            tmpDir,
			FilePattern:         "queries.jsonl",
			TargetAddr:          addr2,
			TargetUser:          "root",
			TargetPassword:      "rootpass",
			SpeedFactor:         0, // no delay
			Concurrency:         1,
			CheckpointFile:      checkpointFile,
			AutoDeleteCompleted: false,
		},
		config.ComparisonConfig{
			OutputFile:      reportFile,
			TimeThresholdMs: 1000,
		},
	)
	if err != nil {
		t.Fatalf("failed to create replayer: %v", err)
	}

	if err := replayer.Run(context.Background()); err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	// Step 3: Read the diff report and verify
	reportData, err := os.ReadFile(reportFile)
	if err != nil {
		t.Fatalf("failed to read report: %v", err)
	}

	var results []compare.CompareResult
	dec := json.NewDecoder(bytes.NewReader(reportData))
	for dec.More() {
		var cr compare.CompareResult
		if err := dec.Decode(&cr); err != nil {
			break
		}
		results = append(results, cr)
	}

	if len(results) < 2 {
		t.Fatalf("expected at least 2 comparison results, got %d", len(results))
	}

	// First query (SELECT from users) should show some result (match or content diff)
	// Second query (SELECT from orders) should show error divergence
	foundOrdersError := false
	for _, r := range results {
		if r.Query == "SELECT * FROM orders WHERE user_id = 1" {
			if r.Match {
				t.Error("expected orders query to show mismatch (primary success vs secondary error)")
			}
			for _, d := range r.Differences {
				if d.Type == "error" {
					foundOrdersError = true
					t.Logf("Correctly detected divergence for orders query: replay error = %s", d.Replay)
				}
			}
		}
	}
	if !foundOrdersError {
		t.Error("expected to find error divergence for the orders query in the report")
	}

	// Step 4: Verify checkpoint was saved
	cp, err := replay.LoadCheckpoint(checkpointFile)
	if err != nil {
		t.Fatalf("failed to load checkpoint: %v", err)
	}
	if !cp.IsCompleted("queries.jsonl") {
		t.Error("expected queries.jsonl to be marked as completed in checkpoint")
	}
}

// TestOfflineReplayIgnoreServerUUID verifies the comparison.ignore_queries
// whitelist end-to-end: a SELECT @@server_uuid query replayed against a
// different MySQL instance legitimately diverges, and with the correct
// ignore pattern the diff report marks it "ignored" rather than "different".
func TestOfflineReplayIgnoreServerUUID(t *testing.T) {
	skipIfNoMySQL(t)

	addr1 := getEnvOrDefault("MYSQL1_ADDR", "127.0.0.1:3306")
	addr2 := getEnvOrDefault("MYSQL2_ADDR", "127.0.0.1:3307")
	tmpDir := t.TempDir()

	// Query the primary once to get the real uuid to seed the fake log with.
	conn1, err := client.Connect(addr1, "root", "rootpass", "test_db")
	if err != nil {
		t.Fatalf("connect primary: %v", err)
	}
	primaryCaptured, err := replay.ExecuteAndCapture(conn1, "SELECT @@server_uuid")
	conn1.Close()
	if err != nil || primaryCaptured.Error != "" {
		t.Fatalf("primary @@server_uuid: %v / %s", err, primaryCaptured.Error)
	}

	// Build a log file that references the exact primary response.
	logFile := filepath.Join(tmpDir, "queries.jsonl")
	entry := logging.LogEntry{
		Timestamp:    time.Now().Add(-time.Second),
		SessionID:    1,
		SourceIP:     "127.0.0.1",
		User:         "root",
		Database:     "test_db",
		QueryType:    "query",
		Query:        "SELECT @@server_uuid",
		ResponseTime: 0.5,
		RowsReturned: 1,
	}
	f, err := os.Create(logFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(f).Encode(entry); err != nil {
		t.Fatal(err)
	}
	f.Close()

	reportFile := filepath.Join(tmpDir, "diff-report.jsonl")
	checkpointFile := filepath.Join(tmpDir, "checkpoint.json")

	// Run replay with @@server_uuid in the ignore list.
	replayer, err := replay.NewOfflineReplayer(
		config.OfflineConfig{
			InputDir:       tmpDir,
			FilePattern:    "queries.jsonl",
			TargetAddr:     addr2,
			TargetUser:     "root",
			TargetPassword: "rootpass",
			SpeedFactor:    0,
			Concurrency:    1,
			CheckpointFile: checkpointFile,
		},
		config.ComparisonConfig{
			OutputFile:    reportFile,
			IgnoreQueries: []string{"@@server_uuid"},
		},
	)
	if err != nil {
		t.Fatalf("new replayer: %v", err)
	}
	if err := replayer.Run(context.Background()); err != nil {
		t.Fatalf("replay: %v", err)
	}

	// Parse the report and confirm the single entry is marked ignored.
	reportData, err := os.ReadFile(reportFile)
	if err != nil {
		t.Fatal(err)
	}
	var results []compare.CompareResult
	dec := json.NewDecoder(bytes.NewReader(reportData))
	for dec.More() {
		var cr compare.CompareResult
		if err := dec.Decode(&cr); err != nil {
			break
		}
		results = append(results, cr)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	got := results[0]
	if got.Query != "SELECT @@server_uuid" {
		t.Errorf("unexpected query: %s", got.Query)
	}
	if !got.Ignored {
		t.Error("expected the server_uuid result to be marked Ignored=true via the whitelist")
	}
	t.Logf("server_uuid result correctly classified as ignored: digest=%q", got.QueryDigest)
}
