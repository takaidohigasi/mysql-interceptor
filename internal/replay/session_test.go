package replay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// newTestShadowSession builds a ShadowSession without a real backend
// connection. Useful for testing the session-local filter / temp-table
// logic in isolation from network I/O.
func newTestShadowSession(t *testing.T) *ShadowSession {
	t.Helper()
	sender := &ShadowSender{}
	sender.enabled.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	return &ShadowSession{
		sessionID:  1,
		sender:     sender,
		queryCh:    make(chan ShadowQuery, 64),
		tempTables: make(map[string]struct{}),
		done:       make(chan struct{}),
		ctx:        ctx,
		cancel:     cancel,
	}
}

func TestShadowSession_AllowsSelect(t *testing.T) {
	ss := newTestShadowSession(t)
	if !ss.passesCategoryCheck("SELECT * FROM users") {
		t.Error("expected SELECT to pass")
	}
}

func TestShadowSession_RejectsPersistentDML(t *testing.T) {
	ss := newTestShadowSession(t)
	for _, q := range []string{
		"INSERT INTO users VALUES (1)",
		"UPDATE users SET name = 'x' WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
		"REPLACE INTO users VALUES (1)",
		"CREATE TABLE users (x INT)",
		"DROP TABLE users",
		"ALTER TABLE users ADD COLUMN y INT",
		"TRUNCATE users",
	} {
		if ss.passesCategoryCheck(q) {
			t.Errorf("expected %q to be rejected on fresh session", q)
		}
	}
}

func TestShadowSession_TempTableLifecycle(t *testing.T) {
	ss := newTestShadowSession(t)

	// Step 1: CREATE TEMPORARY TABLE — passes, tracks the name.
	if !ss.passesCategoryCheck("CREATE TEMPORARY TABLE scratch (id INT)") {
		t.Fatal("CREATE TEMPORARY TABLE should pass")
	}
	if _, ok := ss.tempTables["scratch"]; !ok {
		t.Fatal("expected 'scratch' to be tracked after CREATE TEMPORARY")
	}

	// Step 2: DML against the temp table — now passes.
	for _, q := range []string{
		"INSERT INTO scratch VALUES (1)",
		"INSERT IGNORE INTO scratch VALUES (2)",
		"UPDATE scratch SET id = 10 WHERE id = 1",
		"DELETE FROM scratch WHERE id = 10",
		"REPLACE INTO scratch VALUES (3)",
		"ALTER TABLE scratch ADD COLUMN v TEXT",
		"TRUNCATE scratch",
	} {
		if !ss.passesCategoryCheck(q) {
			t.Errorf("expected %q to pass (target is tracked temp table)", q)
		}
	}

	// Step 3: DML against an unknown (persistent) table — still rejects.
	if ss.passesCategoryCheck("INSERT INTO real_users VALUES (1)") {
		t.Error("INSERT into persistent table should still be rejected")
	}

	// Step 4: case-insensitive match on table name.
	if !ss.passesCategoryCheck("INSERT INTO SCRATCH VALUES (99)") {
		t.Error("temp-table tracking should be case-insensitive")
	}

	// Step 5: schema-qualified reference matches by table name.
	if !ss.passesCategoryCheck("INSERT INTO mydb.scratch VALUES (99)") {
		t.Error("schema-qualified DML should match the temp table")
	}

	// Step 6: DROP TEMPORARY TABLE — passes, removes tracking.
	if !ss.passesCategoryCheck("DROP TEMPORARY TABLE scratch") {
		t.Fatal("DROP TEMPORARY TABLE should pass")
	}
	if _, ok := ss.tempTables["scratch"]; ok {
		t.Error("expected 'scratch' to be removed after DROP TEMPORARY")
	}

	// Step 7: subsequent INSERT is rejected again — no longer a temp.
	if ss.passesCategoryCheck("INSERT INTO scratch VALUES (1)") {
		t.Error("INSERT after DROP TEMPORARY should be rejected")
	}
}

func TestShadowSession_PlainDropAlsoUntracks(t *testing.T) {
	// MySQL allows plain DROP TABLE on a temp — it checks the temp list
	// first. We mirror that: a plain DROP TABLE on a tracked temp is
	// allowed AND untracked.
	ss := newTestShadowSession(t)
	ss.passesCategoryCheck("CREATE TEMPORARY TABLE scratch (id INT)")
	if _, ok := ss.tempTables["scratch"]; !ok {
		t.Fatal("precondition: scratch should be tracked")
	}

	// Plain DROP TABLE of a tracked temp — allowed, untracked.
	if !ss.passesCategoryCheck("DROP TABLE scratch") {
		t.Error("plain DROP TABLE of a tracked temp should pass")
	}
	if _, ok := ss.tempTables["scratch"]; ok {
		t.Error("plain DROP TABLE of a tracked temp should remove it from tracking")
	}
}

func TestShadowSession_RejectsJoinsEvenWithTempTable(t *testing.T) {
	// Conservative: if the DML mentions JOIN, reject even if one of the
	// tables is a temp. Avoids the risk of a typo mistakenly writing to
	// a persistent table joined in.
	ss := newTestShadowSession(t)
	ss.passesCategoryCheck("CREATE TEMPORARY TABLE t (id INT)")

	if ss.passesCategoryCheck("UPDATE t JOIN persistent_u ON t.id = persistent_u.id SET t.v = 1") {
		t.Error("multi-table UPDATE should be rejected even with one temp")
	}
}

func TestShadowSession_SessionStatePassesAlways(t *testing.T) {
	ss := newTestShadowSession(t)
	for _, q := range []string{
		"SET @v = 1",
		"USE mydb",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"PREPARE s FROM 'SELECT 1'",
		"EXECUTE s",
		"DEALLOCATE PREPARE s",
	} {
		if !ss.passesCategoryCheck(q) {
			t.Errorf("expected session-state statement to pass: %q", q)
		}
	}
}

// TestIsTransportError pins the decision rule that recordResult uses
// to choose between "keep session alive" (server-returned SQL error)
// and "tear down session" (broken connection). The cascade observed
// in kouzoh/microservices#29641 — 5k+ identical i/o-timeout error
// records on one digest in 20 min — happened because the previous
// recordResult treated *all* errors as recoverable and kept dispatching
// queries against a poisoned *client.Conn. This test fixes the
// boundary so a future change can't silently re-introduce that.
func TestIsTransportError(t *testing.T) {
	// Server-returned SQL errors must NOT trigger teardown. These are
	// query-specific and leave the underlying connection healthy for
	// subsequent queries.
	sqlErrors := []error{
		// Bare *mysql.MyError as produced by go-mysql when the server
		// returns ERR_Packet.
		&gomysql.MyError{Code: gomysql.ER_NO_SUCH_TABLE, Message: "Table 'foo.bar' doesn't exist"},
		&gomysql.MyError{Code: gomysql.ER_PARSE_ERROR, Message: "You have an error in your SQL syntax"},
		&gomysql.MyError{Code: gomysql.ER_DUP_ENTRY, Message: "Duplicate entry '1' for key 'PRIMARY'"},
		// Wrapped — errors.As must still surface the MyError.
		fmt.Errorf("execute failed: %w", &gomysql.MyError{Code: gomysql.ER_NO_SUCH_TABLE, Message: "missing"}),
	}
	for _, err := range sqlErrors {
		if isTransportError(err) {
			t.Errorf("server SQL error misclassified as transport-level (would tear down session unnecessarily): %v", err)
		}
	}

	// Transport-level errors MUST trigger teardown: i/o timeout from
	// our SetDeadline poisoning or from kernel TCP keepalive death,
	// io.EOF on read after server closed the conn, generic net.OpError,
	// and the bare wrapped errors go-mysql emits ("Write failed.
	// err ...: i/o timeout: connection was bad").
	transportErrors := []error{
		io.EOF,
		io.ErrUnexpectedEOF,
		&net.OpError{Op: "write", Net: "tcp", Err: errTimeout{}},
		errors.New("Write failed. err write tcp 10.34.27.232:54321->10.38.25.197:3306: i/o timeout: connection was bad"),
		errors.New("io.ReadFull(header) failed. err read tcp ...: i/o timeout: connection was bad"),
		errors.New("connection was bad"),
	}
	for _, err := range transportErrors {
		if !isTransportError(err) {
			t.Errorf("transport-level error misclassified as recoverable (cascade bug re-introduced): %v", err)
		}
	}

	// Nil is never a transport error — guards callers that branch on
	// res.err == nil first.
	if isTransportError(nil) {
		t.Error("nil error must not be classified as transport-level")
	}
}

// errTimeout is a minimal net.Error implementation marking the error
// as a timeout, used to construct realistic *net.OpError fixtures
// without spinning up an actual socket.
type errTimeout struct{}

func (errTimeout) Error() string   { return "i/o timeout" }
func (errTimeout) Timeout() bool   { return true }
func (errTimeout) Temporary() bool { return true }
