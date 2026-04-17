package replay

import "testing"

func TestIsReadOnly(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		// Read-only: allowed
		{"SELECT * FROM users", true},
		{"select id from t", true},
		{"  SELECT 1  ", true},
		{"SHOW TABLES", true},
		{"SHOW CREATE TABLE t", true},
		{"DESCRIBE users", true},
		{"DESC users", true},
		{"EXPLAIN SELECT * FROM t", true},
		{"WITH x AS (SELECT 1) SELECT * FROM x", true},

		// DML: must be filtered
		{"INSERT INTO users VALUES (1)", false},
		{"UPDATE users SET name='x' WHERE id=1", false},
		{"DELETE FROM users WHERE id=1", false},
		{"REPLACE INTO users VALUES (1)", false},
		{"TRUNCATE users", false},

		// DDL: must be filtered
		{"CREATE TABLE t (id INT)", false},
		{"DROP TABLE t", false},
		{"ALTER TABLE t ADD COLUMN x INT", false},

		// Transactions: filter (side-effect producing on connection state)
		{"BEGIN", false},
		{"COMMIT", false},
		{"ROLLBACK", false},

		// Comments should not hide DML
		{"/* comment */ DELETE FROM users", false},
		{"-- comment\nUPDATE users SET x=1", false},

		// But comments before SELECT are fine
		{"/* comment */ SELECT * FROM t", true},
		{"-- comment\nSELECT * FROM t", true},
	}

	for _, tt := range tests {
		got := IsReadOnly(tt.query)
		if got != tt.want {
			t.Errorf("IsReadOnly(%q) = %v, want %v", tt.query, got, tt.want)
		}
	}
}

func TestClassify(t *testing.T) {
	tests := []struct {
		query string
		want  QueryCategory
	}{
		// SELECT-like
		{"SELECT 1", CategorySelect},
		{"  select * from t", CategorySelect},
		{"SHOW TABLES", CategorySelect},
		{"DESCRIBE users", CategorySelect},
		{"DESC users", CategorySelect},
		{"EXPLAIN SELECT 1", CategorySelect},
		{"WITH cte AS (SELECT 1) SELECT * FROM cte", CategorySelect},

		// Session state
		{"SET @x = 1", CategorySessionState},
		{"SET SESSION sql_mode = 'STRICT_ALL_TABLES'", CategorySessionState},
		{"USE mydb", CategorySessionState},
		{"PREPARE stmt FROM 'SELECT 1'", CategorySessionState},
		{"EXECUTE stmt", CategorySessionState},
		{"DEALLOCATE PREPARE stmt", CategorySessionState},
		{"DO SLEEP(0)", CategorySessionState},

		// Temp table DDL
		{"CREATE TEMPORARY TABLE t (x INT)", CategoryTempTable},
		{"create temporary table t2 (id int)", CategoryTempTable},
		{"DROP TEMPORARY TABLE t", CategoryTempTable},
		{"ALTER TEMPORARY TABLE t ADD y INT", CategoryTempTable},

		// Transactions
		{"BEGIN", CategoryTransaction},
		{"START TRANSACTION", CategoryTransaction},
		{"start  transaction READ ONLY", CategoryTransaction},
		{"COMMIT", CategoryTransaction},
		{"ROLLBACK", CategoryTransaction},
		{"SAVEPOINT sp1", CategoryTransaction},
		{"RELEASE SAVEPOINT sp1", CategoryTransaction},

		// DML
		{"INSERT INTO users VALUES (1)", CategoryDML},
		{"UPDATE users SET x=1", CategoryDML},
		{"DELETE FROM users", CategoryDML},
		{"REPLACE INTO users VALUES (1)", CategoryDML},
		{"LOAD DATA INFILE 'x' INTO TABLE t", CategoryDML},

		// DDL (persistent)
		{"CREATE TABLE t (x INT)", CategoryDDL},
		{"DROP TABLE t", CategoryDDL},
		{"ALTER TABLE t ADD COLUMN y INT", CategoryDDL},
		{"TRUNCATE users", CategoryDDL},
		{"RENAME TABLE t TO u", CategoryDDL},

		// Admin
		{"GRANT SELECT ON *.* TO u", CategoryAdmin},
		{"REVOKE ALL FROM u", CategoryAdmin},
		{"FLUSH PRIVILEGES", CategoryAdmin},
		{"LOCK TABLES t READ", CategoryAdmin},
		{"CALL myproc()", CategoryAdmin},
		{"KILL 42", CategoryAdmin},

		// Comments precede the classified statement
		{"/* trace=abc */ SELECT 1", CategorySelect},
		{"-- note\nINSERT INTO t VALUES (1)", CategoryDML},

		// Edge: identifiers that happen to start with a keyword
		{"SELECTIVE_EXECUTION", CategoryUnknown}, // no whitespace after keyword prefix
		{"SELECT", CategorySelect},               // exact match is allowed

		// Unknown
		{"", CategoryUnknown},
		{"/* only a comment */", CategoryUnknown},
	}
	for _, tt := range tests {
		got := Classify(tt.query)
		if got != tt.want {
			t.Errorf("Classify(%q) = %v, want %v", tt.query, got, tt.want)
		}
	}
}

func TestExtractTempTableName(t *testing.T) {
	tests := []struct {
		query string
		want  string
	}{
		{"CREATE TEMPORARY TABLE t (x INT)", "t"},
		{"create temporary table T (x INT)", "t"},
		{"CREATE TEMPORARY TABLE IF NOT EXISTS foo (id INT)", "foo"},
		{"CREATE TEMPORARY TABLE `my-temp` (x INT)", "my-temp"},
		{"CREATE TEMPORARY TABLE mydb.mytemp (x INT)", "mytemp"},
		{"CREATE TEMPORARY TABLE `mydb`.`mytemp` (x INT)", "mytemp"},
		{"DROP TEMPORARY TABLE t", "t"},
		{"DROP TEMPORARY TABLE IF EXISTS t", "t"},
		{"ALTER TEMPORARY TABLE t ADD COLUMN y INT", "t"},
		{"/* trace */ CREATE TEMPORARY TABLE t (x INT)", "t"},

		// Not a temp statement
		{"CREATE TABLE t (x INT)", ""},
		{"SELECT 1", ""},
	}
	for _, tt := range tests {
		got := ExtractTempTableName(tt.query)
		if got != tt.want {
			t.Errorf("ExtractTempTableName(%q) = %q, want %q", tt.query, got, tt.want)
		}
	}
}

func TestExtractDMLTargetTable(t *testing.T) {
	tests := []struct {
		query string
		want  string
	}{
		{"INSERT INTO t VALUES (1)", "t"},
		{"insert into T values (1)", "t"},
		{"INSERT IGNORE INTO t VALUES (1)", "t"},
		{"INSERT INTO mydb.t VALUES (1)", "t"},
		{"INSERT INTO `t` VALUES (1)", "t"},
		{"REPLACE INTO t VALUES (1)", "t"},
		{"REPLACE t VALUES (1)", "t"},
		{"UPDATE t SET x = 1", "t"},
		{"UPDATE t AS alias SET x = 1", "t"},
		{"DELETE FROM t WHERE id = 1", "t"},
		{"TRUNCATE t", "t"},
		{"TRUNCATE TABLE t", "t"},
		{"DROP TABLE t", "t"},
		{"DROP TABLE IF EXISTS t", "t"},
		{"ALTER TABLE t ADD COLUMN x INT", "t"},
		{"/* hint */ INSERT INTO t VALUES (1)", "t"},

		// Rejects:
		{"SELECT * FROM t", ""},                                // not DML
		{"UPDATE t JOIN u ON t.id = u.id SET t.x = 1", ""},     // multi-table
		{"DELETE t1, t2 FROM t1 JOIN t2 ON t1.id = t2.id", ""}, // multi-table DELETE
		{"INSERT INTO t SELECT * FROM u JOIN v", ""},           // join in subquery → reject
		{"", ""}, // empty
	}
	for _, tt := range tests {
		got := ExtractDMLTargetTable(tt.query)
		if got != tt.want {
			t.Errorf("ExtractDMLTargetTable(%q) = %q, want %q", tt.query, got, tt.want)
		}
	}
}

func TestIsSafeForShadowSession(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		// SAFE for a pinned shadow connection:
		{"SELECT 1", true},
		{"SHOW TABLES", true},
		{"CREATE TEMPORARY TABLE t (x INT)", true},
		{"DROP TEMPORARY TABLE t", true},
		{"SET @v = 1", true},
		{"USE mydb", true},
		{"BEGIN", true},
		{"COMMIT", true},
		{"PREPARE s FROM 'SELECT 1'", true},

		// UNSAFE — would mutate persistent state on shadow:
		{"INSERT INTO users VALUES (1)", false},
		{"UPDATE users SET x=1", false},
		{"DELETE FROM users", false},
		{"TRUNCATE users", false}, // not temporary
		{"CREATE TABLE t (x INT)", false},
		{"DROP TABLE t", false},
		{"ALTER TABLE t ADD y INT", false},
		{"GRANT SELECT ON *.* TO u", false},
		{"CALL myproc()", false},
		{"LOAD DATA INFILE 'x' INTO TABLE t", false},
	}
	for _, tt := range tests {
		got := IsSafeForShadowSession(Classify(tt.query))
		if got != tt.want {
			t.Errorf("IsSafeForShadowSession(Classify(%q)) = %v, want %v", tt.query, got, tt.want)
		}
	}
}
