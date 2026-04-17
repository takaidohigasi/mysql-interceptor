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
