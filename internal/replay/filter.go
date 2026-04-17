package replay

import "strings"

// QueryCategory classifies a statement by what kind of state it touches.
// The shadow path uses this to decide whether a query is safe to run
// against a session-pinned shadow connection without mutating persistent
// data on the shadow server.
type QueryCategory int

const (
	CategoryUnknown      QueryCategory = iota
	CategorySelect                     // SELECT, SHOW, DESC, EXPLAIN, WITH…SELECT
	CategorySessionState               // SET, USE, PREPARE/EXECUTE/DEALLOCATE, DO, HELP, RESET CONNECTION
	CategoryTempTable                  // CREATE/DROP/ALTER/TRUNCATE TEMPORARY TABLE
	CategoryTransaction                // BEGIN, START TRANSACTION, COMMIT, ROLLBACK, SAVEPOINT…
	CategoryDML                        // INSERT, UPDATE, DELETE, REPLACE, MERGE, LOAD DATA
	CategoryDDL                        // persistent schema changes
	CategoryAdmin                      // GRANT/REVOKE/FLUSH/LOCK/CALL and other side-effectful commands
)

// Classify returns the category of a SQL statement by inspecting its
// leading keyword (after stripping comments and whitespace). Case-insensitive.
func Classify(query string) QueryCategory {
	q := stripLeadingCommentsAndWS(query)
	if q == "" {
		return CategoryUnknown
	}
	up := strings.ToUpper(q)

	// Leading-keyword decisions. Order matters: we check the longest
	// matches first (e.g. START TRANSACTION before START).
	switch {
	case hasKeyword(up, "SELECT"),
		hasKeyword(up, "SHOW"),
		hasKeyword(up, "DESCRIBE"),
		hasKeyword(up, "DESC"),
		hasKeyword(up, "EXPLAIN"),
		hasKeyword(up, "WITH"):
		return CategorySelect

	case hasKeyword(up, "SET"),
		hasKeyword(up, "USE"),
		hasKeyword(up, "PREPARE"),
		hasKeyword(up, "EXECUTE"),
		hasKeyword(up, "DEALLOCATE"),
		hasKeyword(up, "DO"),
		hasKeyword(up, "HELP"):
		return CategorySessionState

	case hasKeyword(up, "BEGIN"),
		startsWithPhrase(up, "START", "TRANSACTION"),
		hasKeyword(up, "COMMIT"),
		hasKeyword(up, "ROLLBACK"),
		hasKeyword(up, "SAVEPOINT"),
		startsWithPhrase(up, "RELEASE", "SAVEPOINT"):
		return CategoryTransaction

	case startsWithPhrase(up, "CREATE", "TEMPORARY"),
		startsWithPhrase(up, "DROP", "TEMPORARY"),
		startsWithPhrase(up, "ALTER", "TEMPORARY"),
		startsWithPhrase(up, "TRUNCATE", "TEMPORARY"):
		return CategoryTempTable

	case hasKeyword(up, "INSERT"),
		hasKeyword(up, "UPDATE"),
		hasKeyword(up, "DELETE"),
		hasKeyword(up, "REPLACE"),
		hasKeyword(up, "MERGE"),
		startsWithPhrase(up, "LOAD", "DATA"):
		return CategoryDML

	case hasKeyword(up, "CREATE"),
		hasKeyword(up, "DROP"),
		hasKeyword(up, "ALTER"),
		hasKeyword(up, "RENAME"),
		hasKeyword(up, "TRUNCATE"):
		return CategoryDDL

	case hasKeyword(up, "GRANT"),
		hasKeyword(up, "REVOKE"),
		hasKeyword(up, "FLUSH"),
		hasKeyword(up, "RESET"),
		hasKeyword(up, "LOCK"),
		hasKeyword(up, "UNLOCK"),
		hasKeyword(up, "CALL"),
		hasKeyword(up, "KILL"),
		hasKeyword(up, "PURGE"):
		return CategoryAdmin
	}

	return CategoryUnknown
}

// IsReadOnly is the strict filter used by offline replay: only pure
// SELECT-like queries are accepted.
func IsReadOnly(query string) bool {
	return Classify(query) == CategorySelect
}

// IsSafeForShadowSession is the broader filter used by session-pinned
// shadow replay. It accepts anything whose side-effects are confined to
// the shadow connection itself (session variables, temp tables,
// transactions), so the shadow state can track the primary's state.
// It still rejects anything that would mutate persistent data.
func IsSafeForShadowSession(category QueryCategory) bool {
	switch category {
	case CategorySelect, CategorySessionState, CategoryTempTable, CategoryTransaction:
		return true
	default:
		return false
	}
}

// stripLeadingCommentsAndWS removes leading whitespace plus any
// /* … */ block comments and -- line comments that precede the first
// real keyword. Returns the remainder.
func stripLeadingCommentsAndWS(query string) string {
	q := strings.TrimSpace(query)
	for {
		if strings.HasPrefix(q, "/*") {
			end := strings.Index(q, "*/")
			if end == -1 {
				return ""
			}
			q = strings.TrimSpace(q[end+2:])
			continue
		}
		if strings.HasPrefix(q, "--") {
			nl := strings.Index(q, "\n")
			if nl == -1 {
				return ""
			}
			q = strings.TrimSpace(q[nl+1:])
			continue
		}
		return q
	}
}

// hasKeyword returns true if s starts with kw and the following character
// is a word-boundary (space, end, or ";("). Avoids matching identifiers
// that happen to share a prefix with a keyword (e.g. "SELECTIVE").
func hasKeyword(s, kw string) bool {
	if !strings.HasPrefix(s, kw) {
		return false
	}
	if len(s) == len(kw) {
		return true
	}
	c := s[len(kw)]
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ';' || c == '('
}

// startsWithPhrase checks for "<kw1> [whitespace]+ <kw2>" as the first
// two tokens, case-insensitive on the caller's upper-cased input.
func startsWithPhrase(s, kw1, kw2 string) bool {
	if !hasKeyword(s, kw1) {
		return false
	}
	rest := strings.TrimLeft(s[len(kw1):], " \t\r\n")
	return hasKeyword(rest, kw2)
}

// parseIdent consumes one identifier from the front of s (either
// `backtick-quoted` or plain [A-Za-z0-9_$]+). Returns the unquoted name
// and the remainder of s. Returns ("", s) if no identifier is found.
func parseIdent(s string) (name, rest string) {
	s = strings.TrimLeft(s, " \t\r\n")
	if s == "" {
		return "", ""
	}
	if s[0] == '`' {
		end := strings.IndexByte(s[1:], '`')
		if end == -1 {
			return "", s
		}
		return s[1 : 1+end], s[1+end+1:]
	}
	i := 0
	for i < len(s) {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '_' || c == '$' {
			i++
			continue
		}
		break
	}
	if i == 0 {
		return "", s
	}
	return s[:i], s[i:]
}

// parseTableName parses "[schema.]table" and returns only the table
// part (lowercased). Returns ("", s) if no identifier is found.
func parseTableName(s string) (name, rest string) {
	first, r := parseIdent(s)
	if first == "" {
		return "", s
	}
	r2 := strings.TrimLeft(r, " \t")
	if strings.HasPrefix(r2, ".") {
		second, r3 := parseIdent(r2[1:])
		if second != "" {
			return strings.ToLower(second), r3
		}
	}
	return strings.ToLower(first), r
}

// ExtractTempTableName returns the table name from
// CREATE/DROP/ALTER TEMPORARY TABLE statements, or "" if the query
// isn't a temp-table statement or can't be parsed cleanly. Normalizes
// to lowercase and drops backticks/schema qualifier.
//
// Multi-table forms like "DROP TEMPORARY TABLE t1, t2" return only the
// first; callers should be aware. For single-table creates this is fine.
func ExtractTempTableName(query string) string {
	q := stripLeadingCommentsAndWS(query)
	up := strings.ToUpper(q)

	var rest string
	switch {
	case startsWithPhrase(up, "CREATE", "TEMPORARY"):
		rest = afterPhrase(q, "CREATE", "TEMPORARY")
	case startsWithPhrase(up, "DROP", "TEMPORARY"):
		rest = afterPhrase(q, "DROP", "TEMPORARY")
	case startsWithPhrase(up, "ALTER", "TEMPORARY"):
		rest = afterPhrase(q, "ALTER", "TEMPORARY")
	default:
		return ""
	}

	rest = strings.TrimLeft(rest, " \t\r\n")
	upR := strings.ToUpper(rest)
	if !hasKeyword(upR, "TABLE") {
		return ""
	}
	rest = strings.TrimLeft(rest[len("TABLE"):], " \t\r\n")
	upR = strings.ToUpper(rest)

	// Skip optional IF [NOT] EXISTS
	if strings.HasPrefix(upR, "IF NOT EXISTS") {
		rest = strings.TrimLeft(rest[len("IF NOT EXISTS"):], " \t\r\n")
	} else if strings.HasPrefix(upR, "IF EXISTS") {
		rest = strings.TrimLeft(rest[len("IF EXISTS"):], " \t\r\n")
	}

	name, _ := parseTableName(rest)
	return name
}

// ExtractDMLTargetTable returns the primary target table of a DML/DDL
// statement (INSERT, UPDATE, DELETE, REPLACE, DROP TABLE, TRUNCATE,
// ALTER TABLE) as lowercase with any schema qualifier stripped.
//
// Returns "" if the query isn't one of these forms or looks multi-table
// (contains JOIN). We err on the side of caution — false negatives just
// mean a query that could have been forwarded gets rejected instead.
func ExtractDMLTargetTable(query string) string {
	q := stripLeadingCommentsAndWS(query)
	up := strings.ToUpper(q)

	var rest string
	switch {
	case hasKeyword(up, "INSERT"):
		rest = strings.TrimLeft(q[len("INSERT"):], " \t\r\n")
		upR := strings.ToUpper(rest)
		if hasKeyword(upR, "IGNORE") {
			rest = strings.TrimLeft(rest[len("IGNORE"):], " \t\r\n")
			upR = strings.ToUpper(rest)
		}
		if hasKeyword(upR, "INTO") {
			rest = strings.TrimLeft(rest[len("INTO"):], " \t\r\n")
		}
	case hasKeyword(up, "REPLACE"):
		rest = strings.TrimLeft(q[len("REPLACE"):], " \t\r\n")
		if hasKeyword(strings.ToUpper(rest), "INTO") {
			rest = strings.TrimLeft(rest[len("INTO"):], " \t\r\n")
		}
	case hasKeyword(up, "DELETE"):
		rest = strings.TrimLeft(q[len("DELETE"):], " \t\r\n")
		if !hasKeyword(strings.ToUpper(rest), "FROM") {
			return "" // multi-table DELETE or unusual form
		}
		rest = strings.TrimLeft(rest[len("FROM"):], " \t\r\n")
	case hasKeyword(up, "UPDATE"):
		rest = strings.TrimLeft(q[len("UPDATE"):], " \t\r\n")
	case hasKeyword(up, "TRUNCATE"):
		rest = strings.TrimLeft(q[len("TRUNCATE"):], " \t\r\n")
		if hasKeyword(strings.ToUpper(rest), "TABLE") {
			rest = strings.TrimLeft(rest[len("TABLE"):], " \t\r\n")
		}
	case startsWithPhrase(up, "DROP", "TABLE"):
		rest = afterPhrase(q, "DROP", "TABLE")
		rest = strings.TrimLeft(rest, " \t\r\n")
		if strings.HasPrefix(strings.ToUpper(rest), "IF EXISTS") {
			rest = strings.TrimLeft(rest[len("IF EXISTS"):], " \t\r\n")
		}
	case startsWithPhrase(up, "ALTER", "TABLE"):
		rest = afterPhrase(q, "ALTER", "TABLE")
		rest = strings.TrimLeft(rest, " \t\r\n")
	default:
		return ""
	}

	name, after := parseTableName(rest)
	if name == "" {
		return ""
	}
	// Conservative: if we see JOIN anywhere downstream, assume multi-table
	// and reject. Comma-joins (implicit cross-join in UPDATE/DELETE)
	// likewise suggest multi-table; reject.
	afterUp := strings.ToUpper(after)
	if strings.Contains(afterUp, "JOIN ") {
		return ""
	}
	return name
}

// afterPhrase returns the substring following the two whitespace-separated
// keywords kw1 kw2. Assumes startsWithPhrase(upper(s), kw1, kw2) is true
// for case-insensitive input.
func afterPhrase(s, kw1, kw2 string) string {
	r := strings.TrimLeft(s[len(kw1):], " \t\r\n")
	return r[len(kw2):]
}
