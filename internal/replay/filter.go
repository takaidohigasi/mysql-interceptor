package replay

import "strings"

// IsReadOnly reports whether the query is safe to replay (SELECT-like only).
// This is a hard safety boundary — UPDATE/DELETE/INSERT/DDL must never be
// replayed against any target server, as it would mutate data.
func IsReadOnly(query string) bool {
	q := strings.TrimSpace(strings.ToUpper(query))
	// Strip leading comments like /* ... */ and -- ...
	for {
		if strings.HasPrefix(q, "/*") {
			end := strings.Index(q, "*/")
			if end == -1 {
				return false
			}
			q = strings.TrimSpace(q[end+2:])
			continue
		}
		if strings.HasPrefix(q, "--") {
			nl := strings.Index(q, "\n")
			if nl == -1 {
				return false
			}
			q = strings.TrimSpace(q[nl+1:])
			continue
		}
		break
	}

	return strings.HasPrefix(q, "SELECT") ||
		strings.HasPrefix(q, "SHOW") ||
		strings.HasPrefix(q, "DESCRIBE") ||
		strings.HasPrefix(q, "DESC ") ||
		strings.HasPrefix(q, "EXPLAIN") ||
		strings.HasPrefix(q, "WITH ") // CTEs that end in SELECT
}
