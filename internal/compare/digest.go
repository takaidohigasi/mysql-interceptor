package compare

import (
	"strings"
	"unicode"
)

// Digest normalizes a SQL query into a digest form by replacing literal values
// with placeholders (?). This groups queries like:
//
//	SELECT * FROM users WHERE id = 1
//	SELECT * FROM users WHERE id = 42
//
// into the same digest:
//
//	SELECT * FROM users WHERE id = ?
func Digest(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return ""
	}

	var b strings.Builder
	b.Grow(len(query))

	i := 0
	for i < len(query) {
		ch := query[i]

		// Collapse whitespace runs into a single space
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			b.WriteByte(' ')
			for i < len(query) && (query[i] == ' ' || query[i] == '\t' || query[i] == '\n' || query[i] == '\r') {
				i++
			}
			continue
		}

		// Single-quoted string literal
		if ch == '\'' {
			b.WriteByte('?')
			i++
			for i < len(query) {
				if query[i] == '\'' {
					i++
					// Escaped quote ''
					if i < len(query) && query[i] == '\'' {
						i++
						continue
					}
					break
				}
				if query[i] == '\\' && i+1 < len(query) {
					i += 2
					continue
				}
				i++
			}
			continue
		}

		// Double-quoted string literal
		if ch == '"' {
			b.WriteByte('?')
			i++
			for i < len(query) {
				if query[i] == '"' {
					i++
					if i < len(query) && query[i] == '"' {
						i++
						continue
					}
					break
				}
				if query[i] == '\\' && i+1 < len(query) {
					i += 2
					continue
				}
				i++
			}
			continue
		}

		// Numeric literal (integers, decimals, hex, negative)
		if isDigit(ch) || (ch == '-' && i+1 < len(query) && isDigit(query[i+1]) && (i == 0 || isNumericContext(query[i-1]))) {
			// Check for hex: 0x...
			if ch == '0' && i+1 < len(query) && (query[i+1] == 'x' || query[i+1] == 'X') {
				b.WriteByte('?')
				i += 2
				for i < len(query) && isHexDigit(query[i]) {
					i++
				}
			} else {
				if ch == '-' {
					i++
				}
				b.WriteByte('?')
				for i < len(query) && isDigit(query[i]) {
					i++
				}
				// Decimal part
				if i < len(query) && query[i] == '.' {
					i++
					for i < len(query) && isDigit(query[i]) {
						i++
					}
				}
				// Scientific notation
				if i < len(query) && (query[i] == 'e' || query[i] == 'E') {
					i++
					if i < len(query) && (query[i] == '+' || query[i] == '-') {
						i++
					}
					for i < len(query) && isDigit(query[i]) {
						i++
					}
				}
			}
			continue
		}

		// IN (...) lists: collapse to IN (?)
		if i+2 < len(query) && strings.EqualFold(query[i:i+2], "IN") {
			j := i + 2
			// Skip whitespace after IN
			for j < len(query) && (query[j] == ' ' || query[j] == '\t') {
				j++
			}
			if j < len(query) && query[j] == '(' {
				b.WriteString(query[i : i+2]) // "IN"
				// skip whitespace
				for k := i + 2; k < j; k++ {
					b.WriteByte(' ')
				}
				b.WriteString("(?)")
				// Skip past the closing paren
				depth := 1
				j++
				for j < len(query) && depth > 0 {
					if query[j] == '(' {
						depth++
					} else if query[j] == ')' {
						depth--
					} else if query[j] == '\'' {
						j++
						for j < len(query) && query[j] != '\'' {
							if query[j] == '\\' {
								j++
							}
							j++
						}
					}
					j++
				}
				i = j
				continue
			}
		}

		// Pass through everything else (keywords, identifiers, operators)
		b.WriteByte(ch)
		i++
	}

	// Normalize to lowercase for consistent grouping
	return strings.ToLower(strings.TrimSpace(b.String()))
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isHexDigit(ch byte) bool {
	return isDigit(ch) || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
}

func isNumericContext(ch byte) bool {
	// Characters after which a '-' likely indicates a negative number
	return ch == '=' || ch == '(' || ch == ',' || ch == ' ' || ch == '\t' ||
		ch == '>' || ch == '<' || ch == '+' || ch == '-' || ch == '*' || ch == '/' ||
		unicode.IsSpace(rune(ch))
}
