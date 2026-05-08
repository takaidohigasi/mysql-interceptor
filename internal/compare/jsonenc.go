package compare

import (
	"math"
	"strconv"
)

// Hand-rolled JSON encoders for the report types (CompareResult,
// Difference, HeartbeatRecord). Used by Reporter.emit so the per-
// record hot path skips encoding/json's reflection + interface
// dispatch entirely.
//
// Output is byte-identical to json.Encoder with SetEscapeHTML(false)
// for valid UTF-8 inputs over the value ranges we actually emit
// (timing floats in ms, ASCII / UTF-8 query strings, MySQL session
// IDs). The TestJSONEnc_MatchesEncodingJSON tests pin compatibility
// with a representative corpus; if the report schema changes, the
// appendJSON methods AND those tests must be updated together.

// appendJSONString appends s as a JSON string literal (with
// surrounding double quotes) to buf. Escaping matches encoding/json
// with SetEscapeHTML(false): ", \, and ASCII control characters
// (< 0x20) are escaped, and so are U+2028 / U+2029 (encoding/json
// escapes those even with EscapeHTML=false because they're invalid
// in JSONP and some browser embed contexts). HTML-significant
// characters (<, >, &) and other UTF-8 sequences pass through.
func appendJSONString(buf []byte, s string) []byte {
	buf = append(buf, '"')
	start := 0
	for i := 0; i < len(s); i++ {
		b := s[i]
		// Fast path: ordinary printable ASCII bytes (and UTF-8
		// continuation bytes) need no escaping. The branch covers
		// 0x20-0x7F minus " and \, plus 0x80-0xFF. UTF-8 lead /
		// continuation bytes are checked below for the U+2028 /
		// U+2029 special case before the fast path passes them
		// through verbatim.
		if b >= 0x20 && b != '"' && b != '\\' {
			// U+2028 = 0xE2 0x80 0xA8, U+2029 = 0xE2 0x80 0xA9.
			// Detect on the lead byte; encoding/json escapes both
			// regardless of EscapeHTML so we do the same.
			if b == 0xE2 && i+2 < len(s) && s[i+1] == 0x80 && (s[i+2] == 0xA8 || s[i+2] == 0xA9) {
				if start < i {
					buf = append(buf, s[start:i]...)
				}
				buf = append(buf, '\\', 'u', '2', '0', '2', '8')
				if s[i+2] == 0xA9 {
					buf[len(buf)-1] = '9'
				}
				i += 2
				start = i + 1
			}
			continue
		}
		if start < i {
			buf = append(buf, s[start:i]...)
		}
		switch b {
		case '"':
			buf = append(buf, '\\', '"')
		case '\\':
			buf = append(buf, '\\', '\\')
		case '\b':
			buf = append(buf, '\\', 'b')
		case '\f':
			buf = append(buf, '\\', 'f')
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\t':
			buf = append(buf, '\\', 't')
		default:
			// 0x00-0x1F that aren't named above: \u00XX form.
			buf = append(buf, '\\', 'u', '0', '0',
				hexDigit(b>>4), hexDigit(b&0x0F))
		}
		start = i + 1
	}
	if start < len(s) {
		buf = append(buf, s[start:]...)
	}
	buf = append(buf, '"')
	return buf
}

// hexDigit returns the lowercase hex character for a nibble. JSON
// allows either case; encoding/json emits lowercase, so we match.
func hexDigit(b byte) byte {
	if b < 10 {
		return '0' + b
	}
	return 'a' + b - 10
}

// appendJSONFloat appends f using encoding/json's exact format
// rules: 'f' for normal magnitudes, 'e' for very small / very large,
// with the same e-exponent canonicalization (e-09 → e-9).
func appendJSONFloat(buf []byte, f float64) []byte {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		// encoding/json refuses NaN/Inf with an error. We can't
		// surface an error from the writer goroutine path cleanly,
		// so emit a JSON null which any consumer can recognize as
		// "value missing". This path is exceedingly unlikely with
		// timing values in ms.
		return append(buf, 'n', 'u', 'l', 'l')
	}
	abs := math.Abs(f)
	fmtChar := byte('f')
	if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		fmtChar = 'e'
	}
	n := len(buf)
	buf = strconv.AppendFloat(buf, f, fmtChar, -1, 64)
	if fmtChar == 'e' {
		// Canonicalize "e-09" → "e-9" / "e+09" → "e+9" to mirror
		// encoding/json. The exponent always sits at the very end
		// of the appended bytes.
		end := len(buf)
		if end-n >= 4 && buf[end-4] == 'e' && (buf[end-3] == '-' || buf[end-3] == '+') && buf[end-2] == '0' {
			buf[end-2] = buf[end-1]
			buf = buf[:end-1]
		}
	}
	return buf
}

// appendJSONBool appends "true" or "false".
func appendJSONBool(buf []byte, b bool) []byte {
	if b {
		return append(buf, 't', 'r', 'u', 'e')
	}
	return append(buf, 'f', 'a', 'l', 's', 'e')
}
