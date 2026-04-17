package compare

import "fmt"

// FormatCellValue converts a MySQL column value (as returned by go-mysql's
// FieldValue.Value()) to its human-readable string form.
//
// go-mysql returns string/VARCHAR/TEXT/BLOB columns as []byte. The default
// fmt.Sprintf("%v", []byte) produces the numeric array representation
// (e.g. "[97 108 105 99 101]" for "alice"), which makes diff reports
// unreadable. This helper renders []byte as a string instead.
//
// NULL values are rendered as the literal "NULL" so they can be distinguished
// from empty strings in comparisons.
func FormatCellValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case []byte:
		return string(val)
	case string:
		return val
	default:
		return fmt.Sprintf("%v", v)
	}
}
