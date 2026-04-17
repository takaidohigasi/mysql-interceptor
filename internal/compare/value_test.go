package compare

import "testing"

func TestFormatCellValue(t *testing.T) {
	tests := []struct {
		name string
		in   interface{}
		want string
	}{
		{"nil renders as NULL", nil, "NULL"},
		{"string passes through", "alice", "alice"},
		{"empty string stays empty", "", ""},
		{"byte slice renders as string", []byte("alice"), "alice"},
		{"empty byte slice", []byte{}, ""},
		{"int64 as decimal", int64(42), "42"},
		{"uint64 as decimal", uint64(100), "100"},
		{"float64 as decimal", 3.14, "3.14"},
		{"bool true", true, "true"},
	}
	for _, tt := range tests {
		got := FormatCellValue(tt.in)
		if got != tt.want {
			t.Errorf("%s: got %q, want %q", tt.name, got, tt.want)
		}
	}
}
