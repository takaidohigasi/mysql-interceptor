package compare

import "testing"

func TestDigest_NumberLiterals(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			input: "SELECT * FROM users WHERE id = 1",
			want:  "select * from users where id = ?",
		},
		{
			input: "SELECT * FROM users WHERE id = 42",
			want:  "select * from users where id = ?",
		},
		{
			input: "SELECT * FROM users WHERE id = 1 AND age > 25",
			want:  "select * from users where id = ? and age > ?",
		},
		{
			input: "UPDATE users SET score = 99.5 WHERE id = 3",
			want:  "update users set score = ? where id = ?",
		},
	}

	for _, tt := range tests {
		got := Digest(tt.input)
		if got != tt.want {
			t.Errorf("Digest(%q)\n  got:  %q\n  want: %q", tt.input, got, tt.want)
		}
	}
}

func TestDigest_StringLiterals(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			input: "SELECT * FROM users WHERE name = 'alice'",
			want:  "select * from users where name = ?",
		},
		{
			input: "SELECT * FROM users WHERE name = 'bob' AND email = 'bob@example.com'",
			want:  "select * from users where name = ? and email = ?",
		},
		{
			input: `INSERT INTO users (name) VALUES ('it''s a test')`,
			want:  "insert into users (name) values (?)",
		},
		{
			input: `SELECT * FROM users WHERE name = "alice"`,
			want:  `select * from users where name = ?`,
		},
	}

	for _, tt := range tests {
		got := Digest(tt.input)
		if got != tt.want {
			t.Errorf("Digest(%q)\n  got:  %q\n  want: %q", tt.input, got, tt.want)
		}
	}
}

func TestDigest_INList(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			input: "SELECT * FROM users WHERE id IN (1, 2, 3)",
			want:  "select * from users where id in (?)",
		},
		{
			input: "SELECT * FROM users WHERE id IN (1,2,3,4,5,6,7,8,9,10)",
			want:  "select * from users where id in (?)",
		},
		{
			input: "SELECT * FROM users WHERE name IN ('alice', 'bob')",
			want:  "select * from users where name in (?)",
		},
	}

	for _, tt := range tests {
		got := Digest(tt.input)
		if got != tt.want {
			t.Errorf("Digest(%q)\n  got:  %q\n  want: %q", tt.input, got, tt.want)
		}
	}
}

func TestDigest_WhitespaceNormalization(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			input: "  SELECT   *   FROM   users  ",
			want:  "select * from users",
		},
		{
			input: "SELECT *\nFROM users\nWHERE id = 1",
			want:  "select * from users where id = ?",
		},
	}

	for _, tt := range tests {
		got := Digest(tt.input)
		if got != tt.want {
			t.Errorf("Digest(%q)\n  got:  %q\n  want: %q", tt.input, got, tt.want)
		}
	}
}

func TestDigest_SameDigestForDifferentParams(t *testing.T) {
	q1 := Digest("SELECT * FROM users WHERE id = 1 AND name = 'alice'")
	q2 := Digest("SELECT * FROM users WHERE id = 999 AND name = 'bob'")

	if q1 != q2 {
		t.Errorf("expected same digest for parameterized queries:\n  q1: %s\n  q2: %s", q1, q2)
	}
}

func TestDigest_Empty(t *testing.T) {
	if got := Digest(""); got != "" {
		t.Errorf("expected empty digest for empty input, got: %q", got)
	}
}

func TestDigest_StripsComments(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			input: "/* trace_id=abc123 */ SELECT * FROM users WHERE id = 1",
			want:  "select * from users where id = ?",
		},
		{
			input: "-- app annotation\nSELECT 1",
			want:  "select ?",
		},
		{
			input: "SELECT * FROM users /* hint */ WHERE id = 1",
			want:  "select * from users where id = ?",
		},
	}
	for _, tt := range tests {
		got := Digest(tt.input)
		if got != tt.want {
			t.Errorf("Digest(%q)\n  got:  %q\n  want: %q", tt.input, got, tt.want)
		}
	}
}

func TestDigest_CommentsWithNumbersDontLeak(t *testing.T) {
	// Traces/hints with embedded numbers must not produce distinct digests.
	q1 := Digest("/* trace=abc123 */ SELECT * FROM users WHERE id = 1")
	q2 := Digest("/* trace=xyz999 */ SELECT * FROM users WHERE id = 2")
	if q1 != q2 {
		t.Errorf("expected same digest for comment-annotated queries:\n  q1: %s\n  q2: %s", q1, q2)
	}
}

func TestDigest_HexLiteral(t *testing.T) {
	got := Digest("SELECT * FROM users WHERE token = 0xDEADBEEF")
	want := "select * from users where token = ?"
	if got != want {
		t.Errorf("Digest hex\n  got:  %q\n  want: %q", got, want)
	}
}
