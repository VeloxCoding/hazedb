package hazedb

import (
	"errors"
	"strings"
	"testing"
)

// A crafted query with very deep parenthesis or NOT nesting must be rejected with
// a normal parse error, never run the recursive-descent parser into a stack
// overflow (a fatal runtime error recover() cannot catch — a one-request kill of
// the process and the in-memory DB). The guard lives in the parser so both the
// HTTP and the cgo PHP path are covered. Nesting just under the limit still
// parses, so the cap never clips a real query.
func TestParseExprDepthLimit(t *testing.T) {
	// Far past maxExprDepth but far below any depth that could overflow the
	// stack — proves the guard fires early, as a clean error.
	deepParens := "SELECT * FROM t WHERE " + strings.Repeat("(", 5000) + "1=1" + strings.Repeat(")", 5000)
	if _, err := parseSQL(deepParens); !errors.Is(err, ErrParse) {
		t.Fatalf("deep parens: got %v, want ErrParse", err)
	}

	deepNot := "SELECT * FROM t WHERE " + strings.Repeat("NOT ", 5000) + "active"
	if _, err := parseSQL(deepNot); !errors.Is(err, ErrParse) {
		t.Fatalf("deep NOT: got %v, want ErrParse", err)
	}

	// Well under the limit: a genuinely (if unusually) nested query still parses.
	okParens := "SELECT * FROM t WHERE " + strings.Repeat("(", 200) + "1=1" + strings.Repeat(")", 200)
	if _, err := parseSQL(okParens); err != nil {
		t.Fatalf("200-deep parens should parse: %v", err)
	}
	okNot := "SELECT * FROM t WHERE " + strings.Repeat("NOT ", 200) + "active"
	if _, err := parseSQL(okNot); err != nil {
		t.Fatalf("200-deep NOT should parse: %v", err)
	}
}
