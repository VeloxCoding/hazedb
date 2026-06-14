package hazedb

import (
	"errors"
	"testing"
)

// ExecScript runs a trusted multi-statement seed file: it splits on top-level ';'
// with the lexer (so a ';' inside a string literal does not split), permits inline
// literals (a seed file has no ? args), and does not cache — so the same literal
// SQL via the normal Exec path is still rejected.
func TestExecScript(t *testing.T) {
	db := openEmpty(t)
	script := `
		CREATE TABLE cfg (id uuid primary key, k text, v text);
		INSERT INTO cfg (id, k, v) VALUES ('00000000-0000-7000-8000-000000000001', 'greeting', 'hello; world');
		INSERT INTO cfg (id, k, v) VALUES ('00000000-0000-7000-8000-000000000002', 'flag', 'on');
	`
	n, err := db.ExecScript(script)
	if err != nil {
		t.Fatalf("ExecScript: %v", err)
	}
	if n != 2 { // two INSERTs; CREATE TABLE counts 0
		t.Fatalf("affected = %d, want 2", n)
	}

	// The ';' inside 'hello; world' must not have split the statement.
	_, rows, err := db.Query("SELECT v FROM cfg WHERE k = ?", "greeting")
	if err != nil || len(rows) != 1 || rows[0][0].Str() != "hello; world" {
		t.Fatalf("greeting: rows=%v err=%v", rows, err)
	}

	// The trusted plan was not cached, so the identical literal SQL via the normal
	// Exec path still hits the inline-literal ban.
	if _, err := db.Exec("INSERT INTO cfg (id, k, v) VALUES ('00000000-0000-7000-8000-000000000002', 'flag', 'on')"); !errors.Is(err, ErrParse) {
		t.Fatalf("literal Exec must be rejected, got %v", err)
	}
}

// A failing statement stops the script, names itself in the error, and leaves the
// statements before it applied (no implicit rollback across statements).
func TestExecScriptStopsOnError(t *testing.T) {
	db := openEmpty(t)
	_, err := db.ExecScript("CREATE TABLE t (id uuid primary key, n int); INSERT INTO t (id, n) VALUES ('not-a-uuid', 1)")
	if err == nil {
		t.Fatal("expected error from the bad UUID literal")
	}
	// The CREATE TABLE before the failure took effect.
	if _, _, err := db.Query("SELECT n FROM t"); err != nil {
		t.Fatalf("table from the first statement should exist: %v", err)
	}
}
