package caddymodule

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/VeloxCoding/hazedb"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

// noopNext is the downstream handler; for matched mux paths it is never called.
var noopNext = caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error { return nil })

func do(t *testing.T, h *Handler, path, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	w := httptest.NewRecorder()
	if err := h.ServeHTTP(w, r, noopNext); err != nil {
		t.Fatalf("ServeHTTP %s: %v", path, err)
	}
	var out map[string]any
	if w.Body.Len() > 0 {
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatalf("%s: response not JSON: %q", path, w.Body.String())
		}
	}
	return w, out
}

// The module provisions in memory, runs the full DDL→insert→query path over
// HTTP, and publishes the *DB under "default" for in-process consumers.
func TestModuleEndToEnd(t *testing.T) {
	h := &Handler{}
	if err := h.Provision(caddy.Context{}); err != nil {
		t.Fatalf("provision: %v", err)
	}
	defer h.Cleanup()

	// Shared-instance contract: the PHP extension resolves this same slot.
	if hazedb.LookupDB("default") != h.db {
		t.Fatal("DB not registered under \"default\"")
	}

	// CREATE TABLE via /exec.
	if w, _ := do(t, h, "/exec", `{"sql":"CREATE TABLE users (id uuid primary key, name text, age int)"}`); w.Code != 200 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}

	// INSERT with a UUID arg (string parses to UUID) → affected 1.
	id := hazedb.NewUUIDv7().String()
	w, out := do(t, h, "/exec", `{"sql":"INSERT INTO users (id, name, age) VALUES (?, ?, ?)","args":["`+id+`","alice",30]}`)
	if w.Code != 200 || out["affected"].(float64) != 1 {
		t.Fatalf("insert: %d %v", w.Code, out)
	}

	// SELECT it back via /query.
	w, out = do(t, h, "/query", `{"sql":"SELECT name, age FROM users WHERE id = ?","args":["`+id+`"]}`)
	if w.Code != 200 {
		t.Fatalf("query: %d %s", w.Code, w.Body.String())
	}
	rows := out["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %v", out)
	}
	row := rows[0].([]any)
	if row[0].(string) != "alice" || row[1].(float64) != 30 {
		t.Fatalf("row mismatch: %v", row)
	}

	// A bad SQL string is a 400 with an error envelope, not a panic.
	if w, out := do(t, h, "/query", `{"sql":"SELECT * FROM nope"}`); w.Code != 400 || out["error"] == nil {
		t.Fatalf("expected 400 error envelope, got %d %v", w.Code, out)
	}
}

// GET /meta returns the store-size overview; a non-GET is rejected. The size
// estimate must track payload weight (a 1 KB-text table outweighs an int table).
func TestModuleMeta(t *testing.T) {
	h := &Handler{}
	if err := h.Provision(caddy.Context{}); err != nil {
		t.Fatalf("provision: %v", err)
	}
	defer h.Cleanup()

	if _, err := h.db.Exec("CREATE TABLE small (id uuid primary key, n int)"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.db.Exec("CREATE TABLE big (id uuid primary key, body text)"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		h.db.Exec("INSERT INTO small (id, n) VALUES (?, ?)", hazedb.NewUUIDv7(), i)
		h.db.Exec("INSERT INTO big (id, body) VALUES (?, ?)", hazedb.NewUUIDv7(), strings.Repeat("x", 1000))
	}

	r := httptest.NewRequest(http.MethodGet, "/meta", nil)
	w := httptest.NewRecorder()
	if err := h.ServeHTTP(w, r, noopNext); err != nil {
		t.Fatalf("ServeHTTP /meta: %v", err)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("/meta: %d %s", w.Code, w.Body.String())
	}
	var m hazedb.StoreMeta
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil {
		t.Fatalf("/meta not StoreMeta JSON: %q", w.Body.String())
	}
	if m.Tables != 2 || len(m.TableStats) != 2 {
		t.Fatalf("meta = %+v, want 2 tables", m)
	}
	stat := map[string]hazedb.TableStat{}
	for _, ts := range m.TableStats {
		stat[ts.Name] = ts
	}
	if stat["small"].Rows != 20 || stat["big"].Rows != 20 {
		t.Fatalf("rows: small=%d big=%d, want 20 each", stat["small"].Rows, stat["big"].Rows)
	}
	if stat["big"].ApproxBytes <= stat["small"].ApproxBytes {
		t.Fatalf("big (%d) should outweigh small (%d)", stat["big"].ApproxBytes, stat["small"].ApproxBytes)
	}

	// A non-GET is a clean 405, not a panic or a write.
	rp := httptest.NewRequest(http.MethodPost, "/meta", strings.NewReader("{}"))
	wp := httptest.NewRecorder()
	if err := h.ServeHTTP(wp, rp, noopNext); err != nil {
		t.Fatalf("ServeHTTP POST /meta: %v", err)
	}
	if wp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /meta: got %d, want 405", wp.Code)
	}
}

// Cleanup must clear the registry slot so a removed handler leaves no instance.
func TestModuleCleanupDeregisters(t *testing.T) {
	h := &Handler{}
	if err := h.Provision(caddy.Context{}); err != nil {
		t.Fatal(err)
	}
	if hazedb.LookupDB("default") == nil {
		t.Fatal("not registered after provision")
	}
	if err := h.Cleanup(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if hazedb.LookupDB("default") != nil {
		t.Fatal("still registered after cleanup")
	}
}

// TestBodyLimit: a POST body over MaxBodyBytes is rejected with 413 instead of
// being read into memory — the memory-exhaustion DoS guard.
func TestBodyLimit(t *testing.T) {
	h := &Handler{MaxBodyBytes: 64} // tiny cap; Provision leaves a non-zero value alone
	if err := h.Provision(caddy.Context{}); err != nil {
		t.Fatalf("provision: %v", err)
	}
	defer h.Cleanup()

	big := `{"sql":"SELECT 1","args":["` + strings.Repeat("x", 200) + `"]}` // ~220 B > 64
	if w, _ := do(t, h, "/query", big); w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body: got %d, want 413; body=%s", w.Code, w.Body.String())
	}
	// A small body under the cap still works (sanity: the cap is not rejecting
	// everything).
	if w, _ := do(t, h, "/query", `{"sql":"SELECT 1"}`); w.Code == http.StatusRequestEntityTooLarge {
		t.Fatalf("small body wrongly rejected as too large: %s", w.Body.String())
	}
}

// TestGetListRejectInjection guards the identifier validation on /get and /list,
// which build SQL by concatenating table/cols/col. A refactor could silently
// weaken it, so pin that injection payloads (quote, semicolon, space, …) are
// rejected with 400 and that valid identifiers (incl. whitespace around a comma,
// which is normalized) are accepted.
func TestGetListRejectInjection(t *testing.T) {
	h := &Handler{}
	if err := h.Provision(caddy.Context{}); err != nil {
		t.Fatalf("provision: %v", err)
	}
	defer h.Cleanup()
	if _, err := h.db.Exec("CREATE TABLE users (id uuid primary key, name text)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	get := func(path string) int {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		if err := h.ServeHTTP(w, r, noopNext); err != nil {
			t.Fatalf("ServeHTTP %s: %v", path, err)
		}
		return w.Code
	}

	const validUUID = "00000000-0000-7000-8000-000000000000"
	bad := []string{
		"users;DROP TABLE users", // semicolon
		"users'",                 // single quote
		"users\"",                // double quote
		"a b",                    // space
		"users)",                 // paren
		"1users",                 // leading digit
	}
	for _, v := range bad {
		e := url.QueryEscape(v)
		if code := get("/get?table=" + e + "&id=" + validUUID); code != http.StatusBadRequest {
			t.Errorf("/get table=%q: got %d, want 400", v, code)
		}
		if code := get("/list?table=" + e); code != http.StatusBadRequest {
			t.Errorf("/list table=%q: got %d, want 400", v, code)
		}
		if code := get("/list?table=users&cols=" + e); code != http.StatusBadRequest {
			t.Errorf("/list cols=%q: got %d, want 400", v, code)
		}
		if code := get("/list?table=users&col=" + e + "&val=x"); code != http.StatusBadRequest {
			t.Errorf("/list col=%q: got %d, want 400", v, code)
		}
	}

	// Positive controls: valid identifiers are accepted, and whitespace around a
	// comma in cols is normalized (not rejected) so the SQL matches what was
	// validated.
	if code := get("/list?table=users"); code != http.StatusOK {
		t.Errorf("/list valid table: got %d, want 200", code)
	}
	if code := get("/list?table=users&cols=" + url.QueryEscape("id, name")); code != http.StatusOK {
		t.Errorf("/list cols with spaces: got %d, want 200", code)
	}
}
