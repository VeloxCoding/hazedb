package caddymodule

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
