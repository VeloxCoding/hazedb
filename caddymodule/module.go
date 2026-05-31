// Package caddymodule exposes hazedb as a Caddy HTTP handler.
//
// The core hazedb package stays stdlib-only (plus goccy/go-json); this adapter
// owns the transport: it opens a *DB, serves POST /query and POST /exec over an
// internal mux, and registers the *DB under a name in the core registry so an
// in-process consumer (the FrankenPHP/PHP extension) reaches the very same
// instance. Per the gateway boundary in the RFC, request-context cross-cutting
// concerns (auth, per-tenant routing, rate limits) belong here, never in core.
//
// Schema: the module opens with an empty schema by default. hazedb supports
// runtime CREATE TABLE, so operators define tables via an init_sql file (run
// once at Provision) or by POSTing DDL to /exec.
//
// WAL + reload caveat: with wal_path set, Caddy config reload runs the new
// Provision (which opens the same file) before the old Cleanup (which closes
// it) — two writers on one WAL file for that window. Memory mode (no wal_path)
// reloads cleanly. For durable deployments, restart rather than graceful-reload
// when changing this handler.
package caddymodule

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/VeloxCoding/hazedb"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

// Handler is the Caddy HTTP handler that embeds hazedb.
type Handler struct {
	// WALLevel is the durability switch: 0 = memory only, 1 = background fsync
	// (~1s window), 2 = fsync every write (slowest, safest). Above 0, WALPath
	// is required.
	WALLevel int `json:"wal_level,omitempty"`
	// WALPath is the directory holding the write-ahead log segments. Required
	// when wal_level > 0.
	WALPath string `json:"wal_path,omitempty"`
	// WALRotateMillis is how often the active segment is sealed, in ms. 0 = 5s.
	WALRotateMillis int `json:"wal_rotate_ms,omitempty"`
	// SQLitePath enables the on-disk SQLite mirror at this path (system of record
	// + recovery source). Empty = no mirror. Requires wal_level > 0.
	SQLitePath string `json:"sqlite_path,omitempty"`
	// InitSQL is an absolute path to a .sql file run once at Provision, before
	// Caddy serves — typically CREATE TABLE + seed rows. Statements are split on
	// ';'; do not put a semicolon inside a string literal in this file.
	InitSQL string `json:"init_sql,omitempty"`
	// RegistryName is the name the *DB is published under for in-process
	// consumers. Empty = "default" (what the PHP extension looks up).
	RegistryName string `json:"registry_name,omitempty"`

	db   *hazedb.DB
	mux  *http.ServeMux
	name string
}

// CaddyModule registers the handler under http.handlers.* so it works as a
// `handle`/`hazedb` directive or a JSON handler entry.
func (Handler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.hazedb",
		New: func() caddy.Module { return new(Handler) },
	}
}

// Provision opens the *DB, runs init_sql, wires the routes, and registers the
// instance. Called once per module instance at Caddy start / config reload.
func (h *Handler) Provision(ctx caddy.Context) error {
	if h.WALRotateMillis < 0 {
		return fmt.Errorf("hazedb: wal_rotate_ms must be >= 0")
	}
	opts := hazedb.Options{
		Schema:     hazedb.Schema{}, // tables created at runtime (init_sql / POST /exec)
		WALLevel:   h.WALLevel,
		WALPath:    h.WALPath,
		SQLitePath: h.SQLitePath,
	}
	if h.WALRotateMillis > 0 {
		opts.WALRotateInterval = time.Duration(h.WALRotateMillis) * time.Millisecond
	}
	db, err := hazedb.Open(opts)
	if err != nil {
		return fmt.Errorf("hazedb: open: %w", err)
	}
	h.db = db

	if h.InitSQL != "" {
		if err := h.runInitSQL(h.InitSQL); err != nil {
			_ = db.Close()
			h.db = nil
			return fmt.Errorf("hazedb: init_sql %q: %w", h.InitSQL, err)
		}
	}

	h.mux = http.NewServeMux()
	h.mux.HandleFunc("/query", h.handleQuery)
	h.mux.HandleFunc("/exec", h.handleExec)

	h.name = h.RegistryName
	if h.name == "" {
		h.name = "default"
	}
	hazedb.RegisterDB(h.name, db)
	return nil
}

// runInitSQL runs each ';'-separated statement from the file through Exec.
func (h *Handler) runInitSQL(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for _, stmt := range strings.Split(string(data), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := h.db.Exec(stmt); err != nil {
			return fmt.Errorf("statement %q: %w", stmt, err)
		}
	}
	return nil
}

// Cleanup deregisters and closes the *DB. DeregisterDBIf is the CAS-safe form:
// during a config reload the new Provision has already overwritten the slot, so
// this won't clobber it; it only clears when the handler is fully removed.
func (h *Handler) Cleanup() error {
	if h.db != nil {
		hazedb.DeregisterDBIf(h.name, h.db)
		err := h.db.Close()
		h.db = nil
		return err
	}
	return nil
}

// ServeHTTP dispatches to the hazedb mux; unmatched paths fall through to the
// next handler, so the module can be mounted under a prefix alongside others.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	if handler, pattern := h.mux.Handler(r); pattern != "" {
		handler.ServeHTTP(w, r)
		return nil
	}
	return next.ServeHTTP(w, r)
}

// sqlRequest is the POST body for /query and /exec: a SQL string plus optional
// positional args as a JSON array (see hazedb.ArgsFromJSON for the type mapping).
type sqlRequest struct {
	SQL  string          `json:"sql"`
	Args json.RawMessage `json:"args"`
}

func (h *Handler) decode(w http.ResponseWriter, r *http.Request) (sqlRequest, []any, bool) {
	var req sqlRequest
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, hazedb.ErrorJSON("use POST with a JSON body"))
		return req, nil, false
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, hazedb.ErrorJSON("invalid JSON body: "+err.Error()))
		return req, nil, false
	}
	args, err := hazedb.ArgsFromJSON(req.Args)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, hazedb.ErrorJSON(err.Error()))
		return req, nil, false
	}
	return req, args, true
}

// handleQuery runs a SELECT and returns {"columns":[...],"rows":[[...],...]}.
func (h *Handler) handleQuery(w http.ResponseWriter, r *http.Request) {
	req, args, ok := h.decode(w, r)
	if !ok {
		return
	}
	cols, rows, err := h.db.Query(req.SQL, args...)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, hazedb.ErrorJSON(err.Error()))
		return
	}
	body, err := hazedb.RowsToJSON(cols, rows)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, hazedb.ErrorJSON(err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, body)
}

// handleExec runs INSERT/UPDATE/DELETE/CREATE/DROP and returns {"affected":n}.
func (h *Handler) handleExec(w http.ResponseWriter, r *http.Request) {
	req, args, ok := h.decode(w, r)
	if !ok {
		return
	}
	n, err := h.db.Exec(req.SQL, args...)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, hazedb.ErrorJSON(err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, hazedb.ExecResultJSON(n))
}

func writeJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// UnmarshalCaddyfile parses the `hazedb` handler directive. Example:
//
//	hazedb {
//	    wal_level       1
//	    wal_path        /var/lib/hazedb/wal
//	    wal_rotation    5s
//	    sqlite_path     /var/lib/hazedb/hazedb.db
//	    init_sql        /etc/hazedb/schema.sql
//	    registry_name   default
//	}
func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		if d.NextArg() {
			return d.ArgErr()
		}
		for d.NextBlock(0) {
			key := d.Val()
			if !d.NextArg() {
				return d.ArgErr()
			}
			value := d.Val()
			switch key {
			case "wal_path":
				h.WALPath = value
			case "sqlite_path":
				h.SQLitePath = value
			case "init_sql":
				h.InitSQL = value
			case "registry_name":
				h.RegistryName = value
			case "wal_level":
				n, err := strconv.Atoi(value)
				if err != nil {
					return d.Errf("%s: %v", key, err)
				}
				h.WALLevel = n
			case "wal_rotation":
				dur, err := time.ParseDuration(value)
				if err != nil {
					return d.Errf("%s: %v", key, err)
				}
				h.WALRotateMillis = int(dur / time.Millisecond)
			default:
				return d.Errf("unrecognized option: %s", key)
			}
		}
	}
	return nil
}

// parseCaddyfile is the Caddyfile entry point for the `hazedb { ... }` directive.
func parseCaddyfile(helper httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	var m Handler
	if err := m.UnmarshalCaddyfile(helper.Dispenser); err != nil {
		return nil, err
	}
	return &m, nil
}

func init() {
	caddy.RegisterModule(Handler{})
	httpcaddyfile.RegisterHandlerDirective("hazedb", parseCaddyfile)
	httpcaddyfile.RegisterDirectiveOrder("hazedb", httpcaddyfile.Before, "respond")
}

var (
	_ caddy.Module                = (*Handler)(nil)
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddy.CleanerUpper          = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
	_ caddyfile.Unmarshaler       = (*Handler)(nil)
)
