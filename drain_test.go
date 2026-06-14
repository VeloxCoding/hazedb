package hazedb

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// renderVal / renderAny render an engine cell and a SQLite-scanned value to the
// SAME canonical string, compared position-wise (so a typed column always
// matches its mirror column). Bytes/UUID -> hex, bool -> 0/1, null -> "NULL".
func renderVal(v Value) string {
	switch v.Kind {
	case KindNull:
		return "NULL"
	case KindInt:
		return strconv.FormatInt(v.Int(), 10)
	case KindBool:
		if v.Bool() {
			return "1"
		}
		return "0"
	case KindString:
		return v.Str()
	case KindBytes:
		return hex.EncodeToString(v.Bytes())
	case KindUUID:
		u := v.UUID()
		return hex.EncodeToString(u[:])
	}
	return "?"
}

func renderAny(x any) string {
	switch t := x.(type) {
	case nil:
		return "NULL"
	case int64:
		return strconv.FormatInt(t, 10)
	case string:
		return t
	case []byte:
		return hex.EncodeToString(t)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	}
	return "?"
}

func engineRows(t *testing.T, db *DB, sqlText string) map[string]string {
	t.Helper()
	_, rows, err := db.Query(sqlText)
	if err != nil {
		t.Fatalf("engine query: %v", err)
	}
	out := map[string]string{}
	for _, r := range rows {
		parts := make([]string, len(r))
		for i, v := range r {
			parts[i] = renderVal(v)
		}
		out[parts[0]] = strings.Join(parts, "|")
	}
	return out
}

func sqliteRows(t *testing.T, sdb *sql.DB, sqlText string) map[string]string {
	t.Helper()
	rows, err := sdb.Query(sqlText)
	if err != nil {
		t.Fatalf("sqlite query: %v", err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	out := map[string]string{}
	for rows.Next() {
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("sqlite scan: %v", err)
		}
		parts := make([]string, len(cols))
		for i, v := range raw {
			parts[i] = renderAny(v)
		}
		out[parts[0]] = strings.Join(parts, "|")
	}
	return out
}

func compareMaps(t *testing.T, label string, want, got map[string]string) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("%s: row count engine=%d mirror=%d", label, len(want), len(got))
	}
	for k, wv := range want {
		gv, ok := got[k]
		if !ok {
			t.Fatalf("%s: pk %s present in engine, missing in mirror", label, k)
		}
		if wv != gv {
			t.Fatalf("%s: pk %s mismatch\n engine=%s\n mirror=%s", label, k, wv, gv)
		}
	}
}

// The drained SQLite mirror must reproduce the engine's current state exactly:
// bootstrap-table inserts/updates/deletes AND a runtime-created table.
func TestDrainMirrorMatchesEngine(t *testing.T) {
	dir := t.TempDir()
	sqPath := filepath.Join(t.TempDir(), "mirror.db")
	db, err := Open(Options{
		Schema:        testSchema(),
		WALPath:       dir,
		CompanionPath: sqPath,
		drainInterval: -1, // no background loop; sealed + drained manually below
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 1; i <= 50; i++ {
		if _, err := db.Exec("INSERT INTO users (id, name, age, active) VALUES (?, ?, ?, ?)",
			tid(i), "user"+strconv.Itoa(i), i, i%2 == 0); err != nil {
			t.Fatal(err)
		}
	}
	db.Exec("UPDATE users SET age = ? WHERE id = ?", 999, tid(10))
	db.Exec("UPDATE users SET name = ? WHERE id = ?", "renamed", tid(20))
	db.Exec("DELETE FROM users WHERE id = ?", tid(30))

	// runtime CREATE TABLE → exercises the recCreateTable drain path
	if _, err := db.Exec("CREATE TABLE logs (id uuid primary key, msg text)"); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if _, err := db.Exec("INSERT INTO logs (id, msg) VALUES (?, ?)", tid(1000+i), "m"+strconv.Itoa(i)); err != nil {
			t.Fatal(err)
		}
	}

	if err := db.wal.flush(); err != nil { // seal all writes
		t.Fatal(err)
	}
	if err := db.drainOnce(); err != nil {
		t.Fatalf("drain: %v", err)
	}

	eng := engineRows(t, db, "SELECT id, name, age, active FROM users")
	if len(eng) != 49 { // 50 inserted - 1 deleted
		t.Fatalf("engine users = %d, want 49", len(eng))
	}
	compareMaps(t, "users", eng, sqliteRows(t, db.sq.sdb, `SELECT "id","name","age","active" FROM "users"`))

	eng2 := engineRows(t, db, "SELECT id, msg FROM logs")
	if len(eng2) != 5 {
		t.Fatalf("engine logs = %d, want 5", len(eng2))
	}
	compareMaps(t, "logs", eng2, sqliteRows(t, db.sq.sdb, `SELECT "id","msg" FROM "logs"`))
}

// The drain validates WAL values against the schema before mirroring, exactly as
// the in-memory replay does: a wrong-typed CRC-valid record (a UUID in the INT
// column) is rejected as ErrWALCorrupt and the cursor does not advance, so the
// corruption is never committed to the dynamically-typed companion base.
func TestDrainRejectsWrongTypedValue(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Schema: testSchema(), WALPath: dir, walFlushInterval: time.Hour, drainInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rt := db.cat.Load().byName["users"]
	// age (TypeInt) receives a UUID — a record the validating write path would
	// never produce, injected straight into the WAL.
	bad := encodeInsertMutation(nil, rt.tableID, Row{UUIDVal(tid(1)), Str("alice"), UUIDVal(tid(9)), Bool(true)})
	if err := db.wal.writeRecord(recMutation, bad); err != nil {
		t.Fatal(err)
	}
	if err := db.wal.flush(); err != nil { // seal the segment so the drain sees it
		t.Fatal(err)
	}
	if err := db.drainOnce(); !errors.Is(err, ErrWALCorrupt) {
		t.Fatalf("drainOnce on a wrong-typed record: got %v, want ErrWALCorrupt", err)
	}
	if db.sq.lastDrained != 0 {
		t.Fatalf("cursor advanced past corruption: lastDrained = %d, want 0", db.sq.lastDrained)
	}
}

// The mirror runs synchronous=FULL: the drain reclaims a hazedb WAL segment only
// after its SQLite commit, so that commit must be power-loss durable, or a reclaim
// could lose data the WAL had already fsynced (durability.md §5/§6). NORMAL in WAL
// mode does not fsync per commit, so it would break that guarantee.
func TestMirrorSynchronousFull(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Schema: testSchema(), WALPath: dir, walFlushInterval: time.Hour, drainInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var sync int
	if err := db.sq.sdb.QueryRow("PRAGMA synchronous").Scan(&sync); err != nil {
		t.Fatal(err)
	}
	if sync != 2 { // 0=OFF 1=NORMAL 2=FULL 3=EXTRA
		t.Fatalf("mirror PRAGMA synchronous = %d, want 2 (FULL)", sync)
	}
}
