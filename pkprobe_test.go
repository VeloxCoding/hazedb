package hazedb

import "testing"

// pkProbe pins the PK fast path for a WHERE that constrains the PK by equality
// inside an AND-chain (WHERE id = ? AND age = ?) but is not a bare PK equality.
// These tests cover two things: the planner picks pkProbe only in the right gap,
// and every executor re-checks the residual conjunct (a row matched by PK but not
// by the rest of the WHERE must NOT be returned/updated/deleted).

func TestPlanPKProbeSelection(t *testing.T) {
	db := openEmpty(t)
	mustExec := func(sql string) {
		t.Helper()
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	mustExec("CREATE TABLE users (id uuid primary key, name text, age int, active bool null)")
	mustExec("CREATE TABLE messages (id uuid primary key, thread uuid partition key, body text, n int)")
	mustExec("CREATE TABLE accounts (id uuid primary key, email text, age int, INDEX (email))")

	planOf := func(sql string) *plan {
		t.Helper()
		pl, err := db.prepare(sql, db.cat.Load())
		if err != nil {
			t.Fatalf("prepare %q: %v", sql, err)
		}
		return pl
	}

	// The gap: PK pinned inside an AND-chain on a non-indexed extra column → pkProbe.
	if pl := planOf("SELECT name FROM users WHERE id = ? AND age = ?"); pl.pkProbe == nil || pl.pkLookup || pl.idxLookup {
		t.Fatalf("id=? AND age=? : pkProbe=%v pkLookup=%v idxLookup=%v, want set/false/false", pl.pkProbe != nil, pl.pkLookup, pl.idxLookup)
	}
	// Bare PK equality stays on the dedicated bare path, NOT pkProbe.
	if pl := planOf("SELECT name FROM users WHERE id = ?"); !pl.pkLookup || pl.pkProbe != nil {
		t.Fatalf("id=? : pkLookup=%v pkProbe=%v, want true/nil", pl.pkLookup, pl.pkProbe != nil)
	}
	// No PK constraint → neither fast path (full scan).
	if pl := planOf("SELECT name FROM users WHERE age = ?"); pl.pkProbe != nil || pl.pkLookup {
		t.Fatalf("age=? : pkProbe=%v pkLookup=%v, want nil/false", pl.pkProbe != nil, pl.pkLookup)
	}
	// An indexed extra column already avoids the scan → index wins, no pkProbe.
	if pl := planOf("SELECT age FROM accounts WHERE id = ? AND email = ?"); !pl.idxLookup || pl.pkProbe != nil {
		t.Fatalf("id=? AND email=? : idxLookup=%v pkProbe=%v, want true/nil", pl.idxLookup, pl.pkProbe != nil)
	}
	// Partitioned tables are gated off (the write candidate paths are not pkDir-aware).
	if pl := planOf("SELECT body FROM messages WHERE id = ? AND n = ?"); pl.pkProbe != nil {
		t.Fatalf("partitioned id=? AND n=? : pkProbe set, want nil")
	}
	// Writes take pkProbe on the same gap.
	if pl := planOf("UPDATE users SET name = ? WHERE id = ? AND age = ?"); pl.pkProbe == nil || pl.pkLookup {
		t.Fatalf("UPDATE id=? AND age=? : pkProbe=%v pkLookup=%v, want set/false", pl.pkProbe != nil, pl.pkLookup)
	}
	if pl := planOf("DELETE FROM users WHERE id = ? AND age = ?"); pl.pkProbe == nil || pl.pkLookup {
		t.Fatalf("DELETE id=? AND age=? : pkProbe=%v pkLookup=%v, want set/false", pl.pkProbe != nil, pl.pkLookup)
	}
}

// The headline correctness property: the PK probe honours the residual conjunct.
// A row found by PK but failing the rest of the WHERE must be invisible to reads
// and untouched by writes — the bug a naive pkLookup-for-everything would cause.
func TestPKProbeResidualHonoured(t *testing.T) {
	db := openMem(t) // users(id, name, age, active)
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "bob", 40); err != nil {
		t.Fatal(err)
	}

	// SELECT: residual matches → row; residual fails → empty (NOT alice ignoring age).
	if _, rows, err := db.Query("SELECT name FROM users WHERE id = ? AND age = ?", tid(1), 30); err != nil || len(rows) != 1 || rows[0][0].Str() != "alice" {
		t.Fatalf("matching residual: rows=%v err=%v, want [alice]", rows, err)
	}
	if _, rows, err := db.Query("SELECT name FROM users WHERE id = ? AND age = ?", tid(1), 99); err != nil || len(rows) != 0 {
		t.Fatalf("failing residual: rows=%v err=%v, want []", rows, err)
	}
	// QueryRow path.
	if _, r, err := db.QueryRow("SELECT name FROM users WHERE id = ? AND age = ?", tid(1), 99); err != nil || r != nil {
		t.Fatalf("QueryRow failing residual: row=%v err=%v, want nil", r, err)
	}
	// QueryEach (streaming) path.
	seen := 0
	if err := db.QueryEach("SELECT name FROM users WHERE id = ? AND age = ?",
		[]Value{UUIDVal(tid(1)), Int(99)}, func([]string, Row) bool { seen++; return true }); err != nil {
		t.Fatal(err)
	}
	if seen != 0 {
		t.Fatalf("QueryEach failing residual visited %d rows, want 0", seen)
	}

	// UPDATE: residual fails → no-op, row unchanged.
	if n, err := db.Exec("UPDATE users SET name = ? WHERE id = ? AND age = ?", "X", tid(1), 99); err != nil || n != 0 {
		t.Fatalf("UPDATE failing residual: n=%d err=%v, want 0", n, err)
	}
	if _, r, _ := db.QueryRow("SELECT name FROM users WHERE id = ?", tid(1)); r == nil || r[0].Str() != "alice" {
		t.Fatalf("row mutated by a non-matching UPDATE: %v", r)
	}
	// UPDATE: residual matches → applies. Multi-column SET exercises updateByCandidates.
	if n, err := db.Exec("UPDATE users SET name = ?, active = ? WHERE id = ? AND age = ?", "ALICE", true, tid(1), 30); err != nil || n != 1 {
		t.Fatalf("UPDATE matching residual: n=%d err=%v, want 1", n, err)
	}
	if _, r, _ := db.QueryRow("SELECT name, active FROM users WHERE id = ?", tid(1)); r == nil || r[0].Str() != "ALICE" || !r[1].Bool() {
		t.Fatalf("matching UPDATE did not apply: %v", r)
	}

	// DELETE: residual fails → no-op; residual matches → removes.
	if n, err := db.Exec("DELETE FROM users WHERE id = ? AND age = ?", tid(2), 99); err != nil || n != 0 {
		t.Fatalf("DELETE failing residual: n=%d err=%v, want 0", n, err)
	}
	if _, r, _ := db.QueryRow("SELECT name FROM users WHERE id = ?", tid(2)); r == nil {
		t.Fatalf("row deleted by a non-matching DELETE")
	}
	if n, err := db.Exec("DELETE FROM users WHERE id = ? AND age = ?", tid(2), 40); err != nil || n != 1 {
		t.Fatalf("DELETE matching residual: n=%d err=%v, want 1", n, err)
	}
	if _, r, _ := db.QueryRow("SELECT name FROM users WHERE id = ?", tid(2)); r != nil {
		t.Fatalf("matching DELETE did not remove the row: %v", r)
	}
}

// The probe must return the same rows the scan fallback would, so a plan that
// happens to pick pkProbe is verified against an equivalent unindexed-filter scan.
func TestPKProbeMatchesScan(t *testing.T) {
	db := openMem(t)
	for i := 1; i <= 5; i++ {
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "u", i*10); err != nil {
			t.Fatal(err)
		}
	}
	// Probe (id pins the PK) vs scan (age = ? AND name = ?, no PK) over the same data.
	_, probe, err := db.Query("SELECT name, age FROM users WHERE id = ? AND age = ?", tid(3), 30)
	if err != nil {
		t.Fatal(err)
	}
	_, scan, err := db.Query("SELECT name, age FROM users WHERE age = ? AND name = ?", 30, "u")
	if err != nil {
		t.Fatal(err)
	}
	if len(probe) != 1 || len(scan) != 1 || probe[0][1].Int() != scan[0][1].Int() {
		t.Fatalf("probe %v != scan %v", probe, scan)
	}
}
