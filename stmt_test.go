package hazedb

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

// A prepared SELECT must return the same rows as the bare DB path, and a
// prepared write must apply identically.
func TestStmtParityWithDB(t *testing.T) {
	db := openMem(t)
	for i := 0; i < 5; i++ {
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
			tid(i), fmt.Sprintf("u%d", i), i*10); err != nil {
			t.Fatal(err)
		}
	}
	sel, err := db.Prepare("SELECT name, age FROM users WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		_, want, _ := db.QueryRow("SELECT name, age FROM users WHERE id = ?", tid(i))
		_, got, _ := sel.QueryRow(tid(i))
		if got == nil || got[0].Str() != want[0].Str() || got[1].Int() != want[1].Int() {
			t.Fatalf("row %d: stmt %v != db %v", i, got, want)
		}
	}
	// Prepared UPDATE applies like DB.Exec.
	upd, err := db.Prepare("UPDATE users SET age = ? WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := upd.Exec(99, tid(2)); err != nil || n != 1 {
		t.Fatalf("prepared update: n=%d err=%v", n, err)
	}
	if _, r, _ := sel.QueryRow(tid(2)); r == nil || r[1].Int() != 99 {
		t.Fatalf("prepared update not visible: %v", r)
	}
}

// A handle must survive DDL: after a DROP it re-binds to a clean ErrUnknownTable,
// and after the table is re-created it works again — never pointing at stale
// storage.
func TestStmtRebindsAfterDDL(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE t (id uuid primary key, n int)")
	id := tid(1)
	db.Exec("INSERT INTO t (id, n) VALUES (?, ?)", id, 7)

	st, err := db.Prepare("SELECT n FROM t WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	if _, r, _ := st.QueryRow(id); r == nil || r[0].Int() != 7 {
		t.Fatalf("pre-DROP: %v", r)
	}
	db.Exec("DROP TABLE t")
	if _, _, err := st.QueryRow(id); !errors.Is(err, ErrUnknownTable) {
		t.Fatalf("after DROP: want ErrUnknownTable, got %v", err)
	}
	db.Exec("CREATE TABLE t (id uuid primary key, n int)")
	db.Exec("INSERT INTO t (id, n) VALUES (?, ?)", id, 42)
	if _, r, err := st.QueryRow(id); err != nil || r == nil || r[0].Int() != 42 {
		t.Fatalf("after re-CREATE: row=%v err=%v", r, err)
	}
}

// QueryRowByPK matches Query, handles not-found, rejects a non-PK-pinned plan,
// and — the headline — allocates nothing for a byte-free projection.
func TestStmtQueryRowByPK(t *testing.T) {
	db := openMem(t)
	id := tid(1)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", id, "alice", 30)
	st, err := db.Prepare("SELECT name, age FROM users WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}

	out, found, err := st.QueryRowByPK(id, make([]Value, 0, 4))
	if err != nil || !found || out[0].Str() != "alice" || out[1].Int() != 30 {
		t.Fatalf("got out=%v found=%v err=%v", out, found, err)
	}
	if _, found, _ := st.QueryRowByPK(tid(999), out); found {
		t.Fatal("expected not-found for absent PK")
	}

	// Non-PK-pinned SELECT is a misuse → error, not a wrong answer.
	noPK, _ := db.Prepare("SELECT name FROM users")
	if _, _, err := noPK.QueryRowByPK(id, nil); err == nil {
		t.Fatal("QueryRowByPK on non-PK-pinned SELECT: want error")
	}

	// Zero allocations when reusing the buffer (name+age carry no BYTES cells).
	avg := testing.AllocsPerRun(200, func() { out, _, _ = st.QueryRowByPK(id, out) })
	if avg != 0 {
		t.Fatalf("QueryRowByPK allocated %v/op, want 0", avg)
	}
}

// QueryRowByIndex matches Query, finds a not-yet-merged row via the dirty
// overlay, rejects a non-indexed plan, and allocates only the index-bucket copy.
func TestStmtQueryRowByIndex(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, name text, age int, INDEX (name))")
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "bob", 25)
	db.mergeIndexes()
	st, err := db.Prepare("SELECT id, age FROM users WHERE name = ? LIMIT 1")
	if err != nil {
		t.Fatal(err)
	}

	out, found, err := st.QueryRowByIndex(Str("alice"), make([]Value, 0, 4))
	if err != nil || !found || out[1].Int() != 30 {
		t.Fatalf("got out=%v found=%v err=%v", out, found, err)
	}
	if _, found, _ := st.QueryRowByIndex(Str("nobody"), out); found {
		t.Fatal("expected not-found for absent value")
	}

	// A row only in the dirty overlay (not yet merged) must still be found.
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(3), "carol", 40)
	if out, found, _ = st.QueryRowByIndex(Str("carol"), out); !found || out[1].Int() != 40 {
		t.Fatalf("dirty-overlay fetch: out=%v found=%v", out, found)
	}

	// Non-indexed WHERE is a misuse → error, not a wrong answer.
	noIdx, _ := db.Prepare("SELECT name FROM users WHERE age = ?")
	if _, _, err := noIdx.QueryRowByIndex(Int(30), nil); err == nil {
		t.Fatal("QueryRowByIndex on non-indexed SELECT: want error")
	}

	// LIMIT 1 is mandatory — it keeps the single-row intent explicit. A statement
	// without it is a misuse → error.
	noLimit, _ := db.Prepare("SELECT id, age FROM users WHERE name = ?")
	if _, _, err := noLimit.QueryRowByIndex(Str("alice"), nil); err == nil {
		t.Fatal("QueryRowByIndex without LIMIT 1: want error")
	}

	// ORDER BY is rejected — there is no ordering on this path (use the ordered
	// walk for newest/highest), so a misuse must error rather than mislead.
	ordered, _ := db.Prepare("SELECT id, age FROM users WHERE name = ? ORDER BY age LIMIT 1")
	if _, _, err := ordered.QueryRowByIndex(Str("alice"), nil); err == nil {
		t.Fatal("QueryRowByIndex with ORDER BY: want error")
	}

	// Near-zero: only the index-bucket copy for a byte-free projection.
	db.mergeIndexes() // drain the overlay so steady state holds
	avg := testing.AllocsPerRun(200, func() { out, _, _ = st.QueryRowByIndex(Str("alice"), out) })
	if avg > 1 {
		t.Fatalf("QueryRowByIndex allocated %v/op, want <= 1", avg)
	}
}

// BYTES cells must be cloned out, so mutating a returned slice can't corrupt
// stored data.
func TestStmtQueryRowByPKClonesBytes(t *testing.T) {
	db := openMem(t)
	db.Exec("CREATE TABLE blobs (id uuid primary key, data bytes)")
	id := tid(1)
	db.Exec("INSERT INTO blobs (id, data) VALUES (?, ?)", id, []byte{1, 2, 3})
	st, err := db.Prepare("SELECT data FROM blobs WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	out, found, err := st.QueryRowByPK(id, nil)
	if err != nil || !found || !bytes.Equal(out[0].Bytes(), []byte{1, 2, 3}) {
		t.Fatalf("got out=%v found=%v err=%v", out, found, err)
	}
	out[0].Bytes()[0] = 99 // mutate the returned slice
	out2, _, _ := st.QueryRowByPK(id, nil)
	if !bytes.Equal(out2[0].Bytes(), []byte{1, 2, 3}) {
		t.Fatalf("byte alias leak into storage: %v", out2[0].Bytes())
	}
}

// Prepared handle, ...any args — measures the statement-cache-hash saving.
func BenchmarkStmtQueryRow(b *testing.B) {
	db := newBenchDB(b, 10000)
	st, _ := db.Prepare("SELECT name, age FROM users WHERE id = ?")
	b.ResetTimer()
	b.ReportAllocs()
	var sink int64
	for i := 0; i < b.N; i++ {
		if _, row, _ := st.QueryRow(tid(i % 10000)); row != nil {
			sink += row[1].Int()
		}
	}
	_ = sink
}

// Typed scan-into fast path — should report 0 allocs/op.
func BenchmarkStmtQueryRowByPK(b *testing.B) {
	db := newBenchDB(b, 10000)
	st, _ := db.Prepare("SELECT name, age FROM users WHERE id = ?")
	out := make([]Value, 0, 4)
	b.ResetTimer()
	b.ReportAllocs()
	var sink int64
	for i := 0; i < b.N; i++ {
		var found bool
		if out, found, _ = st.QueryRowByPK(tid(i%10000), out); found {
			sink += out[1].Int()
		}
	}
	_ = sink
}
