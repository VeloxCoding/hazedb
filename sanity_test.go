package hazedb

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
)

// TestBigSanity is a single end-to-end sweep over the whole public path:
// runtime CREATE TABLE, INSERT/UPDATE/DELETE, every query shape, transactions,
// the error envelope, and WAL restart — plus white-box checks that the planner
// compiles each SQL string into the right execution plan (PK fast-path vs
// partition scan vs full scan, correct projection ordinals/names). It is the
// "does what I put in come back out exactly, and is the query routed right?"
// report; failures are localised by subtest. One WAL-backed DB is shared so the
// final restart subtest can prove the surviving state replays exactly.
//
// Reserved id ranges keep subtests from colliding: tid(1xxx) durable users,
// tid(2xxx) delete, tid(3xxx) messages/thread A, tid(4xxx) messages/thread B,
// tid(5xxx) accounts.
func TestBigSanity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sanity.wal")

	db, err := Open(Options{Schema: Schema{}, WALLevel: WALPeriodic, WALPath: path})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := db.Exec(sql, args...); err != nil {
			t.Fatalf("Exec %q: %v", sql, err)
		}
	}

	// --- Runtime DDL + plan compilation ------------------------------------
	// Two tables created at runtime: a flat one and a partitioned one covering
	// every column type. Each CREATE must bump the catalog version, and SELECTs
	// must compile to the plan the WHERE clause implies.
	t.Run("ddl_and_plan_compilation", func(t *testing.T) {
		v0 := db.cat.Load().version
		mustExec("CREATE TABLE users (id uuid primary key, name text, age int, active bool null)")
		mustExec("CREATE TABLE messages (id uuid primary key, thread uuid partition key, " +
			"body text, n int, data bytes, flag bool, note text null)")
		v1 := db.cat.Load().version
		if v1 <= v0 {
			t.Fatalf("catalog version did not advance across two CREATEs: %d -> %d", v0, v1)
		}
		if _, err := db.Exec("CREATE TABLE users (id uuid primary key)"); !errors.Is(err, ErrTableExists) {
			t.Fatalf("re-CREATE: want ErrTableExists, got %v", err)
		}

		planOf := func(sql string) *plan {
			t.Helper()
			pl, err := db.prepare(sql, db.cat.Load())
			if err != nil {
				t.Fatalf("prepare %q: %v", sql, err)
			}
			return pl
		}
		eqStrs := func(got, want []string) bool {
			if len(got) != len(want) {
				return false
			}
			for i := range got {
				if got[i] != want[i] {
					return false
				}
			}
			return true
		}

		// WHERE id = ? on either table → PK fast path, never a partition scan.
		if pl := planOf("SELECT body FROM messages WHERE id = ?"); !pl.pkLookup || pl.partLookup {
			t.Fatalf("WHERE id=? : pkLookup=%v partLookup=%v, want true/false", pl.pkLookup, pl.partLookup)
		} else if !eqStrs(pl.colNames, []string{"body"}) {
			t.Fatalf("projection names = %v, want [body]", pl.colNames)
		}
		// WHERE partitionkey = ? → indexed partition scan, not a PK lookup.
		if pl := planOf("SELECT body, n FROM messages WHERE thread = ?"); !pl.partLookup || pl.pkLookup {
			t.Fatalf("WHERE thread=? : partLookup=%v pkLookup=%v, want true/false", pl.partLookup, pl.pkLookup)
		} else if !eqStrs(pl.colNames, []string{"body", "n"}) {
			t.Fatalf("projection names = %v, want [body n]", pl.colNames)
		}
		// WHERE on an unindexed column → neither fast path (full scan).
		if pl := planOf("SELECT body FROM messages WHERE n = ?"); pl.pkLookup || pl.partLookup {
			t.Fatalf("WHERE n=? : pkLookup=%v partLookup=%v, want false/false", pl.pkLookup, pl.partLookup)
		}
		// ORDER BY resolves the order column to an ordinal.
		if pl := planOf("SELECT n FROM messages WHERE thread = ? ORDER BY n LIMIT 5"); pl.orderOrdinal < 0 {
			t.Fatalf("ORDER BY n : orderOrdinal=%d, want >=0", pl.orderOrdinal)
		}
	})

	// --- INSERT + exact round-trip across every value kind -----------------
	// Insert one fully-populated row and read each field back through its typed
	// accessor: int, text, bytes, bool, uuid (the PK), and a NULL.
	durableID := tid(1000)
	t.Run("insert_roundtrip_all_types", func(t *testing.T) {
		mustExec("INSERT INTO users (id, name, age, active) VALUES (?, ?, ?, ?)",
			durableID, "durable", 42, true)

		_, row, err := db.QueryRow("SELECT id, name, age, active FROM users WHERE id = ?", durableID)
		if err != nil {
			t.Fatalf("QueryRow: %v", err)
		}
		if row == nil {
			t.Fatal("inserted row not found")
		}
		if row[0].UUID() != durableID {
			t.Errorf("id round-trip: got %v want %v", row[0].UUID(), durableID)
		}
		if row[1].Str() != "durable" {
			t.Errorf("name round-trip: got %q want %q", row[1].Str(), "durable")
		}
		if row[2].Int() != 42 {
			t.Errorf("age round-trip: got %d want 42", row[2].Int())
		}
		if !row[3].Bool() {
			t.Errorf("active round-trip: got false want true")
		}

		// NULL into a nullable column round-trips as IsNull.
		nullID := tid(1001)
		mustExec("INSERT INTO users (id, name, age, active) VALUES (?, ?, ?, ?)",
			nullID, "no-flag", 7, nil)
		_, nrow, err := db.QueryRow("SELECT active FROM users WHERE id = ?", nullID)
		if err != nil || nrow == nil {
			t.Fatalf("QueryRow null: row=%v err=%v", nrow, err)
		}
		if !nrow[0].IsNull() {
			t.Errorf("NULL round-trip: got %v, want null", nrow[0])
		}

		// Bytes + bool through the partitioned table.
		mid := tid(3000)
		want := []byte{0x00, 0x01, 0xff, 0x7f, 0x80}
		mustExec("INSERT INTO messages (id, thread, body, n, data, flag, note) VALUES (?, ?, ?, ?, ?, ?, ?)",
			mid, tid(3001), "hello", 5, want, false, nil)
		_, mrow, err := db.QueryRow("SELECT data, flag FROM messages WHERE id = ?", mid)
		if err != nil || mrow == nil {
			t.Fatalf("QueryRow bytes: row=%v err=%v", mrow, err)
		}
		if !bytes.Equal(mrow[0].Bytes(), want) {
			t.Errorf("bytes round-trip: got %v want %v", mrow[0].Bytes(), want)
		}
		if mrow[0].Kind != KindBytes {
			t.Errorf("bytes kind: got %v want KindBytes", mrow[0].Kind)
		}
		if mrow[1].Bool() {
			t.Errorf("flag round-trip: got true want false")
		}
	})

	// --- Byte-boundary isolation -------------------------------------------
	// Storage must not alias the caller's slice on the way in, and returned
	// rows must not alias storage on the way out. Mutating either side must
	// leave the stored bytes untouched.
	t.Run("byte_boundary_isolation", func(t *testing.T) {
		id := tid(3100)
		in := []byte{1, 2, 3}
		mustExec("INSERT INTO messages (id, thread, body, n, data, flag, note) VALUES (?, ?, ?, ?, ?, ?, ?)",
			id, tid(3101), "b", 0, in, false, nil)
		in[0] = 99 // mutate caller slice after the write

		_, r1, _ := db.QueryRow("SELECT data FROM messages WHERE id = ?", id)
		if !bytes.Equal(r1[0].Bytes(), []byte{1, 2, 3}) {
			t.Fatalf("write boundary aliased caller slice: stored %v", r1[0].Bytes())
		}
		r1[0].Bytes()[0] = 88 // mutate the returned slice

		_, r2, _ := db.QueryRow("SELECT data FROM messages WHERE id = ?", id)
		if !bytes.Equal(r2[0].Bytes(), []byte{1, 2, 3}) {
			t.Fatalf("read boundary aliased storage: stored %v", r2[0].Bytes())
		}
	})

	// --- UPDATE ------------------------------------------------------------
	t.Run("update", func(t *testing.T) {
		n, err := db.Exec("UPDATE users SET name = ?, age = ? WHERE id = ?", "renamed", 43, durableID)
		if err != nil || n != 1 {
			t.Fatalf("UPDATE: n=%d err=%v", n, err)
		}
		_, row, _ := db.QueryRow("SELECT name, age FROM users WHERE id = ?", durableID)
		if row[0].Str() != "renamed" || row[1].Int() != 43 {
			t.Fatalf("after UPDATE: %v", row)
		}
		// Arithmetic SET referencing the column itself.
		if _, err := db.Exec("UPDATE users SET age = age + ? WHERE id = ?", 10, durableID); err != nil {
			t.Fatalf("arith UPDATE: %v", err)
		}
		_, row, _ = db.QueryRow("SELECT age FROM users WHERE id = ?", durableID)
		if row[0].Int() != 53 {
			t.Fatalf("after arith UPDATE: age=%d want 53", row[0].Int())
		}
		// UPDATE on the PK column is rejected at plan time.
		if _, err := db.Exec("UPDATE users SET id = ? WHERE id = ?", tid(1), durableID); !errors.Is(err, ErrPKUpdate) {
			t.Fatalf("UPDATE PK: want ErrPKUpdate, got %v", err)
		}
	})

	// --- DELETE ------------------------------------------------------------
	t.Run("delete", func(t *testing.T) {
		gone := tid(2000)
		mustExec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", gone, "tmp", 1)
		n, err := db.Exec("DELETE FROM users WHERE id = ?", gone)
		if err != nil || n != 1 {
			t.Fatalf("DELETE: n=%d err=%v", n, err)
		}
		if _, row, _ := db.QueryRow("SELECT name FROM users WHERE id = ?", gone); row != nil {
			t.Fatalf("row still present after DELETE: %v", row)
		}
		// Re-inserting the freed PK works.
		mustExec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", gone, "reborn", 2)
		if _, row, _ := db.QueryRow("SELECT name FROM users WHERE id = ?", gone); row == nil || row[0].Str() != "reborn" {
			t.Fatalf("re-insert after DELETE failed: %v", row)
		}
		// Clean up so the restart subtest can assert this PK is absent.
		mustExec("DELETE FROM users WHERE id = ?", gone)
	})

	// --- Scans and query paths ---------------------------------------------
	// Two partitions; verify partition isolation, LIMIT bounds, and ORDER BY.
	t.Run("scans_and_query_paths", func(t *testing.T) {
		threadA, threadB := tid(3500), tid(4500)
		for i := 0; i < 6; i++ {
			mustExec("INSERT INTO messages (id, thread, body, n, data, flag, note) VALUES (?, ?, ?, ?, ?, ?, ?)",
				tid(3500+i+1), threadA, "a", i, []byte{byte(i)}, false, nil)
		}
		for i := 0; i < 4; i++ {
			mustExec("INSERT INTO messages (id, thread, body, n, data, flag, note) VALUES (?, ?, ?, ?, ?, ?, ?)",
				tid(4500+i+1), threadB, "b", i, []byte{byte(i)}, false, nil)
		}
		// Partition scan returns only its own thread's rows.
		_, aRows, err := db.Query("SELECT body FROM messages WHERE thread = ?", threadA)
		if err != nil || len(aRows) != 6 {
			t.Fatalf("partition A scan: %d rows err=%v (want 6)", len(aRows), err)
		}
		for _, r := range aRows {
			if r[0].Str() != "a" {
				t.Fatalf("partition leak: got body %q in thread A", r[0].Str())
			}
		}
		// LIMIT without ORDER BY bounds the result.
		_, lim, err := db.Query("SELECT n FROM messages WHERE thread = ? LIMIT 3", threadA)
		if err != nil || len(lim) != 3 {
			t.Fatalf("LIMIT 3: %d rows err=%v", len(lim), err)
		}
		// ORDER BY n DESC LIMIT 2 → the two largest n (5, 4).
		_, top, err := db.Query("SELECT n FROM messages WHERE thread = ? ORDER BY n DESC LIMIT 2", threadA)
		if err != nil || len(top) != 2 || top[0][0].Int() != 5 || top[1][0].Int() != 4 {
			t.Fatalf("ORDER BY n DESC LIMIT 2: %v err=%v", top, err)
		}
	})

	// --- Transactions ------------------------------------------------------
	// Commit applies the whole closure; an error rolls all of it back.
	t.Run("transactions", func(t *testing.T) {
		mustExec("CREATE TABLE accounts (id uuid primary key, balance int)")
		from, to := tid(5001), tid(5002)
		mustExec("INSERT INTO accounts (id, balance) VALUES (?, ?)", from, 100)
		mustExec("INSERT INTO accounts (id, balance) VALUES (?, ?)", to, 0)

		if err := db.Transaction(func(tx *Tx) error {
			if _, err := tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = ?", 30, from); err != nil {
				return err
			}
			_, err := tx.Exec("UPDATE accounts SET balance = balance + ? WHERE id = ?", 30, to)
			return err
		}); err != nil {
			t.Fatalf("commit: %v", err)
		}
		assertBal := func(id UUID, want int64) {
			t.Helper()
			_, row, _ := db.QueryRow("SELECT balance FROM accounts WHERE id = ?", id)
			if row == nil || row[0].Int() != want {
				t.Fatalf("balance %v = %v, want %d", id, row, want)
			}
		}
		assertBal(from, 70)
		assertBal(to, 30)

		// Rollback: returning an error must leave both balances unchanged.
		sentinel := errors.New("abort")
		if err := db.Transaction(func(tx *Tx) error {
			tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = ?", 50, from)
			tx.Exec("UPDATE accounts SET balance = balance + ? WHERE id = ?", 50, to)
			return sentinel
		}); err != sentinel {
			t.Fatalf("rollback: got err %v, want sentinel", err)
		}
		assertBal(from, 70)
		assertBal(to, 30)
	})

	// --- Error and validation envelope -------------------------------------
	t.Run("errors_and_validation", func(t *testing.T) {
		// Unsupported Go arg type → ErrTypeMismatch.
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(9001), "x", 3.14); !errors.Is(err, ErrTypeMismatch) {
			t.Fatalf("float arg: want ErrTypeMismatch, got %v", err)
		}
		// Wrong column type (string into INT) → an error (not a panic).
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(9002), "x", "not-an-int"); err == nil {
			t.Fatal("string into INT column: want error, got nil")
		}
		// NOT NULL violation.
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(9003), nil, 3); err == nil {
			t.Fatal("NULL into NOT NULL column: want error, got nil")
		}
		// Duplicate PK.
		dup := tid(9004)
		mustExec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", dup, "first", 1)
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", dup, "second", 2); !errors.Is(err, ErrDuplicatePK) {
			t.Fatalf("duplicate PK: want ErrDuplicatePK, got %v", err)
		}
		// Unknown table.
		if _, _, err := db.Query("SELECT x FROM nope WHERE id = ?", tid(1)); !errors.Is(err, ErrUnknownTable) {
			t.Fatalf("unknown table: want ErrUnknownTable, got %v", err)
		}
		// DROP then query → catalog version advances and the plan re-binds to a
		// clean ErrUnknownTable instead of stale storage.
		mustExec("CREATE TABLE temp (id uuid primary key, n int)")
		mustExec("INSERT INTO temp (id, n) VALUES (?, ?)", tid(9100), 1)
		if _, _, err := db.Query("SELECT n FROM temp WHERE id = ?", tid(9100)); err != nil {
			t.Fatalf("pre-DROP query: %v", err)
		}
		vBefore := db.cat.Load().version
		mustExec("DROP TABLE temp")
		if db.cat.Load().version <= vBefore {
			t.Fatalf("DROP did not advance catalog version")
		}
		if _, _, err := db.Query("SELECT n FROM temp WHERE id = ?", tid(9100)); !errors.Is(err, ErrUnknownTable) {
			t.Fatalf("post-DROP query: want ErrUnknownTable, got %v", err)
		}
	})

	// --- WAL restart durability --------------------------------------------
	// Close, reopen from the same WAL, and confirm the surviving state replays
	// exactly: the durable user (with its updated age), the committed transfer,
	// the deleted PK staying gone, and the dropped table staying dropped.
	t.Run("wal_restart_durability", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		db2, err := Open(Options{Schema: Schema{}, WALLevel: WALPeriodic, WALPath: path})
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer db2.Close()

		_, row, err := db2.QueryRow("SELECT name, age FROM users WHERE id = ?", durableID)
		if err != nil || row == nil {
			t.Fatalf("durable user lost: row=%v err=%v", row, err)
		}
		if row[0].Str() != "renamed" || row[1].Int() != 53 {
			t.Fatalf("durable user diverged after replay: %v", row)
		}
		_, bal, err := db2.QueryRow("SELECT balance FROM accounts WHERE id = ?", tid(5001))
		if err != nil || bal == nil || bal[0].Int() != 70 {
			t.Fatalf("committed transfer lost after replay: bal=%v err=%v", bal, err)
		}
		if _, gone, _ := db2.QueryRow("SELECT name FROM users WHERE id = ?", tid(2000)); gone != nil {
			t.Fatalf("deleted PK resurrected after replay: %v", gone)
		}
		if _, _, err := db2.Query("SELECT n FROM temp WHERE id = ?", tid(9100)); !errors.Is(err, ErrUnknownTable) {
			t.Fatalf("dropped table resurrected after replay: %v", err)
		}
	})
}
