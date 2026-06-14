package hazedb

import (
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3" // cgo driver: "sqlite3"
	_ "modernc.org/sqlite"          // pure-Go driver: "sqlite"
)

// These benchmarks compare hazedb's interpreter path to SQLite (via
// database/sql, both the cgo and pure-Go drivers). Same logical operation:
// INSERT one user, SELECT one user by PK, UPDATE by PK, DELETE by PK. Same row
// shape (id, name, age).
//
// Goal: honest interpreter-path numbers vs the store anyone would realistically
// reach for. The codegen target would shave parser+plan dispatch cost; these
// benchmarks describe today's path, not tomorrow's.
//
// Both stores use the same 16-byte UUID key (key16) so the comparison is fair
// on key width. Remaining caveats to read these numbers honestly:
//   - Reads are the cleanest comparison. For WRITES, hazedb-Mem is in-memory
//     only while SQLite syncs per its journal mode — different durability, so
//     write rows are not like-for-like.
//   - hazedb's interpreter path still carries per-row overhead that the
//     planned codegen step removes; treat hazedb's numbers as a floor.

const compareN = 10000

// key16 is the shared 16-byte UUID key used by ALL three stores, so the
// comparison is apples-to-apples on key width (hazedb's PK is a UUID).
func key16(i int) []byte {
	u := tid(i)
	k := make([]byte, 16)
	copy(k, u[:])
	return k
}

func setupSQLite(b *testing.B) (*sql.DB, func()) {
	b.Helper()
	dir := b.TempDir()
	dsn := filepath.Join(dir, "cmp.sqlite")
	d, err := sql.Open("sqlite3", dsn+"?_journal=WAL&_sync=NORMAL")
	if err != nil {
		b.Fatal(err)
	}
	if _, err := d.Exec("CREATE TABLE users (id BLOB PRIMARY KEY, name TEXT, age INTEGER)"); err != nil {
		b.Fatal(err)
	}
	stmt, err := d.Prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < compareN; i++ {
		if _, err := stmt.Exec(key16(i), "name", i%100); err != nil {
			b.Fatal(err)
		}
	}
	return d, func() { d.Close(); os.Remove(dsn) }
}

// setupSQLiteMem is the in-memory SQLite — RAM-vs-RAM with hazedb (no disk).
// MaxOpenConns(1) pins one connection so all calls hit the same in-memory DB
// (a fresh ":memory:" connection would otherwise be a separate empty DB).
func setupSQLiteMem(b *testing.B) (*sql.DB, func()) {
	b.Helper()
	d, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	d.SetMaxOpenConns(1)
	if _, err := d.Exec("CREATE TABLE users (id BLOB PRIMARY KEY, name TEXT, age INTEGER)"); err != nil {
		b.Fatal(err)
	}
	stmt, err := d.Prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < compareN; i++ {
		if _, err := stmt.Exec(key16(i), "name", i%100); err != nil {
			b.Fatal(err)
		}
	}
	return d, func() { d.Close() }
}

func BenchmarkInsert_SQLiteMem(b *testing.B) {
	d, cleanup := setupSQLiteMem(b)
	defer cleanup()
	stmt, _ := d.Prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
	defer stmt.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stmt.Exec(key16(compareN+i), "name", i%100)
	}
}

func BenchmarkSelectByPK_SQLiteMem(b *testing.B) {
	d, cleanup := setupSQLiteMem(b)
	defer cleanup()
	stmt, _ := d.Prepare("SELECT name, age FROM users WHERE id = ?")
	defer stmt.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var name string
		var age int64
		stmt.QueryRow(key16(i%compareN)).Scan(&name, &age)
	}
}

func BenchmarkUpdateByPK_SQLiteMem(b *testing.B) {
	d, cleanup := setupSQLiteMem(b)
	defer cleanup()
	stmt, _ := d.Prepare("UPDATE users SET age = ? WHERE id = ?")
	defer stmt.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stmt.Exec((i%100)+1, key16(i%compareN))
	}
}

// setupSQLitePureMem is in-memory SQLite via the PURE-GO driver
// (modernc.org/sqlite, registered as "sqlite") — same database/sql layer as
// the cgo build, but no cgo boundary. The gap between this and the cgo
// :memory: benchmark isolates the cost of the cgo crossing itself.
func setupSQLitePureMem(b *testing.B) (*sql.DB, func()) {
	b.Helper()
	d, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	d.SetMaxOpenConns(1)
	if _, err := d.Exec("CREATE TABLE users (id BLOB PRIMARY KEY, name TEXT, age INTEGER)"); err != nil {
		b.Fatal(err)
	}
	stmt, err := d.Prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < compareN; i++ {
		if _, err := stmt.Exec(key16(i), "name", i%100); err != nil {
			b.Fatal(err)
		}
	}
	return d, func() { d.Close() }
}

func BenchmarkInsert_SQLitePureMem(b *testing.B) {
	d, cleanup := setupSQLitePureMem(b)
	defer cleanup()
	stmt, _ := d.Prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
	defer stmt.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stmt.Exec(key16(compareN+i), "name", i%100)
	}
}

func BenchmarkSelectByPK_SQLitePureMem(b *testing.B) {
	d, cleanup := setupSQLitePureMem(b)
	defer cleanup()
	stmt, _ := d.Prepare("SELECT name, age FROM users WHERE id = ?")
	defer stmt.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var name string
		var age int64
		stmt.QueryRow(key16(i%compareN)).Scan(&name, &age)
	}
}

func BenchmarkUpdateByPK_SQLitePureMem(b *testing.B) {
	d, cleanup := setupSQLitePureMem(b)
	defer cleanup()
	stmt, _ := d.Prepare("UPDATE users SET age = ? WHERE id = ?")
	defer stmt.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stmt.Exec((i%100)+1, key16(i%compareN))
	}
}

// -------- INSERT --------

func BenchmarkInsert_hazedb_Mem(b *testing.B) {
	db, _ := Open(Options{Schema: benchSchema(), sizeHint: b.N})
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(compareN+i), "name", i%100)
	}
}

func BenchmarkInsert_SQLite(b *testing.B) {
	d, cleanup := setupSQLite(b)
	defer cleanup()
	stmt, _ := d.Prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
	defer stmt.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stmt.Exec(key16(compareN+i), "name", i%100)
	}
}

// -------- SELECT BY PK --------

func BenchmarkSelectByPK_hazedb_Mem(b *testing.B) {
	db := newBenchDB(b, compareN)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.Query("SELECT name, age FROM users WHERE id = ?", tid(i%compareN))
	}
}

func BenchmarkSelectByPK_SQLite(b *testing.B) {
	d, cleanup := setupSQLite(b)
	defer cleanup()
	stmt, _ := d.Prepare("SELECT name, age FROM users WHERE id = ?")
	defer stmt.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var name string
		var age int64
		stmt.QueryRow(key16(i%compareN)).Scan(&name, &age)
	}
}

// -------- UPDATE BY PK --------

func BenchmarkUpdateByPK_hazedb_Mem(b *testing.B) {
	db := newBenchDB(b, compareN)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.Exec("UPDATE users SET age = ? WHERE id = ?", (i%100)+1, tid(i%compareN))
	}
}

func BenchmarkUpdateByPK_SQLite(b *testing.B) {
	d, cleanup := setupSQLite(b)
	defer cleanup()
	stmt, _ := d.Prepare("UPDATE users SET age = ? WHERE id = ?")
	defer stmt.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stmt.Exec((i%100)+1, key16(i%compareN))
	}
}

// -------- DELETE BY PK -------- (note: each iter deletes a fresh row
// to avoid running out, so the work isn't symmetric across stores;
// reinsert overhead is included in every iter.)

func BenchmarkDeleteByPK_hazedb_Mem(b *testing.B) {
	db, _ := Open(Options{Schema: benchSchema(), sizeHint: b.N})
	defer db.Close()
	for i := 0; i < b.N; i++ {
		db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "name", i%100)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.Exec("DELETE FROM users WHERE id = ?", tid(i))
	}
}

func BenchmarkDeleteByPK_SQLite(b *testing.B) {
	dir := b.TempDir()
	dsn := filepath.Join(dir, "cmp.sqlite")
	d, _ := sql.Open("sqlite3", dsn+"?_journal=WAL&_sync=NORMAL")
	defer d.Close()
	d.Exec("CREATE TABLE users (id BLOB PRIMARY KEY, name TEXT, age INTEGER)")
	ins, _ := d.Prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
	for i := 0; i < b.N; i++ {
		ins.Exec(key16(i), "name", i%100)
	}
	ins.Close()
	del, _ := d.Prepare("DELETE FROM users WHERE id = ?")
	defer del.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		del.Exec(key16(i))
	}
}

// -------- DELETE BY PK (in-memory, fair RAM-vs-RAM) --------
// Mirrors BenchmarkDeleteByPK_hazedb_Mem: fresh in-memory store, insert b.N
// rows, then time deleting them.
func BenchmarkDeleteByPK_SQLiteMem(b *testing.B) {
	d, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	d.SetMaxOpenConns(1)
	defer d.Close()
	d.Exec("CREATE TABLE users (id BLOB PRIMARY KEY, name TEXT, age INTEGER)")
	ins, _ := d.Prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
	for i := 0; i < b.N; i++ {
		ins.Exec(key16(i), "name", i%100)
	}
	ins.Close()
	del, _ := d.Prepare("DELETE FROM users WHERE id = ?")
	defer del.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		del.Exec(key16(i))
	}
}

// ===== Index vs scan: find ONE row three ways (PK / secondary index / scan) =====
// Shared table t(id PK, email [INDEX], code [no index], age), compareN rows. Every
// op below targets a single row found by its indexed column (email) or by an
// UN-indexed column (code, a full scan) — isolating the lookup cost. PK variants
// are the *ByPK benchmarks above. compareN rows; email/code unique per row.
//
// Note on hazedb's async index: an index lookup unions the dirty overlay, so a
// tight WRITE loop that touches an indexed column grows that overlay and inflates
// the next lookup. Only a write to the indexed column itself accrues the overlay:
// the by-index UPDATE sets age (un-indexed) and so needs no drain, but the
// by-index DELETE inserts a fresh email-indexed row each iteration and therefore
// drains with an untimed mergeIndexes(). Scan paths read live rows directly and
// need no drain. (SQLite has no async index — its loops are plain.)
//
// CAVEAT — wall time of the per-iteration-StopTimer benches is NOT the op cost.
// The benches that call b.StopTimer()/b.StartTimer() each iteration (to exclude
// an untimed fresh insert / merge) pay runtime.ReadMemStats per call, a
// stop-the-world that dwarfs the actual sub-µs op. Read allocs/op (the reliable
// signal) from those, not ns/op. This applies to the DELETE-by-index/scan benches
// (fresh insert each iteration); UpdateByIndex no longer uses that pattern, so
// its ns/op is now a real number.

func newIdxScanDB(b *testing.B) *DB {
	db, err := Open(Options{Schema: Schema{}, indexMergeInterval: -1, sizeHint: compareN})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	db.Exec("CREATE TABLE t (id uuid primary key, email text, code text, age int, INDEX (email))")
	for i := 0; i < compareN; i++ {
		db.Exec("INSERT INTO t (id, email, code, age) VALUES (?, ?, ?, ?)", tid(i), "e"+strconv.Itoa(i), "c"+strconv.Itoa(i), i%100)
	}
	db.mergeIndexes()
	return db
}

func newIdxScanSQLite(b *testing.B) *sql.DB {
	d, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	d.SetMaxOpenConns(1)
	b.Cleanup(func() { d.Close() })
	d.Exec("CREATE TABLE t (id BLOB PRIMARY KEY, email TEXT, code TEXT, age INTEGER)")
	d.Exec("CREATE INDEX idx_t_email ON t(email)")
	ins, _ := d.Prepare("INSERT INTO t (id, email, code, age) VALUES (?, ?, ?, ?)")
	for i := 0; i < compareN; i++ {
		ins.Exec(key16(i), "e"+strconv.Itoa(i), "c"+strconv.Itoa(i), i%100)
	}
	ins.Close()
	return d
}

// ---- FETCH by indexed column / by scan ----
func BenchmarkFetchByIndex_hazedb_Mem(b *testing.B) {
	db := newIdxScanDB(b)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, r, err := db.Query("SELECT age FROM t WHERE email = ?", "e"+strconv.Itoa(i%compareN)); err != nil || len(r) != 1 {
			b.Fatalf("rows=%d err=%v", len(r), err)
		}
	}
}
func BenchmarkFetchByIndex_SQLiteMem(b *testing.B) {
	s, _ := newIdxScanSQLite(b).Prepare("SELECT age FROM t WHERE email = ?")
	defer s.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var a int64
		s.QueryRow("e" + strconv.Itoa(i%compareN)).Scan(&a)
	}
}
func BenchmarkFetchByScan_hazedb_Mem(b *testing.B) {
	db := newIdxScanDB(b)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, r, err := db.Query("SELECT age FROM t WHERE code = ?", "c"+strconv.Itoa(i%compareN)); err != nil || len(r) != 1 {
			b.Fatalf("rows=%d err=%v", len(r), err)
		}
	}
}
func BenchmarkFetchByScan_SQLiteMem(b *testing.B) {
	s, _ := newIdxScanSQLite(b).Prepare("SELECT age FROM t WHERE code = ?")
	defer s.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var a int64
		s.QueryRow("c" + strconv.Itoa(i%compareN)).Scan(&a)
	}
}

// ---- COUNT(*) over an indexed column (compareN/countBuckets rows per key) ----
// A separate table c(id PK, bucket [INDEX], n) with compareN rows spread over
// countBuckets buckets, so COUNT(*) WHERE bucket = ? counts ~100 rows resolved
// through the index — not a full scan, and not a single row.
const countBuckets = 100

func newCountIdxDB(b *testing.B) *DB {
	db, err := Open(Options{Schema: Schema{}, indexMergeInterval: -1, sizeHint: compareN})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	db.Exec("CREATE TABLE c (id uuid primary key, bucket text, n int, INDEX (bucket))")
	for i := 0; i < compareN; i++ {
		db.Exec("INSERT INTO c (id, bucket, n) VALUES (?, ?, ?)", tid(i), "b"+strconv.Itoa(i%countBuckets), i)
	}
	db.mergeIndexes()
	return db
}

func newCountIdxSQLite(b *testing.B) *sql.DB {
	d, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	d.SetMaxOpenConns(1)
	b.Cleanup(func() { d.Close() })
	d.Exec("CREATE TABLE c (id BLOB PRIMARY KEY, bucket TEXT, n INTEGER)")
	d.Exec("CREATE INDEX idx_c_bucket ON c(bucket)")
	ins, _ := d.Prepare("INSERT INTO c (id, bucket, n) VALUES (?, ?, ?)")
	for i := 0; i < compareN; i++ {
		ins.Exec(key16(i), "b"+strconv.Itoa(i%countBuckets), i)
	}
	ins.Close()
	return d
}

func BenchmarkCountByIndex_hazedb_Mem(b *testing.B) {
	db := newCountIdxDB(b)
	want := int64(compareN / countBuckets)
	keys := make([]string, countBuckets) // prebuilt: the key string is not part of the op
	for i := range keys {
		keys[i] = "b" + strconv.Itoa(i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, r, err := db.Query("SELECT COUNT(*) FROM c WHERE bucket = ?", keys[i%countBuckets])
		if err != nil || r[0][0].Int() != want {
			b.Fatalf("err=%v rows=%v", err, r)
		}
	}
}

// Typed-arg variant: QueryValues skips the []any boxing + conversion, isolating
// hazedb's own count-result allocations.
func BenchmarkCountByIndex_hazedb_Typed(b *testing.B) {
	db := newCountIdxDB(b)
	want := int64(compareN / countBuckets)
	keys := make([]Value, countBuckets)
	for i := range keys {
		keys[i] = Str("b" + strconv.Itoa(i))
	}
	argv := make([]Value, 1)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		argv[0] = keys[i%countBuckets]
		_, r, err := db.QueryValues("SELECT COUNT(*) FROM c WHERE bucket = ?", argv...)
		if err != nil || r[0][0].Int() != want {
			b.Fatalf("err=%v rows=%v", err, r)
		}
	}
}

func BenchmarkCountByIndex_SQLiteMem(b *testing.B) {
	s, _ := newCountIdxSQLite(b).Prepare("SELECT COUNT(*) FROM c WHERE bucket = ?")
	defer s.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var n int64
		if err := s.QueryRow("b" + strconv.Itoa(i%countBuckets)).Scan(&n); err != nil {
			b.Fatal(err)
		}
	}
}

// JSON variant: QueryJSONInto into a reused buffer is the adapter path
// (fetchall_json / HTTP). For COUNT(*) it should encode alloc-free.
func BenchmarkCountByIndex_hazedb_JSON(b *testing.B) {
	db := newCountIdxDB(b)
	keys := make([]Value, countBuckets)
	for i := range keys {
		keys[i] = Str("b" + strconv.Itoa(i))
	}
	argv := make([]Value, 1)
	dst := make([]byte, 0, 64)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		argv[0] = keys[i%countBuckets]
		_, js, err := db.QueryJSONInto(dst[:0], "SELECT COUNT(*) FROM c WHERE bucket = ?", argv...)
		if err != nil {
			b.Fatal(err)
		}
		_ = js
	}
}

// ---- UPDATE by indexed column / by scan (1 row; WHERE column unchanged) ----
func BenchmarkUpdateByIndex_hazedb_Mem(b *testing.B) {
	db := newIdxScanDB(b)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// SET targets age, which is NOT indexed, so the write accrues no index
		// overlay — the loop stays steady-state with no per-iteration drain. (No
		// StopTimer/mergeIndexes here: that pattern's untimed ReadMemStats would
		// dwarf this sub-µs op; it is only needed by the DELETE benches below,
		// which insert a fresh indexed row each iteration.)
		db.Exec("UPDATE t SET age = ? WHERE email = ?", (i%100)+1, "e"+strconv.Itoa(i%compareN))
	}
}
func BenchmarkUpdateByIndex_SQLiteMem(b *testing.B) {
	s, _ := newIdxScanSQLite(b).Prepare("UPDATE t SET age = ? WHERE email = ?")
	defer s.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Exec((i%100)+1, "e"+strconv.Itoa(i%compareN))
	}
}
func BenchmarkUpdateByScan_hazedb_Mem(b *testing.B) {
	db := newIdxScanDB(b)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.Exec("UPDATE t SET age = ? WHERE code = ?", (i%100)+1, "c"+strconv.Itoa(i%compareN))
	}
}
func BenchmarkUpdateByScan_SQLiteMem(b *testing.B) {
	s, _ := newIdxScanSQLite(b).Prepare("UPDATE t SET age = ? WHERE code = ?")
	defer s.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Exec((i%100)+1, "c"+strconv.Itoa(i%compareN))
	}
}

// ---- DELETE by indexed column / by scan (insert a fresh row untimed, time its
// removal; table size stays ~constant) ----
func BenchmarkDeleteByIndex_hazedb_Mem(b *testing.B) {
	db := newIdxScanDB(b)
	// Pre-insert b.N deletable rows untimed, merge the index once, then time ONLY
	// the deletes — the same clean shape as BenchmarkDeleteByPK. The old
	// per-iteration StopTimer/StartTimer around a sub-µs delete inflated the
	// number ~15× (harness overhead, not delete cost).
	for i := 0; i < b.N; i++ {
		db.Exec("INSERT INTO t (id, email, code, age) VALUES (?, ?, ?, ?)", tid(compareN+i), "ek"+strconv.Itoa(i), "x", 1)
	}
	db.mergeIndexes()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.Exec("DELETE FROM t WHERE email = ?", "ek"+strconv.Itoa(i))
	}
}
func BenchmarkDeleteByIndex_SQLiteMem(b *testing.B) {
	d := newIdxScanSQLite(b)
	ins, _ := d.Prepare("INSERT INTO t (id, email, code, age) VALUES (?, ?, ?, ?)")
	for i := 0; i < b.N; i++ {
		ins.Exec(key16(compareN+i), "ek"+strconv.Itoa(i), "x", 1)
	}
	ins.Close()
	del, _ := d.Prepare("DELETE FROM t WHERE email = ?")
	defer del.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		del.Exec("ek" + strconv.Itoa(i))
	}
}
func BenchmarkDeleteByScan_hazedb_Mem(b *testing.B) {
	db := newIdxScanDB(b)
	// Pre-insert b.N rows untimed, then time ONLY the deletes (clean shape, no
	// per-iteration StopTimer). code is unindexed, so each delete full-scans.
	for i := 0; i < b.N; i++ {
		db.Exec("INSERT INTO t (id, email, code, age) VALUES (?, ?, ?, ?)", tid(compareN+i), "x"+strconv.Itoa(i), "ck"+strconv.Itoa(i), 1)
	}
	db.mergeIndexes()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.Exec("DELETE FROM t WHERE code = ?", "ck"+strconv.Itoa(i)) // full scan finds the live row
	}
}
func BenchmarkDeleteByScan_SQLiteMem(b *testing.B) {
	d := newIdxScanSQLite(b)
	ins, _ := d.Prepare("INSERT INTO t (id, email, code, age) VALUES (?, ?, ?, ?)")
	for i := 0; i < b.N; i++ {
		ins.Exec(key16(compareN+i), "x"+strconv.Itoa(i), "ck"+strconv.Itoa(i), 1)
	}
	ins.Close()
	del, _ := d.Prepare("DELETE FROM t WHERE code = ?")
	defer del.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		del.Exec("ck" + strconv.Itoa(i))
	}
}

// ---- ORDER BY on a composite ordered index (the "latest-N sorted" pattern) ----
// posts(id PK, author, title) with an ordered (author, title) index, compareN rows
// spread over orderAuthors authors (~compareN/orderAuthors each). The hot query is
// the top-N-sorted shape — WHERE author = ? ORDER BY title LIMIT N — which BOTH
// engines serve by walking the (author, title) index in order and stopping at the
// LIMIT, with no sort of the per-author set (hazedb's compWalk; SQLite's covering
// index). Titles are inserted out of order (a coprime stride) so the ORDER BY is
// real work, not an artefact of insertion order. This is a fair op-vs-op race —
// both walk the index — so the gap reflects in-process vs cgo+row-marshalling, not
// an algorithmic difference.
const orderAuthors = 100

func newOrderWalkDB(b *testing.B) *DB {
	db, err := Open(Options{Schema: Schema{}, indexMergeInterval: -1, sizeHint: compareN})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	db.Exec("CREATE TABLE posts (id uuid primary key, author text, title text, ORDERED INDEX (author, title))")
	for i := 0; i < compareN; i++ {
		db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)",
			tid(i), "a"+strconv.Itoa(i%orderAuthors), "t"+strconv.Itoa((i*7)%compareN))
	}
	db.mergeIndexes()
	return db
}

func newOrderWalkSQLite(b *testing.B) *sql.DB {
	d, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	d.SetMaxOpenConns(1)
	b.Cleanup(func() { d.Close() })
	d.Exec("CREATE TABLE posts (id BLOB PRIMARY KEY, author TEXT, title TEXT)")
	d.Exec("CREATE INDEX idx_posts_author_title ON posts(author, title)")
	ins, _ := d.Prepare("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)")
	for i := 0; i < compareN; i++ {
		ins.Exec(key16(i), "a"+strconv.Itoa(i%orderAuthors), "t"+strconv.Itoa((i*7)%compareN))
	}
	ins.Close()
	return d
}

func BenchmarkOrderByIndex_hazedb_Mem(b *testing.B) {
	db := newOrderWalkDB(b)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, r, err := db.Query("SELECT title FROM posts WHERE author = ? ORDER BY title LIMIT 20", "a"+strconv.Itoa(i%orderAuthors)); err != nil || len(r) != 20 {
			b.Fatalf("rows=%d err=%v", len(r), err)
		}
	}
}

func BenchmarkOrderByIndex_SQLiteMem(b *testing.B) {
	s, _ := newOrderWalkSQLite(b).Prepare("SELECT title FROM posts WHERE author = ? ORDER BY title LIMIT 20")
	defer s.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rows, err := s.Query("a" + strconv.Itoa(i%orderAuthors))
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var t string
			rows.Scan(&t)
		}
		rows.Close()
	}
}

// ---- Partition-pinned read (recent-N in a thread) ----
// hazedb's signature shape: msgs(id PK, thread PARTITION KEY, seq ordered), so
// WHERE thread = ? ORDER BY seq DESC LIMIT N reads ONLY that partition and walks
// its ordered tail — no cross-partition scan, no sort. SQLite has no partitioning,
// so the fair counterpart is a (thread, seq) index it walks backward, stopping at
// the LIMIT. compareN rows over msgThreads threads (~compareN/msgThreads each).
const msgThreads = 100

func newPartReadDB(b *testing.B) *DB {
	db, err := Open(Options{Schema: Schema{}, indexMergeInterval: -1, sizeHint: compareN})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	db.Exec("CREATE TABLE msgs (id uuid primary key, thread uuid partition key, seq int immutable, body text)")
	for i := 0; i < compareN; i++ {
		db.Exec("INSERT INTO msgs (id, thread, seq, body) VALUES (?, ?, ?, ?)", tid(i), tid(i%msgThreads), i, "b")
	}
	db.mergeIndexes()
	return db
}

func newPartReadSQLite(b *testing.B) *sql.DB {
	d, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	d.SetMaxOpenConns(1)
	b.Cleanup(func() { d.Close() })
	d.Exec("CREATE TABLE msgs (id BLOB PRIMARY KEY, thread BLOB, seq INTEGER, body TEXT)")
	d.Exec("CREATE INDEX idx_msgs_thread_seq ON msgs(thread, seq)")
	ins, _ := d.Prepare("INSERT INTO msgs (id, thread, seq, body) VALUES (?, ?, ?, ?)")
	for i := 0; i < compareN; i++ {
		ins.Exec(key16(i), key16(i%msgThreads), i, "b")
	}
	ins.Close()
	return d
}

func BenchmarkPartitionRead_hazedb_Mem(b *testing.B) {
	db := newPartReadDB(b)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, r, err := db.Query("SELECT body FROM msgs WHERE thread = ? ORDER BY seq DESC LIMIT 20", tid(i%msgThreads)); err != nil || len(r) != 20 {
			b.Fatalf("rows=%d err=%v", len(r), err)
		}
	}
}

func BenchmarkPartitionRead_SQLiteMem(b *testing.B) {
	s, _ := newPartReadSQLite(b).Prepare("SELECT body FROM msgs WHERE thread = ? ORDER BY seq DESC LIMIT 20")
	defer s.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rows, err := s.Query(key16(i % msgThreads))
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var body string
			rows.Scan(&body)
		}
		rows.Close()
	}
}

// ---- Bulk INSERT (multi-row VALUES) ----
// One statement inserts insBatch rows: hazedb compiles a per-tuple template and
// applies the batch atomically; SQLite uses a multi-VALUES INSERT. ns/op is per
// BATCH (divide by insBatch for per-row). Fresh PKs each iteration, no dup-key
// churn. Both in-memory (RAM vs RAM).
const insBatch = 50

func bulkInsertSQL(table string) string {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(table)
	b.WriteString(" (id, name, age) VALUES ")
	for i := 0; i < insBatch; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(?, ?, ?)")
	}
	return b.String()
}

func BenchmarkBulkInsert_hazedb_Mem(b *testing.B) {
	db, _ := Open(Options{Schema: benchSchema(), sizeHint: b.N * insBatch})
	defer db.Close()
	q := bulkInsertSQL("users")
	args := make([]any, insBatch*3)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		base := compareN + i*insBatch
		for j := 0; j < insBatch; j++ {
			args[j*3], args[j*3+1], args[j*3+2] = tid(base+j), "name", j%100
		}
		if n, err := db.Exec(q, args...); err != nil || n != insBatch {
			b.Fatalf("n=%d err=%v", n, err)
		}
	}
}

func BenchmarkBulkInsert_SQLiteMem(b *testing.B) {
	d, cleanup := setupSQLiteMem(b)
	defer cleanup()
	stmt, _ := d.Prepare(bulkInsertSQL("users"))
	defer stmt.Close()
	args := make([]any, insBatch*3)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		base := compareN + i*insBatch
		for j := 0; j < insBatch; j++ {
			args[j*3], args[j*3+1], args[j*3+2] = key16(base+j), "name", j%100
		}
		stmt.Exec(args...)
	}
}

// ---- Range + sorted list (filter a window, ordered, paginated) ----
// WHERE age >= ? AND age < ? ORDER BY age LIMIT N over an ordered index on age:
// hazedb walks the index in order and residual-filters the range (orderWalk),
// stopping at the LIMIT; SQLite walks the same index over the range. compareN rows,
// age spread 0..ageSpan so a 100-wide window holds many candidates the LIMIT trims.
const ageSpan = 1000

func newRangeDB(b *testing.B) *DB {
	db, err := Open(Options{Schema: Schema{}, indexMergeInterval: -1, sizeHint: compareN})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	db.Exec("CREATE TABLE u (id uuid primary key, name text, age int, ORDERED INDEX (age))")
	for i := 0; i < compareN; i++ {
		db.Exec("INSERT INTO u (id, name, age) VALUES (?, ?, ?)", tid(i), "n", (i*7)%ageSpan)
	}
	db.mergeIndexes()
	return db
}

func newRangeSQLite(b *testing.B) *sql.DB {
	d, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	d.SetMaxOpenConns(1)
	b.Cleanup(func() { d.Close() })
	d.Exec("CREATE TABLE u (id BLOB PRIMARY KEY, name TEXT, age INTEGER)")
	d.Exec("CREATE INDEX idx_u_age ON u(age)")
	ins, _ := d.Prepare("INSERT INTO u (id, name, age) VALUES (?, ?, ?)")
	for i := 0; i < compareN; i++ {
		ins.Exec(key16(i), "n", (i*7)%ageSpan)
	}
	ins.Close()
	return d
}

func BenchmarkRangeOrder_hazedb_Mem(b *testing.B) {
	db := newRangeDB(b)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lo := i % (ageSpan - 100)
		if _, r, err := db.Query("SELECT name FROM u WHERE age >= ? AND age < ? ORDER BY age LIMIT 20", lo, lo+100); err != nil || len(r) != 20 {
			b.Fatalf("rows=%d err=%v", len(r), err)
		}
	}
}

func BenchmarkRangeOrder_SQLiteMem(b *testing.B) {
	s, _ := newRangeSQLite(b).Prepare("SELECT name FROM u WHERE age >= ? AND age < ? ORDER BY age LIMIT 20")
	defer s.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lo := i % (ageSpan - 100)
		rows, err := s.Query(lo, lo+100)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var n string
			rows.Scan(&n)
		}
		rows.Close()
	}
}
