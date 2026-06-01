package hazedb

import (
	"database/sql"
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	_ "github.com/mattn/go-sqlite3" // cgo driver: "sqlite3"
	bolt "go.etcd.io/bbolt"
	_ "modernc.org/sqlite" // pure-Go driver: "sqlite"
)

// These benchmarks compare FASTSQL's interpreter path to two pure
// alternatives: SQLite (via database/sql + cgo) and BoltDB (pure-Go
// B+tree). Same logical operation: INSERT one user, SELECT one user
// by PK, UPDATE by PK, DELETE by PK. Same row shape (id, name, age).
//
// Goal: honest interpreter-path numbers vs the two stores anyone would
// realistically reach for. The codegen target would shave parser+plan
// dispatch cost; these benchmarks describe today's path, not tomorrow's.
//
// All three stores use the same 16-byte UUID key (key16) so the comparison is
// fair on key width. Remaining caveats to read these numbers honestly:
//   - Reads are the cleanest comparison. For WRITES, hazedb-Mem is in-memory
//     only while Bolt fsyncs per transaction and SQLite syncs per its journal
//     mode — different durability, so write rows are not like-for-like.
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

func setupBolt(b *testing.B) (*bolt.DB, func()) {
	b.Helper()
	dir := b.TempDir()
	path := filepath.Join(dir, "cmp.bolt")
	d, err := bolt.Open(path, 0644, nil)
	if err != nil {
		b.Fatal(err)
	}
	if err := d.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists([]byte("users"))
		return e
	}); err != nil {
		b.Fatal(err)
	}
	if err := d.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("users"))
		for i := 0; i < compareN; i++ {
			val := append([]byte{}, "name|"...)
			val = binary.LittleEndian.AppendUint64(val, uint64(i%100))
			if err := bkt.Put(key16(i), val); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	return d, func() { d.Close() }
}

// -------- INSERT --------

func BenchmarkInsert_FASTSQL_Mem(b *testing.B) {
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

func BenchmarkInsert_Bolt(b *testing.B) {
	d, cleanup := setupBolt(b)
	defer cleanup()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d.Update(func(tx *bolt.Tx) error {
			bkt := tx.Bucket([]byte("users"))
			val := append([]byte{}, "name|"...)
			val = binary.LittleEndian.AppendUint64(val, uint64(i%100))
			return bkt.Put(key16(compareN+i), val)
		})
	}
}

// -------- SELECT BY PK --------

func BenchmarkSelectByPK_FASTSQL_Mem(b *testing.B) {
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

func BenchmarkSelectByPK_Bolt(b *testing.B) {
	d, cleanup := setupBolt(b)
	defer cleanup()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d.View(func(tx *bolt.Tx) error {
			bkt := tx.Bucket([]byte("users"))
			v := bkt.Get(key16(i % compareN))
			_ = v
			return nil
		})
	}
}

// -------- UPDATE BY PK --------

func BenchmarkUpdateByPK_FASTSQL_Mem(b *testing.B) {
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

func BenchmarkUpdateByPK_Bolt(b *testing.B) {
	d, cleanup := setupBolt(b)
	defer cleanup()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d.Update(func(tx *bolt.Tx) error {
			bkt := tx.Bucket([]byte("users"))
			k := key16(i % compareN)
			v := bkt.Get(k)
			nv := append([]byte{}, v...) // mutate-safe copy
			return bkt.Put(k, nv)
		})
	}
}

// -------- DELETE BY PK -------- (note: each iter deletes a fresh row
// to avoid running out, so the work isn't symmetric across stores;
// reinsert overhead is included in every iter.)

func BenchmarkDeleteByPK_FASTSQL_Mem(b *testing.B) {
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

func BenchmarkDeleteByPK_Bolt(b *testing.B) {
	dir := b.TempDir()
	d, _ := bolt.Open(filepath.Join(dir, "cmp.bolt"), 0644, nil)
	defer d.Close()
	d.Update(func(tx *bolt.Tx) error {
		bkt, _ := tx.CreateBucketIfNotExists([]byte("users"))
		for i := 0; i < b.N; i++ {
			bkt.Put(key16(i), []byte("v"))
		}
		return nil
	})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d.Update(func(tx *bolt.Tx) error {
			bkt := tx.Bucket([]byte("users"))
			return bkt.Delete(key16(i))
		})
	}
}

// -------- DELETE BY PK (in-memory, fair RAM-vs-RAM) --------
// Mirrors BenchmarkDeleteByPK_FASTSQL_Mem: fresh in-memory store, insert b.N
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
// tight WRITE loop on an indexed column would grow that overlay and inflate the
// next lookup. The by-index UPDATE/DELETE below therefore drain the overlay with
// an untimed mergeIndexes() each iteration, so the TIMED op measures the
// steady-state (merged) index path. Scan paths read live rows directly and need
// no such drain. (SQLite has no async index — its loops are plain.)

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
func BenchmarkFetchByIndex_FASTSQL_Mem(b *testing.B) {
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
func BenchmarkFetchByScan_FASTSQL_Mem(b *testing.B) {
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

// ---- UPDATE by indexed column / by scan (1 row; WHERE column unchanged) ----
func BenchmarkUpdateByIndex_FASTSQL_Mem(b *testing.B) {
	db := newIdxScanDB(b)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.Exec("UPDATE t SET age = ? WHERE email = ?", (i%100)+1, "e"+strconv.Itoa(i%compareN))
		b.StopTimer()
		db.mergeIndexes() // drain the overlay so the next lookup is steady-state
		b.StartTimer()
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
func BenchmarkUpdateByScan_FASTSQL_Mem(b *testing.B) {
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
func BenchmarkDeleteByIndex_FASTSQL_Mem(b *testing.B) {
	db := newIdxScanDB(b)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		em := "ek" + strconv.Itoa(i)
		db.Exec("INSERT INTO t (id, email, code, age) VALUES (?, ?, ?, ?)", tid(compareN+i), em, "x", 1)
		db.mergeIndexes() // merge the fresh row into the email index → steady-state delete
		b.StartTimer()
		db.Exec("DELETE FROM t WHERE email = ?", em)
	}
}
func BenchmarkDeleteByIndex_SQLiteMem(b *testing.B) {
	d := newIdxScanSQLite(b)
	ins, _ := d.Prepare("INSERT INTO t (id, email, code, age) VALUES (?, ?, ?, ?)")
	defer ins.Close()
	del, _ := d.Prepare("DELETE FROM t WHERE email = ?")
	defer del.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		em := "ek" + strconv.Itoa(i)
		ins.Exec(key16(compareN+i), em, "x", 1)
		b.StartTimer()
		del.Exec(em)
	}
}
func BenchmarkDeleteByScan_FASTSQL_Mem(b *testing.B) {
	db := newIdxScanDB(b)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cd := "ck" + strconv.Itoa(i)
		db.Exec("INSERT INTO t (id, email, code, age) VALUES (?, ?, ?, ?)", tid(compareN+i), "x"+strconv.Itoa(i), cd, 1)
		b.StartTimer()
		db.Exec("DELETE FROM t WHERE code = ?", cd) // full scan finds the live row
	}
}
func BenchmarkDeleteByScan_SQLiteMem(b *testing.B) {
	d := newIdxScanSQLite(b)
	ins, _ := d.Prepare("INSERT INTO t (id, email, code, age) VALUES (?, ?, ?, ?)")
	defer ins.Close()
	del, _ := d.Prepare("DELETE FROM t WHERE code = ?")
	defer del.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cd := "ck" + strconv.Itoa(i)
		ins.Exec(key16(compareN+i), "x"+strconv.Itoa(i), cd, 1)
		b.StartTimer()
		del.Exec(cd)
	}
}
