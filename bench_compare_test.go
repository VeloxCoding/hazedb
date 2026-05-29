package hazedb

import (
	"database/sql"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
	_ "github.com/mattn/go-sqlite3"
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
		stmt.QueryRow(key16(i % compareN)).Scan(&name, &age)
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
	db, _ := Open(Options{Schema: benchSchema(), SizeHint: b.N})
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
		stmt.QueryRow(key16(i % compareN)).Scan(&name, &age)
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
	db, _ := Open(Options{Schema: benchSchema(), SizeHint: b.N})
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
