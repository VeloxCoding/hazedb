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
// FAIRNESS CAVEAT (M4): hazedb's PK is now a 16-byte UUID, while the SQLite
// and Bolt sides below still use 8-byte integer keys. The cross-store
// numbers therefore compare different key widths and are NOT apples-to-apples
// until SQLite (BLOB/TEXT PK) and Bolt (16-byte keys) are switched to UUIDs
// too. Open decision — see the implementation plan. The hazedb-only
// (_FASTSQL_) numbers remain valid in isolation.

const compareN = 10000

func setupSQLite(b *testing.B) (*sql.DB, func()) {
	b.Helper()
	dir := b.TempDir()
	dsn := filepath.Join(dir, "cmp.sqlite")
	d, err := sql.Open("sqlite3", dsn+"?_journal=WAL&_sync=NORMAL")
	if err != nil {
		b.Fatal(err)
	}
	if _, err := d.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"); err != nil {
		b.Fatal(err)
	}
	stmt, err := d.Prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < compareN; i++ {
		if _, err := stmt.Exec(i, "name", i%100); err != nil {
			b.Fatal(err)
		}
	}
	return d, func() { d.Close(); os.Remove(dsn) }
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
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], uint64(i))
			val := append([]byte{}, "name|"...)
			val = binary.LittleEndian.AppendUint64(val, uint64(i%100))
			if err := bkt.Put(k[:], val); err != nil {
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
		stmt.Exec(compareN+i, "name", i%100)
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
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], uint64(compareN+i))
			val := append([]byte{}, "name|"...)
			val = binary.LittleEndian.AppendUint64(val, uint64(i%100))
			return bkt.Put(k[:], val)
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
		stmt.QueryRow(i % compareN).Scan(&name, &age)
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
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], uint64(i%compareN))
			v := bkt.Get(k[:])
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
		stmt.Exec((i%100)+1, i%compareN)
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
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], uint64(i%compareN))
			v := bkt.Get(k[:])
			nv := append([]byte{}, v...) // mutate-safe copy
			return bkt.Put(k[:], nv)
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
	d.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	ins, _ := d.Prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
	for i := 0; i < b.N; i++ {
		ins.Exec(i, "name", i%100)
	}
	ins.Close()
	del, _ := d.Prepare("DELETE FROM users WHERE id = ?")
	defer del.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		del.Exec(i)
	}
}

func BenchmarkDeleteByPK_Bolt(b *testing.B) {
	dir := b.TempDir()
	d, _ := bolt.Open(filepath.Join(dir, "cmp.bolt"), 0644, nil)
	defer d.Close()
	d.Update(func(tx *bolt.Tx) error {
		bkt, _ := tx.CreateBucketIfNotExists([]byte("users"))
		for i := 0; i < b.N; i++ {
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], uint64(i))
			bkt.Put(k[:], []byte("v"))
		}
		return nil
	})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d.Update(func(tx *bolt.Tx) error {
			bkt := tx.Bucket([]byte("users"))
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], uint64(i))
			return bkt.Delete(k[:])
		})
	}
}
