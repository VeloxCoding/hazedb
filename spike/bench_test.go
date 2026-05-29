package spike_test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"go.etcd.io/bbolt"
	_ "github.com/mattn/go-sqlite3"

	"github.com/VeloxCoding/hazedb/spike"
)

// ----- GO-SQLDB benchmarks -----

func Benchmark_go_sqldb_Insert(b *testing.B) {
	for _, sync := range []bool{false, true} {
		name := "async"
		if sync {
			name = "sync"
		}
		b.Run(name, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "wal.log")
			db, err := spike.Open(path, sync)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := fmt.Sprintf("u%d", i)
				if _, err := db.Insert(spike.User{ID: id, Email: id + "@x", Name: id}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func Benchmark_go_sqldb_Get(b *testing.B) {
	path := filepath.Join(b.TempDir(), "wal.log")
	db, _ := spike.Open(path, false)
	defer db.Close()

	const n = 100_000
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("u%d", i)
		db.Insert(spike.User{ID: id, Email: id + "@x", Name: id})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("u%d", i%n)
		_, ok, err := db.Get(id)
		if err != nil || !ok {
			b.Fatal(ok, err)
		}
	}
}

// ----- SQLite (:memory:) benchmarks -----

func openSQLite(b *testing.B, sync string) *sql.DB {
	b.Helper()
	// Use a shared cache in-memory DB so we can configure pragmas.
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1) // memory DB is bound to one connection
	if _, err := db.Exec(`PRAGMA journal_mode=MEMORY; PRAGMA synchronous=` + sync + `;
		CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT, name TEXT);`); err != nil {
		b.Fatal(err)
	}
	return db
}

func BenchmarkSQLite_Insert(b *testing.B) {
	db := openSQLite(b, "OFF")
	defer db.Close()
	stmt, err := db.Prepare("INSERT INTO users(id,email,name) VALUES(?,?,?)")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("u%d", i)
		if _, err := stmt.Exec(id, id+"@x", id); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLite_Get(b *testing.B) {
	db := openSQLite(b, "OFF")
	defer db.Close()
	ins, _ := db.Prepare("INSERT INTO users(id,email,name) VALUES(?,?,?)")
	const n = 100_000
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("u%d", i)
		ins.Exec(id, id+"@x", id)
	}
	ins.Close()

	stmt, err := db.Prepare("SELECT email, name FROM users WHERE id = ?")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	b.ReportAllocs()
	b.ResetTimer()
	var email, name string
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("u%d", i%n)
		if err := stmt.QueryRow(id).Scan(&email, &name); err != nil {
			b.Fatal(err)
		}
	}
}

// ----- Zero-overhead variants: pre-generated IDs ----------------------
//
// The original benches do fmt.Sprintf + string-concat inside the b.N loop,
// which adds ~150 ns + 2 allocs per iteration. The variants below
// pre-generate the IDs so we see the library's actual cost.

func preGenIDs(n int) ([]string, []string, []string) {
	ids := make([]string, n)
	emails := make([]string, n)
	names := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("u%d", i)
		emails[i] = ids[i] + "@x"
		names[i] = ids[i]
	}
	return ids, emails, names
}

func Benchmark_go_sqldb_InsertBatch(b *testing.B) {
	const batchSize = 1000
	path := filepath.Join(b.TempDir(), "wal.log")
	db, _ := spike.OpenWithSize(path, false, b.N)
	defer db.Close()
	ids, emails, names := preGenIDs(b.N)
	batches := (b.N + batchSize - 1) / batchSize
	b.ReportAllocs()
	b.ResetTimer()
	for k := 0; k < batches; k++ {
		start := k * batchSize
		end := start + batchSize
		if end > b.N {
			end = b.N
		}
		users := make([]spike.User, end-start)
		for i := range users {
			j := start + i
			users[i] = spike.User{ID: ids[j], Email: emails[j], Name: names[j]}
		}
		if err := db.InsertBatch(users); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_go_sqldb_InsertBatchMemory(b *testing.B) {
	const batchSize = 1000
	db := spike.OpenMemory(b.N)
	defer db.Close()
	ids, emails, names := preGenIDs(b.N)
	batches := (b.N + batchSize - 1) / batchSize
	b.ReportAllocs()
	b.ResetTimer()
	for k := 0; k < batches; k++ {
		start := k * batchSize
		end := start + batchSize
		if end > b.N {
			end = b.N
		}
		users := make([]spike.User, end-start)
		for i := range users {
			j := start + i
			users[i] = spike.User{ID: ids[j], Email: emails[j], Name: names[j]}
		}
		if err := db.InsertBatch(users); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_go_sqldb_InsertMemory(b *testing.B) {
	db := spike.OpenMemory(b.N)
	defer db.Close()
	ids, emails, names := preGenIDs(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]}); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_go_sqldb_GetMemory(b *testing.B) {
	const n = 100_000
	db := spike.OpenMemory(n)
	defer db.Close()
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok, err := db.Get(ids[i%n])
		if err != nil || !ok {
			b.Fatal(ok, err)
		}
	}
}

func Benchmark_go_sqldb_InsertPreSized(b *testing.B) {
	path := filepath.Join(b.TempDir(), "wal.log")
	db, _ := spike.OpenWithSize(path, false, b.N)
	defer db.Close()
	ids, emails, names := preGenIDs(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]}); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_go_sqldb_InsertPreGen(b *testing.B) {
	path := filepath.Join(b.TempDir(), "wal.log")
	db, _ := spike.Open(path, false)
	defer db.Close()
	ids, emails, names := preGenIDs(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]}); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_go_sqldb_GetPreGen(b *testing.B) {
	path := filepath.Join(b.TempDir(), "wal.log")
	db, _ := spike.Open(path, false)
	defer db.Close()
	const n = 100_000
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok, err := db.Get(ids[i%n])
		if err != nil || !ok {
			b.Fatal(ok, err)
		}
	}
}

func BenchmarkSQLite_GetPreGen(b *testing.B) {
	db := openSQLite(b, "OFF")
	defer db.Close()
	ins, _ := db.Prepare("INSERT INTO users(id,email,name) VALUES(?,?,?)")
	const n = 100_000
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		ins.Exec(ids[i], emails[i], names[i])
	}
	ins.Close()

	stmt, err := db.Prepare("SELECT email, name FROM users WHERE id = ?")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	b.ReportAllocs()
	b.ResetTimer()
	var email, name string
	for i := 0; i < b.N; i++ {
		if err := stmt.QueryRow(ids[i%n]).Scan(&email, &name); err != nil {
			b.Fatal(err)
		}
	}
}

// ----- Parallel: multiple goroutines hammering the same DB -------------

func Benchmark_go_sqldb_InsertParallel(b *testing.B) {
	path := filepath.Join(b.TempDir(), "wal.log")
	db, _ := spike.Open(path, false)
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&counter, 1)
			id := fmt.Sprintf("u%d", n)
			if _, err := db.Insert(spike.User{ID: id, Email: id + "@x", Name: id}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func Benchmark_go_sqldb_UnsafeGetParallel(b *testing.B) {
	path := filepath.Join(b.TempDir(), "wal.log")
	db, _ := spike.Open(path, false)
	defer db.Close()
	const n = 100_000
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]})
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, ok := db.UnsafeGet(ids[i%n])
			if !ok {
				b.Fatal(ok)
			}
			i++
		}
	})
}

func Benchmark_go_sqldb_UnsafeGet(b *testing.B) {
	path := filepath.Join(b.TempDir(), "wal.log")
	db, _ := spike.Open(path, false)
	defer db.Close()
	const n = 100_000
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok := db.UnsafeGet(ids[i%n])
		if !ok {
			b.Fatal(ok)
		}
	}
}

func Benchmark_go_sqldb_GetParallel(b *testing.B) {
	path := filepath.Join(b.TempDir(), "wal.log")
	db, _ := spike.Open(path, false)
	defer db.Close()
	const n = 100_000
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]})
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, ok, err := db.Get(ids[i%n])
			if err != nil || !ok {
				b.Fatal(ok, err)
			}
			i++
		}
	})
}

// ----- BoltDB benchmarks -----

func BenchmarkBolt_Insert(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bolt.db")
	db, err := bbolt.Open(path, 0644, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket([]byte("users"))
		return err
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("u%d", i)
		err := db.Update(func(tx *bbolt.Tx) error {
			return tx.Bucket([]byte("users")).Put([]byte(id), []byte(id+"|"+id+"@x|"+id))
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBolt_Get(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bolt.db")
	db, _ := bbolt.Open(path, 0644, nil)
	defer db.Close()
	db.Update(func(tx *bbolt.Tx) error {
		bkt, _ := tx.CreateBucketIfNotExists([]byte("users"))
		for i := 0; i < 100_000; i++ {
			id := fmt.Sprintf("u%d", i)
			bkt.Put([]byte(id), []byte(id+"|"+id+"@x|"+id))
		}
		return nil
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("u%d", i%100_000)
		err := db.View(func(tx *bbolt.Tx) error {
			v := tx.Bucket([]byte("users")).Get([]byte(id))
			if v == nil {
				return fmt.Errorf("missing")
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
