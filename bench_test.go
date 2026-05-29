package hazedb

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func benchSchema() Schema {
	return Schema{
		Tables: []TableDef{
			{
				Name: "users",
				Columns: []ColumnDef{
					{Name: "id", Type: TypeInt, PK: true},
					{Name: "name", Type: TypeString},
					{Name: "age", Type: TypeInt},
				},
			},
		},
	}
}

func newBenchDB(b *testing.B, n int) *DB {
	b.Helper()
	db, err := Open(Options{Schema: benchSchema(), SizeHint: n})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	for i := 0; i < n; i++ {
		_, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", i, "name", i%100)
		if err != nil {
			b.Fatal(err)
		}
	}
	return db
}

func BenchmarkInsert_Mem(b *testing.B) {
	db, err := Open(Options{Schema: benchSchema(), SizeHint: b.N})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", i, "name", i%100)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsert_WAL(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(Options{Schema: benchSchema(), SizeHint: b.N, WALPath: filepath.Join(dir, "b.wal")})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", i, "name", i%100)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Durability-cost ladder vs BenchmarkInsert_WAL (flush-only). WALSync fsyncs
// on the ticker; WALSyncPerWrite fsyncs every record. Note: b.TempDir() in
// the container sits on an overlay FS, so absolute fsync cost here is not a
// real-disk number — read these as relative mode overhead, not latency SLAs.
func BenchmarkInsert_WALSync(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(Options{Schema: benchSchema(), SizeHint: b.N,
		WALPath: filepath.Join(dir, "b.wal"), WALSync: true, WALFlushInterval: 5 * time.Millisecond})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", i, "name", i%100); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsert_WALSyncPerWrite(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(Options{Schema: benchSchema(), SizeHint: b.N,
		WALPath: filepath.Join(dir, "b.wal"), WALSyncPerWrite: true})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", i, "name", i%100); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectByPK_Mem(b *testing.B) {
	const N = 10000
	db := newBenchDB(b, N)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := db.Query("SELECT name, age FROM users WHERE id = ?", i%N)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectRange_Mem(b *testing.B) {
	const N = 10000
	db := newBenchDB(b, N)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := db.Query("SELECT id FROM users WHERE age > ? LIMIT 10", 50)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdate_Mem(b *testing.B) {
	const N = 10000
	db := newBenchDB(b, N)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec("UPDATE users SET age = ? WHERE id = ?", (i%100)+1, i%N)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDelete_Mem(b *testing.B) {
	// Pre-insert N rows then delete b.N of them. To avoid running out
	// of rows we re-insert in the loop too.
	db, _ := Open(Options{Schema: benchSchema(), SizeHint: b.N})
	defer db.Close()
	for i := 0; i < b.N; i++ {
		db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", i, "name", i%100)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec("DELETE FROM users WHERE id = ?", i)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseOnly(b *testing.B) {
	queries := []string{
		"SELECT id, name FROM users WHERE age > ? LIMIT 10",
		"INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
		"UPDATE users SET age = ? WHERE id = ?",
		"DELETE FROM users WHERE id = ?",
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := parseSQL(queries[i%len(queries)])
		if err != nil {
			b.Fatal(err)
		}
	}
}

// String formatting overhead reference. Compare against BenchmarkInsert
// to gauge how much of the per-call cost is parser+plan vs storage.
func BenchmarkInsertViaStmtNoSQL(b *testing.B) {
	// Bypass SQL: build a plan once, reuse it across iterations.
	db, _ := Open(Options{Schema: benchSchema(), SizeHint: b.N})
	defer db.Close()
	pl, err := db.prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		args := []Value{Int(int64(i)), Str("name"), Int(int64(i % 100))}
		if _, err := db.execInsert(pl, args); err != nil {
			b.Fatal(err)
		}
	}
}

// Same idea for SELECT — measures cost of parse+plan vs raw execSelect.
func BenchmarkSelectByPKViaStmtNoSQL(b *testing.B) {
	const N = 10000
	db := newBenchDB(b, N)
	pl, err := db.prepare("SELECT name, age FROM users WHERE id = ?")
	if err != nil {
		b.Fatal(err)
	}
	args := make([]Value, 1)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		args[0] = Int(int64(i % N))
		_, _, err := db.execSelect(pl, args)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Round-trip parse+plan on every call. Establishes the baseline cost
// of FASTSQL's interpreter path.
func BenchmarkRoundtripSelectByPK(b *testing.B) {
	const N = 10000
	db := newBenchDB(b, N)
	q := fmt.Sprintf("SELECT name, age FROM users WHERE id = ?")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := db.Query(q, i%N)
		if err != nil {
			b.Fatal(err)
		}
	}
}
