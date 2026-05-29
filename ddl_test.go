package hazedb

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func openEmpty(t *testing.T) *DB {
	t.Helper()
	db, err := Open(Options{Schema: Schema{}}) // no bootstrap tables; create at runtime
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestRuntimeCreateTable(t *testing.T) {
	db := openEmpty(t)
	if _, err := db.Exec("CREATE TABLE users (id uuid primary key, name text, age int)"); err != nil {
		t.Fatal(err)
	}
	id := NewUUIDv7()
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", id, "alice", 30); err != nil {
		t.Fatal(err)
	}
	_, rows, err := db.Query("SELECT name, age FROM users WHERE id = ?", id)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0].S != "alice" || rows[0][1].I != 30 {
		t.Fatalf("got %v", rows)
	}
	if _, err := db.Exec("CREATE TABLE users (id uuid primary key)"); !errors.Is(err, ErrTableExists) {
		t.Fatalf("expected ErrTableExists, got %v", err)
	}
}

// A runtime-created table and its rows must survive a restart (the catalog is
// rebuilt from the log's CREATE record before its mutations replay).
func TestRuntimeCreateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ddl.wal")
	db, err := Open(Options{Schema: Schema{}, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	db.Exec("CREATE TABLE t (id uuid primary key, n int)")
	id := NewUUIDv7()
	db.Exec("INSERT INTO t (id, n) VALUES (?, ?)", id, 7)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(Options{Schema: Schema{}, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	_, rows, err := db2.Query("SELECT n FROM t WHERE id = ?", id)
	if err != nil {
		t.Fatalf("table gone after restart: %v", err)
	}
	if len(rows) != 1 || rows[0][0].I != 7 {
		t.Fatalf("data lost after restart: %v", rows)
	}
}

func TestRuntimeDropTable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "drop.wal")
	db, err := Open(Options{Schema: Schema{}, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	db.Exec("CREATE TABLE t (id uuid primary key, n int)")
	db.Exec("INSERT INTO t (id, n) VALUES (?, ?)", NewUUIDv7(), 1)
	if _, err := db.Exec("DROP TABLE t"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.Query("SELECT n FROM t"); !errors.Is(err, ErrUnknownTable) {
		t.Fatalf("expected unknown table after drop, got %v", err)
	}
	db.Close()

	db2, err := Open(Options{Schema: Schema{}, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if _, _, err := db2.Query("SELECT n FROM t"); !errors.Is(err, ErrUnknownTable) {
		t.Fatal("dropped table came back after restart")
	}
}

// A partitioned table created at runtime gets the full feature set — indexed
// partition scan and immutable seq — just like a predeclared one.
func TestRuntimePartitionedTable(t *testing.T) {
	db := openEmpty(t)
	if _, err := db.Exec("CREATE TABLE msgs (id uuid primary key, thread uuid partition key, seq int immutable, body text)"); err != nil {
		t.Fatal(err)
	}
	th := NewUUIDv7()
	for i := 0; i < 5; i++ {
		if _, err := db.Exec("INSERT INTO msgs (id, thread, seq, body) VALUES (?, ?, ?, ?)", NewUUIDv7(), th, i, "m"); err != nil {
			t.Fatal(err)
		}
	}
	_, rows, err := db.Query("SELECT seq FROM msgs WHERE thread = ? ORDER BY seq DESC LIMIT 2", th)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0][0].I != 4 || rows[1][0].I != 3 {
		t.Fatalf("partition scan on runtime table: %v", rows)
	}
	if _, err := db.Exec("UPDATE msgs SET seq = ? WHERE thread = ?", 0, th); err == nil {
		t.Error("expected immutable-seq rejection on runtime table")
	}
}

// A plan cached before a CREATE must keep working after it (the catalog
// version bumps, so the plan re-binds rather than dangling).
func TestPlanRebindAcrossCreate(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE a (id uuid primary key, n int)")
	id := NewUUIDv7()
	db.Exec("INSERT INTO a (id, n) VALUES (?, ?)", id, 1)
	db.Query("SELECT n FROM a WHERE id = ?", id) // prime the cache
	db.Exec("CREATE TABLE b (id uuid primary key)")
	_, rows, err := db.Query("SELECT n FROM a WHERE id = ?", id)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0].I != 1 {
		t.Fatalf("plan rebind after CREATE failed: %v", rows)
	}
}

// Reads/writes on an existing table must be unaffected (and race-free) while
// another goroutine creates tables. Run with -race.
func TestRuntimeCreateConcurrent(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE base (id uuid primary key, n int)")
	ids := make([]UUID, 100)
	for i := range ids {
		ids[i] = NewUUIDv7()
		db.Exec("INSERT INTO base (id, n) VALUES (?, ?)", ids[i], i)
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				db.Query("SELECT n FROM base WHERE id = ?", ids[i%100])
				db.Exec("UPDATE base SET n = ? WHERE id = ?", i, ids[i%100])
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			db.Exec(fmt.Sprintf("CREATE TABLE x%d (id uuid primary key)", i))
		}
	}()
	wg.Wait()
	_, rows, _ := db.Query("SELECT n FROM base WHERE id = ?", ids[0])
	if len(rows) != 1 {
		t.Fatal("base table disturbed by concurrent DDL")
	}
}

// --- benchmarks: a runtime-created table must be as fast as a predeclared one ---

func BenchmarkRuntimeCreatedInsert(b *testing.B) {
	db, _ := Open(Options{Schema: Schema{}, SizeHint: b.N})
	defer db.Close()
	db.Exec("CREATE TABLE u (id uuid primary key, name text, age int)")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.Exec("INSERT INTO u (id, name, age) VALUES (?, ?, ?)", tid(i), "name", i%100)
	}
}

func BenchmarkRuntimeCreatedSelect(b *testing.B) {
	const N = 10000
	db, _ := Open(Options{Schema: Schema{}, SizeHint: N})
	defer db.Close()
	db.Exec("CREATE TABLE u (id uuid primary key, name text, age int)")
	for i := 0; i < N; i++ {
		db.Exec("INSERT INTO u (id, name, age) VALUES (?, ?, ?)", tid(i), "name", i%100)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.Query("SELECT name, age FROM u WHERE id = ?", tid(i%N))
	}
}

// CREATE+DROP cost at a small catalog (create copies the registry, so cost
// scales with the number of tables; this measures the operation itself).
func BenchmarkCreateDropTable(b *testing.B) {
	db, _ := Open(Options{Schema: Schema{}})
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.Exec("CREATE TABLE tmp (id uuid primary key, n int)")
		db.Exec("DROP TABLE tmp")
	}
}
