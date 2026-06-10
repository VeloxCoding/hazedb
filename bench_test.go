package hazedb

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// buildMultiRowSQL returns an INSERT into users with k VALUES tuples.
func buildMultiRowSQL(cols, perTuple string, k int) string {
	var sb strings.Builder
	sb.WriteString("INSERT INTO users (")
	sb.WriteString(cols)
	sb.WriteString(") VALUES ")
	for i := 0; i < k; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(perTuple)
	}
	return sb.String()
}

// BenchmarkInsertMultiRow: k rows per INSERT, explicit PKs, WALPeriodic.
// Compare ns/row to BenchmarkInsert_WAL (single-row, same mode): the batch
// amortises one TXN envelope, one wal.mu acquisition + bufio.Write, and one
// parse across all k rows. In WALPeriodic this is ~neutral serially (buffered
// writes are cheap, offsetting the commit overhead); the decisive win is in
// WALPerWrite, where the batch fsyncs once instead of k times — measured ~96×
// faster per row (≈17.6µs vs ≈1.69ms) in this environment.
func BenchmarkInsertMultiRow(b *testing.B) {
	const k = 100
	sql := buildMultiRowSQL("id, name, age", "(?, ?, ?)", k)
	db, _ := Open(Options{Schema: benchSchema(), sizeHint: b.N * k, WALLevel: WALPeriodic, WALPath: b.TempDir() + "/b.wal"})
	defer db.Close()
	args := make([]any, 0, k*3)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		args = args[:0]
		base := i * k
		for j := 0; j < k; j++ {
			args = append(args, tid(base+j), "name", (base+j)%100)
		}
		if _, err := db.Exec(sql, args...); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*k), "ns/row")
}

// BenchmarkInsertMultiRowAutoPK: same but the id is omitted, so every row's PK
// is generated. Against BenchmarkInsertMultiRow it isolates the per-row UUIDv7
// cost inside a batch.
func BenchmarkInsertMultiRowAutoPK(b *testing.B) {
	const k = 100
	sql := buildMultiRowSQL("name, age", "(?, ?)", k)
	db, _ := Open(Options{Schema: benchSchema(), sizeHint: b.N * k, WALLevel: WALPeriodic, WALPath: b.TempDir() + "/b.wal"})
	defer db.Close()
	args := make([]any, 0, k*2)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		args = args[:0]
		for j := 0; j < k; j++ {
			args = append(args, "name", j%100)
		}
		if _, err := db.Exec(sql, args...); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*k), "ns/row")
}

func benchSchema() Schema {
	return Schema{
		Tables: []TableDef{
			{
				Name: "users",
				Columns: []ColumnDef{
					{Name: "id", Type: TypeUUID, PK: true},
					{Name: "name", Type: TypeString},
					{Name: "age", Type: TypeInt},
				},
			},
		},
	}
}

func newBenchDB(b *testing.B, n int) *DB {
	b.Helper()
	db, err := Open(Options{Schema: benchSchema(), sizeHint: n})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	for i := 0; i < n; i++ {
		_, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "name", i%100)
		if err != nil {
			b.Fatal(err)
		}
	}
	return db
}

func BenchmarkInsert_Mem(b *testing.B) {
	db, err := Open(Options{Schema: benchSchema(), sizeHint: b.N})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "name", i%100)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkInsertAutoPK_Mem: insert with the id omitted, so each row's PK comes
// from the global UUIDv7 generator (its mutex + periodic crypto/rand refill).
// The client-PK Insert benchmarks supply tid(i) and bypass that path entirely.
func BenchmarkInsertAutoPK_Mem(b *testing.B) {
	db, err := Open(Options{Schema: benchSchema(), sizeHint: b.N})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec("INSERT INTO users (name, age) VALUES (?, ?)", "name", i%100); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsert_WAL(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(Options{Schema: benchSchema(), sizeHint: b.N, WALLevel: WALPeriodic, WALPath: filepath.Join(dir, "b.wal")})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "name", i%100)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Durability-cost ladder vs BenchmarkInsert_WAL (WALPeriodic, default ticker).
// This bench fsyncs on a fast ticker; the per-write bench fsyncs every record.
// Note: b.TempDir() in
// the container sits on an overlay FS, so absolute fsync cost here is not a
// real-disk number — read these as relative mode overhead, not latency SLAs.
func BenchmarkInsert_WALSync(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(Options{Schema: benchSchema(), sizeHint: b.N,
		WALPath: filepath.Join(dir, "b.wal"), WALLevel: WALPeriodic, walFlushInterval: 5 * time.Millisecond})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "name", i%100); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsert_WALSyncPerWrite(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(Options{Schema: benchSchema(), sizeHint: b.N,
		WALPath: filepath.Join(dir, "b.wal"), WALLevel: WALPerWrite})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "name", i%100); err != nil {
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
		_, _, err := db.Query("SELECT name, age FROM users WHERE id = ?", tid(i%N))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectByPKRow_Mem(b *testing.B) {
	const N = 10000
	db := newBenchDB(b, N)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := db.QueryRow("SELECT name, age FROM users WHERE id = ?", tid(i%N))
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

// BenchmarkScanMatchAll: full multi-shard scan where every row matches and
// there is no LIMIT, so the packed-projection inner body (the partPinned /
// all-shards twin loops in execSelect) runs once per row — the path whose
// per-row cost matters if that body is factored out.
func BenchmarkScanMatchAll(b *testing.B) {
	const N = 10000
	db := newBenchDB(b, N)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, rows, err := db.Query("SELECT id FROM users WHERE age >= ?", 0)
		if err != nil || len(rows) != N {
			b.Fatalf("rows=%d err=%v", len(rows), err)
		}
	}
}

func BenchmarkUpdate_Mem(b *testing.B) {
	const N = 10000
	db := newBenchDB(b, N)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec("UPDATE users SET age = ? WHERE id = ?", (i%100)+1, tid(i%N))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDelete_Mem(b *testing.B) {
	// Pre-insert N rows then delete b.N of them. To avoid running out
	// of rows we re-insert in the loop too.
	db, _ := Open(Options{Schema: benchSchema(), sizeHint: b.N})
	defer db.Close()
	for i := 0; i < b.N; i++ {
		db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "name", i%100)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec("DELETE FROM users WHERE id = ?", tid(i))
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

// BenchmarkInsertWideSparse: insert into a wide table providing only 2 of 62
// columns (the rest nullable + omitted). Isolates the per-insert work that
// scales with total column count — the Null() prefill and the NOT-NULL sweep
// over every column — both of which the plan-time insert template removes.
func BenchmarkInsertWideSparse(b *testing.B) {
	const ncol = 60
	db, err := Open(Options{Schema: Schema{}})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	create := "CREATE TABLE w (id uuid primary key, c0 int"
	for i := 1; i < ncol; i++ {
		create += fmt.Sprintf(", c%d int null", i)
	}
	create += ")"
	if _, err := db.Exec(create); err != nil {
		b.Fatal(err)
	}
	pl, err := db.prepare("INSERT INTO w (id, c0) VALUES (?, ?)", db.cat.Load())
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		args := []Value{UUIDVal(tid(i)), Int(int64(i))}
		if _, err := db.execInsert(pl, args); err != nil {
			b.Fatal(err)
		}
	}
}

// String formatting overhead reference. Compare against BenchmarkInsert
// to gauge how much of the per-call cost is parser+plan vs storage.
func BenchmarkInsertViaStmtNoSQL(b *testing.B) {
	// Bypass SQL: build a plan once, reuse it across iterations.
	db, _ := Open(Options{Schema: benchSchema(), sizeHint: b.N})
	defer db.Close()
	pl, err := db.prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", db.cat.Load())
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		args := []Value{UUIDVal(tid(i)), Str("name"), Int(int64(i % 100))}
		if _, err := db.execInsert(pl, args); err != nil {
			b.Fatal(err)
		}
	}
}

// Same idea for SELECT — measures cost of parse+plan vs raw execSelect.
func BenchmarkSelectByPKViaStmtNoSQL(b *testing.B) {
	const N = 10000
	db := newBenchDB(b, N)
	pl, err := db.prepare("SELECT name, age FROM users WHERE id = ?", db.cat.Load())
	if err != nil {
		b.Fatal(err)
	}
	args := make([]Value, 1)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		args[0] = UUIDVal(tid(i % N))
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
		_, _, err := db.Query(q, tid(i%N))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkOrderByNoLimitWide: SELECT one column ORDER BY another over a wide
// table, no LIMIT — the case where capturing (key + projection) per match beats
// cloning the whole row and narrowing it afterwards. The win scales with table
// width (here 40 columns, projecting 1).
func BenchmarkOrderByNoLimitWide(b *testing.B) {
	const ncol, nrow = 40, 2000
	db, err := Open(Options{Schema: Schema{}})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	create := "CREATE TABLE w (id uuid primary key, ord int"
	insCols := "INSERT INTO w (id, ord"
	insVals := ") VALUES (?, ?"
	for i := 0; i < ncol; i++ {
		create += fmt.Sprintf(", c%d int", i)
		insCols += fmt.Sprintf(", c%d", i)
		insVals += ", ?"
	}
	create += ")"
	insVals += ")"
	if _, err := db.Exec(create); err != nil {
		b.Fatal(err)
	}
	insSQL := insCols + insVals
	for r := 0; r < nrow; r++ {
		args := make([]any, 0, ncol+2)
		args = append(args, NewUUIDv7(), int64((r*7)%nrow))
		for i := 0; i < ncol; i++ {
			args = append(args, int64(i))
		}
		if _, err := db.Exec(insSQL, args...); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for k := 0; k < b.N; k++ {
		_, rows, err := db.Query("SELECT c0 FROM w ORDER BY ord")
		if err != nil || len(rows) != nrow {
			b.Fatalf("rows=%d err=%v", len(rows), err)
		}
	}
}

// BenchmarkWALReplay: reopen a WAL-backed DB with 20k journaled inserts; each
// iteration replays the whole WAL. scanRecords now reuses one grow-only read
// buffer instead of allocating per record.
func BenchmarkWALReplay(b *testing.B) {
	const nrow = 20000
	path := filepath.Join(b.TempDir(), "replay.wal")
	db, err := Open(Options{Schema: Schema{}, WALLevel: WALPeriodic, WALPath: path})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE r (id uuid primary key, name text, n int)"); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < nrow; i++ {
		if _, err := db.Exec("INSERT INTO r (id, name, n) VALUES (?, ?, ?)", NewUUIDv7(), "x", int64(i)); err != nil {
			b.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for k := 0; k < b.N; k++ {
		d, err := Open(Options{Schema: Schema{}, WALLevel: WALPeriodic, WALPath: path})
		if err != nil {
			b.Fatal(err)
		}
		d.Close()
	}
}
