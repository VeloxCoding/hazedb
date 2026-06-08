package hazedb

// bench_hop_test.go — micro-decomposition of the PHP→hazedb call hop (minus the
// cgo crossing itself, which is fixed FrankenPHP overhead). Each benchmark
// isolates one segment so we can see where CPU and allocations actually go and
// size proposed wins:
//
//   - QueryArgs: adapter arg parse ("" / direct-UUID / JSON-array forms)
//   - prepare:   stmtCache lookup, with vs without the per-call SQL string copy
//   - RowsToJSON: result encoding cost as a function of row count
//   - Full*:     the whole hop a cgo function runs (args + Query + encode)
//
// Run: go test -run x -bench Hop -benchmem
//      go test -run x -bench Hop_Full_PK -benchmem -cpuprofile cpu.out -memprofile mem.out

import (
	"fmt"
	"strings"
	"testing"
)

const hopSelectPK = "SELECT name, age FROM users WHERE id = ?"

// --- arg parsing (adapter QueryArgs) ---

func BenchmarkHop_QueryArgs_DirectUUID(b *testing.B) {
	s := tid(42).String()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := QueryArgs(s); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHop_QueryArgs_JSONArray(b *testing.B) {
	s := `["` + tid(42).String() + `","Alice",30]`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := QueryArgs(s); err != nil {
			b.Fatal(err)
		}
	}
}

// --- prepare: cache hit, with vs without the per-call SQL copy ---

// Current cgo behaviour: the SQL is deep-copied (zendStringCopy) on EVERY call
// before prepare, even though a cache hit never retains the key.
func BenchmarkHop_PrepareHit_Copy(b *testing.B) {
	db := newBenchDB(b, 100)
	cat := db.cat.Load()
	if _, err := db.prepare(hopSelectPK, cat); err != nil { // warm cache
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := db.prepare(strings.Clone(hopSelectPK), db.cat.Load()); err != nil {
			b.Fatal(err)
		}
	}
}

// Proposed: pass a zero-copy view; core clones only on a miss (here: always hit).
func BenchmarkHop_PrepareHit_View(b *testing.B) {
	db := newBenchDB(b, 100)
	cat := db.cat.Load()
	if _, err := db.prepare(hopSelectPK, cat); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := db.prepare(hopSelectPK, db.cat.Load()); err != nil {
			b.Fatal(err)
		}
	}
}

// Isolated cost of the SQL copy alone (the thing we want to remove on hits).
func BenchmarkHop_SQLClone(b *testing.B) {
	b.ReportAllocs()
	var sink string
	for i := 0; i < b.N; i++ {
		sink = strings.Clone(hopSelectPK)
	}
	_ = sink
}

// --- result encoding (the part-2 question, baseline JSON) ---

func benchRows(b *testing.B, n int) ([]string, []Row) {
	b.Helper()
	db := newBenchDB(b, 10000)
	// LIMIT takes a literal integer (not a param); ages are i%100 so age >= 0
	// matches all rows. Inline n into the SQL.
	q := fmt.Sprintf("SELECT id, name, age FROM users WHERE age >= 0 LIMIT %d", n)
	cols, rows, err := db.Query(q)
	if err != nil {
		b.Fatal(err)
	}
	if len(rows) != n {
		b.Fatalf("want %d rows, got %d", n, len(rows))
	}
	return cols, rows
}

func benchRowsToJSON(b *testing.B, n int) {
	cols, rows := benchRows(b, n)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := RowsToJSON(cols, rows); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHop_RowsToJSON_1(b *testing.B)   { benchRowsToJSON(b, 1) }
func BenchmarkHop_RowsToJSON_10(b *testing.B)  { benchRowsToJSON(b, 10) }
func BenchmarkHop_RowsToJSON_100(b *testing.B) { benchRowsToJSON(b, 100) }

// --- full hop a cgo function runs (args parse + Query + JSON encode) ---

// Current shape: SQL copied every call + QueryArgs + Query + RowsToJSON.
func BenchmarkHop_Full_PK_Copy(b *testing.B) {
	const N = 10000
	db := newBenchDB(b, N)
	ids := make([]string, N)
	for i := range ids {
		ids[i] = tid(i).String()
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		args, err := QueryArgs(ids[i%N])
		if err != nil {
			b.Fatal(err)
		}
		cols, rows, err := db.Query(strings.Clone(hopSelectPK), args...)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := RowsToJSON(cols, rows); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHop_Full_GET models the Caddy GET /get read path: the id string is
// passed straight to QueryRow (no QueryArgs — a GET read takes the scalar from
// the URL), then the single row is encoded as a flat JSON object. The read
// counterpart to BenchmarkHop_Full_PK_*, which use Query + RowsToJSON (the
// list/POST shape). This is the path to drive allocs/CPU down on.
func BenchmarkHop_Full_GET(b *testing.B) {
	const N = 10000
	db := newBenchDB(b, N)
	ids := make([]string, N)
	for i := range ids {
		ids[i] = tid(i).String()
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cols, row, err := db.QueryRow(hopSelectPK, ids[i%N])
		if err != nil {
			b.Fatal(err)
		}
		if _, err := RowToJSONObject(cols, row); err != nil {
			b.Fatal(err)
		}
	}
}

// Proposed shape: SQL passed as a view (no per-call copy), rest identical.
func BenchmarkHop_Full_PK_View(b *testing.B) {
	const N = 10000
	db := newBenchDB(b, N)
	ids := make([]string, N)
	for i := range ids {
		ids[i] = tid(i).String()
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		args, err := QueryArgs(ids[i%N])
		if err != nil {
			b.Fatal(err)
		}
		cols, rows, err := db.Query(hopSelectPK, args...)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := RowsToJSON(cols, rows); err != nil {
			b.Fatal(err)
		}
	}
}
