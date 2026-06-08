// Package bench_getpath isolates the Caddy GET /get hot path (QueryRow +
// RowToJSONObject) in a pure-Go, cgo-free test binary so pprof can symbolise it
// cleanly — the core package's own _test.go pulls cgo SQLite, which breaks
// pprof. Imports only the exported hazedb API.
//
//	CGO_ENABLED=0 go test ./bench_getpath -run x -bench GET -benchmem \
//	    -memprofile mem.out -cpuprofile cpu.out
package bench_getpath

import (
	"fmt"
	"testing"

	"github.com/VeloxCoding/hazedb"
)

const selPK = "SELECT name, age FROM users WHERE id = ?"

func setup(tb testing.TB, n int) (*hazedb.DB, []string) {
	tb.Helper()
	db, err := hazedb.Open(hazedb.Options{})
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE users (id uuid primary key, name text, age int)"); err != nil {
		tb.Fatal(err)
	}
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("00000000-0000-7000-8000-%012x", i)
		if _, err := db.Exec("INSERT INTO users (id,name,age) VALUES (?,?,?)", ids[i], fmt.Sprintf("n%d", i), i%100); err != nil {
			tb.Fatal(err)
		}
	}
	return db, ids
}

// TestGETFused checks the fused path returns the same flat object the handler
// produces, and reports not-found for an absent id.
func TestGETFused(t *testing.T) {
	db, ids := setup(t, 100)
	defer db.Close()
	u, err := hazedb.ParseUUID(ids[42])
	if err != nil {
		t.Fatal(err)
	}
	out, found, err := db.QueryRowJSONByPK(nil, selPK, u)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if want := `{"name":"n42","age":42}`; string(out) != want {
		t.Fatalf("got %s want %s", out, want)
	}
	if _, found, _ := db.QueryRowJSONByPK(nil, selPK, hazedb.UUID{}); found {
		t.Fatal("expected not-found for the zero uuid")
	}
}

// BenchmarkGET = the original GET-path shape: QueryRow(string id) + RowToJSONObject.
func BenchmarkGET(b *testing.B) {
	db, ids := setup(b, 10000)
	defer db.Close()
	n := len(ids)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cols, row, err := db.QueryRow(selPK, ids[i%n])
		if err != nil {
			b.Fatal(err)
		}
		if _, err := hazedb.RowToJSONObject(cols, row); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGET_Fused = the optimised GET path: parse the URL id string to a
// typed UUID, then QueryRowJSONByPK encodes under the shard lock into a reused
// buffer — no arg boxing, no Row clone, no per-call output alloc.
func BenchmarkGET_Fused(b *testing.B) {
	db, ids := setup(b, 10000)
	defer db.Close()
	n := len(ids)
	buf := make([]byte, 0, 256)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		u, err := hazedb.ParseUUID(ids[i%n]) // handler parses the URL string per request
		if err != nil {
			b.Fatal(err)
		}
		buf, _, err = db.QueryRowJSONByPK(buf[:0], selPK, u)
		if err != nil {
			b.Fatal(err)
		}
	}
}
