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

// BenchmarkGET = the exact hazedb-side work of the Caddy GET /get handler.
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
