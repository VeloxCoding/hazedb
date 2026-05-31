package hazedb

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

// seedStreamTable builds a table exercising every column kind, a hash index on
// two columns, NULLs, and a BYTES cell, so the streaming-vs-materialized
// comparison covers projection, base64, NULL, and multi-index AND.
func seedStreamTable(t *testing.T) *DB {
	t.Helper()
	db := openEmpty(t)
	if _, err := db.Exec("CREATE TABLE users (id uuid primary key, name text, age int null, city text, data bytes, INDEX (name), INDEX (city))"); err != nil {
		t.Fatal(err)
	}
	rows := []struct {
		name, city string
		age        any
		data       []byte
	}{
		{"Alice", "AMS", 30, []byte{0x00, 0x01, 0xff}},
		{"Peter", "AMS", 41, nil},
		{"Peter", "RTM", 25, []byte("hi")},
		{"Bob", "AMS", nil, []byte{0x7f, 0x80}},
		{"Peter", "AMS", 55, []byte("data\"with\\escapes\n")},
	}
	for _, r := range rows {
		if _, err := db.Exec("INSERT INTO users (id, name, age, city, data) VALUES (?, ?, ?, ?, ?)",
			NewUUIDv7(), r.name, r.age, r.city, r.data); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// The streaming reads must produce byte-identical JSON to the materialized
// Query path, for every SELECT shape — both before the merge (dirty overlay
// feeds the index path) and after. This pins QueryJSON and QueryEach against the
// trusted Query/RowsToJSONObjects oracle.
func TestQueryStreamMatchesMaterialized(t *testing.T) {
	db := seedStreamTable(t)
	queries := []string{
		"SELECT * FROM users",
		"SELECT name, age FROM users",
		"SELECT * FROM users WHERE age >= 30",
		"SELECT name FROM users WHERE name = 'Alice'",                            // indexed eq
		"SELECT id, name, city FROM users WHERE name = 'Peter' AND city = 'AMS'", // two-index AND
		"SELECT name FROM users WHERE city = 'NONE'",                             // empty
		"SELECT name, data FROM users WHERE name = 'Bob'",                        // indexed, BYTES
		"SELECT * FROM users LIMIT 2",                                            // scan + LIMIT
		"SELECT name FROM users WHERE name = 'Peter' LIMIT 2",                    // indexed + LIMIT
		"SELECT name FROM users ORDER BY name ASC",                               // ORDER BY (fallback)
		"SELECT name, age FROM users ORDER BY age DESC LIMIT 2",                  // ORDER BY + LIMIT (fallback)
	}

	for _, phase := range []string{"overlay", "merged"} {
		if phase == "merged" {
			db.mergeIndexes()
		}
		for _, sql := range queries {
			wantCols, wantRows, err := db.Query(sql)
			if err != nil {
				t.Fatalf("[%s] Query(%q): %v", phase, sql, err)
			}
			wantJSON, _ := RowsToJSONObjects(wantCols, wantRows)

			// QueryJSON: byte-identical to the materialized encode.
			gotCols, gotJSON, err := db.QueryJSON(sql)
			if err != nil {
				t.Fatalf("[%s] QueryJSON(%q): %v", phase, sql, err)
			}
			if string(gotJSON) != string(wantJSON) {
				t.Fatalf("[%s] QueryJSON(%q):\n got %s\nwant %s", phase, sql, gotJSON, wantJSON)
			}
			if len(gotCols) != len(wantCols) {
				t.Fatalf("[%s] QueryJSON(%q) cols %v want %v", phase, sql, gotCols, wantCols)
			}

			// QueryEach: clone each streamed row (contract: copy before return),
			// then encode — must match too.
			var each []Row
			if err := db.QueryEach(sql, nil, func(cols []string, row Row) bool {
				each = append(each, row.Clone())
				return true
			}); err != nil {
				t.Fatalf("[%s] QueryEach(%q): %v", phase, sql, err)
			}
			eachJSON, _ := RowsToJSONObjects(wantCols, each)
			if string(eachJSON) != string(wantJSON) {
				t.Fatalf("[%s] QueryEach(%q):\n got %s\nwant %s", phase, sql, eachJSON, wantJSON)
			}
		}
	}
}

// QueryEach must stop when the callback returns false (early-out before LIMIT).
func TestQueryEachEarlyStop(t *testing.T) {
	db := seedStreamTable(t)
	db.mergeIndexes()
	n := 0
	if err := db.QueryEach("SELECT name FROM users", nil, func(cols []string, row Row) bool {
		n++
		return n < 2 // stop after the second row
	}); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("early stop visited %d rows, want 2", n)
	}
}

// Golden invariant under concurrency: streaming reads run their visitor under
// the shard lock while writers mutate the same rows. -race must stay clean and
// no read may observe a torn row.
func TestQueryStreamConcurrent(t *testing.T) {
	db, err := Open(Options{Schema: Schema{}, indexMergeInterval: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.Exec("CREATE TABLE t (id uuid primary key, grp int, name text, data bytes, INDEX (grp))")
	const N = 200
	ids := make([]UUID, N)
	for i := range ids {
		ids[i] = NewUUIDv7()
		db.Exec("INSERT INTO t (id, grp, name, data) VALUES (?, ?, ?, ?)", ids[i], i%10, "n"+strconv.Itoa(i), []byte("payload"))
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			r := seed
			for {
				select {
				case <-stop:
					return
				default:
				}
				db.Exec("UPDATE t SET name = ?, data = ? WHERE id = ?", "x"+strconv.Itoa(r), []byte("p"+strconv.Itoa(r)), ids[r%N])
				r += 7
			}
		}(w)
	}
	var rwg sync.WaitGroup
	for rd := 0; rd < 4; rd++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for k := 0; k < 2000; k++ {
				if _, _, err := db.QueryJSON("SELECT id, grp, name, data FROM t WHERE grp = ?", Int(5)); err != nil {
					t.Error(err)
					return
				}
				err := db.QueryEach("SELECT * FROM t", []Value{}, func(cols []string, row Row) bool {
					_ = row[0].UUID() // touch the live row under the lock
					return true
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	rwg.Wait()
	close(stop)
	wg.Wait()
}

// Benchmark: a full-scan SELECT of n rows encoded to JSON, streaming (QueryJSON,
// no []Row) vs materialized (QueryValues + RowsToJSONObjects). Reports the
// allocation/GC pressure the streaming path removes.
func benchJSON(b *testing.B, n int, stream bool) {
	db, err := Open(Options{Schema: Schema{}, indexMergeInterval: -1, sizeHint: n})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.Exec("CREATE TABLE users (id uuid primary key, name text, email text, age int)")
	for i := 0; i < n; i++ {
		db.Exec("INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
			NewUUIDv7(), "user"+strconv.Itoa(i), "user"+strconv.Itoa(i)+"@example.com", i)
	}
	const sql = "SELECT id, name, email, age FROM users"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if stream {
			if _, body, _ := db.QueryJSON(sql); len(body) == 0 {
				b.Fatal("empty")
			}
		} else {
			cols, rows, _ := db.QueryValues(sql)
			if body, _ := RowsToJSONObjects(cols, rows); len(body) == 0 {
				b.Fatal("empty")
			}
		}
	}
}

func BenchmarkQueryJSON_Stream_1k(b *testing.B)        { benchJSON(b, 1000, true) }
func BenchmarkQueryJSON_Materialized_1k(b *testing.B)  { benchJSON(b, 1000, false) }
func BenchmarkQueryJSON_Stream_10k(b *testing.B)       { benchJSON(b, 10000, true) }
func BenchmarkQueryJSON_Materialized_10k(b *testing.B) { benchJSON(b, 10000, false) }
