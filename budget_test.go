package hazedb

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

// perIntRow is the exact rowCost of a (uuid, int) row with no secondary index:
// two Values plus the fixed per-row overhead. Used to size MaxBytes to a known
// row count.
func perIntRow() int64 {
	return int64(2*int(unsafe.Sizeof(Value{})) + rowFixedOverhead)
}

// budgetTotal sums every table's live byte tally — the figure the budget total
// must equal when the cap is enabled.
func budgetTotal(db *DB) int64 {
	var sum int64
	for _, rt := range db.cat.Load().byName {
		for i := range rt.shards {
			s := &rt.shards[i]
			s.mu.RLock()
			sum += s.bytes
			s.mu.RUnlock()
		}
	}
	return sum
}

// MaxBytes admits inserts up to the cap, rejects the one that would exceed it
// with ErrCapacity (nothing applied), and admits again once a DELETE frees room
// — the cache never auto-evicts.
func TestMaxBytesEnforced(t *testing.T) {
	const rows = 10
	db, err := Open(Options{MaxBytes: perIntRow() * rows})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.Exec("CREATE TABLE t (id uuid primary key, n int)")

	for i := 0; i < rows; i++ {
		if _, err := db.Exec("INSERT INTO t (id, n) VALUES (?, ?)", tid(i), i); err != nil {
			t.Fatalf("insert %d should fit: %v", i, err)
		}
	}
	// The cap is full; the next insert is rejected and applies nothing.
	if _, err := db.Exec("INSERT INTO t (id, n) VALUES (?, ?)", tid(rows), rows); !errors.Is(err, ErrCapacity) {
		t.Fatalf("over-cap insert: got %v, want ErrCapacity", err)
	}
	if got := db.MetaSnapshot().TotalRows; got != rows {
		t.Fatalf("rows after rejected insert=%d, want %d", got, rows)
	}
	if got := db.MetaSnapshot().TotalApproxBytes; got != perIntRow()*rows {
		t.Fatalf("total=%d, want %d", got, perIntRow()*rows)
	}

	// Free one row; the insert now fits.
	if _, err := db.Exec("DELETE FROM t WHERE id = ?", tid(0)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t (id, n) VALUES (?, ?)", tid(rows), rows); err != nil {
		t.Fatalf("after delete, insert should fit: %v", err)
	}
	if got := db.MetaSnapshot().TotalRows; got != rows {
		t.Fatalf("rows after free+insert=%d, want %d", got, rows)
	}
}

// MaxBytes == 0 is unlimited: the byte tally still tracks size, but no insert is
// ever rejected and the budget counter stays at zero (the hot path never touches
// it).
func TestMaxBytesDisabledByDefault(t *testing.T) {
	db := openEmpty(t) // Options.MaxBytes unset
	db.Exec("CREATE TABLE t (id uuid primary key, body text)")
	for i := 0; i < 1000; i++ {
		if _, err := db.Exec("INSERT INTO t (id, body) VALUES (?, ?)", tid(i), strings.Repeat("x", 100)); err != nil {
			t.Fatalf("unlimited insert %d: %v", i, err)
		}
	}
	if db.budget.total.Load() != 0 {
		t.Fatalf("unlimited budget total=%d, want 0 (untouched)", db.budget.total.Load())
	}
	if db.MetaSnapshot().TotalRows != 1000 {
		t.Fatalf("rows=%d, want 1000", db.MetaSnapshot().TotalRows)
	}
}

// With the cap enabled and headroom to spare, the budget counter must stay equal
// to the sum of the per-shard tallies across every insert, delete, and
// size-changing update path — so admission decisions read a faithful total.
func TestMaxBytesAccountingReconciles(t *testing.T) {
	db, err := Open(Options{MaxBytes: 1 << 30}) // 1 GiB: nothing is rejected here
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.Exec("CREATE TABLE u (id uuid primary key, n int, body text, INDEX (body))")
	db.Exec("CREATE TABLE msgs (id uuid primary key, thread uuid partition key, body text)")

	for i := 0; i < 400; i++ {
		db.Exec("INSERT INTO u (id, n, body) VALUES (?, ?, ?)", tid(i), i%5, strings.Repeat("x", 10))
		db.Exec("INSERT INTO msgs (id, thread, body) VALUES (?, ?, ?)", tid(10000+i), tid(i%8), "m")
	}
	db.Exec("UPDATE u SET body = ? WHERE id = ?", strings.Repeat("y", 200), tid(0)) // grow
	db.Exec("UPDATE u SET body = ? WHERE n = ?", "", 2)                             // shrink (scan)
	db.Exec("UPDATE msgs SET body = ? WHERE id = ?", strings.Repeat("p", 50), tid(10000))
	for i := 0; i < 100; i++ {
		db.Exec("DELETE FROM u WHERE id = ?", tid(i))
	}

	if total, walk := db.budget.total.Load(), budgetTotal(db); total != walk {
		t.Fatalf("budget total %d != sum of shard tallies %d", total, walk)
	}
}

// The ceiling holds under concurrent inserts racing the last free bytes: the
// reserve never admits MORE than the cap (the safety guarantee), and the
// Add-then-back-out may conservatively waste a few rows of headroom but never
// the reverse. 8 goroutines each attempt `rows` distinct PKs into a cap sized for
// `rows` total.
func TestMaxBytesConcurrent(t *testing.T) {
	const rows = 2000
	db, err := Open(Options{MaxBytes: perIntRow() * rows, sizeHint: rows})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.Exec("CREATE TABLE t (id uuid primary key, n int)")

	var wg sync.WaitGroup
	var admitted int64
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < rows; i++ {
				_, err := db.Exec("INSERT INTO t (id, n) VALUES (?, ?)", tid(base*rows+i), i)
				if err == nil {
					atomic.AddInt64(&admitted, 1)
				} else if !errors.Is(err, ErrCapacity) {
					t.Errorf("unexpected error: %v", err)
				}
			}
		}(g)
	}
	wg.Wait()

	// Never over-admit (the hard guarantee), and stay close to the cap (boundary
	// waste is bounded by the count of concurrent inserters, far under 64).
	if admitted > rows {
		t.Fatalf("admitted=%d exceeds cap %d — over-admission", admitted, rows)
	}
	if admitted < rows-64 {
		t.Fatalf("admitted=%d, want close to %d (boundary waste should be tiny)", admitted, rows)
	}
	if got := int64(db.MetaSnapshot().TotalRows); got != admitted {
		t.Fatalf("rows=%d, want admitted=%d", got, admitted)
	}
	if got := db.budget.total.Load(); got != perIntRow()*admitted {
		t.Fatalf("budget total=%d, want %d (perRow*admitted)", got, perIntRow()*admitted)
	}
	if got := db.budget.total.Load(); got > perIntRow()*rows {
		t.Fatalf("budget total=%d exceeds cap %d", got, perIntRow()*rows)
	}
}

// multiInsertAutoPK builds an n-tuple auto-PK INSERT (id generated) and its args.
func multiInsertAutoPK(n int) (string, []any) {
	var b strings.Builder
	b.WriteString("INSERT INTO t (n) VALUES ")
	args := make([]any, 0, n)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("(?)")
		args = append(args, i)
	}
	return b.String(), args
}

// The byte cap and the tally must cover the multi-row INSERT / transaction path
// (txInsertLocked etc.), not just single-row inserts — that path bypassed both
// before. A batch that fits is admitted and accounted; a batch that would exceed
// the cap is rejected with ErrCapacity, applying nothing.
func TestMaxBytesCoversBatchInsert(t *testing.T) {
	const fit = 5
	db, err := Open(Options{MaxBytes: perIntRow() * fit})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.Exec("CREATE TABLE t (id uuid primary key, n int)")

	sql, args := multiInsertAutoPK(fit)
	if _, err := db.Exec(sql, args...); err != nil {
		t.Fatalf("batch of %d should fit: %v", fit, err)
	}
	// Tally + budget must reflect the batch (this path used to update neither).
	if got := db.MetaSnapshot().TotalApproxBytes; got != perIntRow()*fit {
		t.Fatalf("after batch: total_approx_bytes=%d, want %d", got, perIntRow()*fit)
	}
	if total, walk := db.budget.total.Load(), budgetTotal(db); total != walk {
		t.Fatalf("budget total %d != tally %d after batch", total, walk)
	}

	// A second batch would push past the cap → rejected, nothing applied.
	sql2, args2 := multiInsertAutoPK(2)
	if _, err := db.Exec(sql2, args2...); !errors.Is(err, ErrCapacity) {
		t.Fatalf("over-cap batch: got %v, want ErrCapacity", err)
	}
	if got := db.MetaSnapshot().TotalRows; got != fit {
		t.Fatalf("after rejected batch: rows=%d, want %d", got, fit)
	}
}

// BenchmarkInsert_Parallel_Capped mirrors BenchmarkInsert_Parallel_Mem but with
// the cap enabled (MaxBytes far above what the run inserts, so nothing is
// rejected) — it measures the cost of the reserve CAS on the shared budget under
// contention, the price an opt-in cap adds versus the uncapped path.
func BenchmarkInsert_Parallel_Capped(b *testing.B) {
	db, _ := Open(Options{Schema: benchSchema(), sizeHint: 2 * 1024 * 1024, MaxBytes: 1 << 40})
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		base := atomic.AddInt64(&counter, 1) * 100000
		i := int(base)
		for pb.Next() {
			db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "name", i%100)
			i++
		}
	})
}
