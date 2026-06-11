package hazedb

import (
	"encoding/json"
	"strings"
	"testing"
	"unsafe"
)

// MetaSnapshot reports the table count and, per table, rows / columns / index
// count, and a size estimate that tracks payload weight: a table of 1 KB text
// rows must estimate far larger than a table of int rows, and DROP must drop the
// table from the overview.
func TestMetaSnapshot(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE small (id uuid primary key, n int)")
	db.Exec("CREATE TABLE big (id uuid primary key, body text, INDEX (body))")

	const n = 50
	big := strings.Repeat("x", 1000)
	for i := 0; i < n; i++ {
		if _, err := db.Exec("INSERT INTO small (id, n) VALUES (?, ?)", tid(i), i); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("INSERT INTO big (id, body) VALUES (?, ?)", tid(1000+i), big); err != nil {
			t.Fatal(err)
		}
	}

	m := db.MetaSnapshot()
	if m.Tables != 2 {
		t.Fatalf("Tables=%d, want 2", m.Tables)
	}
	stat := map[string]TableStat{}
	for _, ts := range m.TableStats {
		stat[ts.Name] = ts
	}

	if stat["small"].Rows != n || stat["big"].Rows != n {
		t.Fatalf("rows: small=%d big=%d, want %d each", stat["small"].Rows, stat["big"].Rows, n)
	}
	if stat["small"].Columns != 2 || stat["big"].Columns != 2 {
		t.Fatalf("columns: small=%d big=%d, want 2 each", stat["small"].Columns, stat["big"].Columns)
	}
	if stat["small"].Indexes != 0 || stat["big"].Indexes != 1 {
		t.Fatalf("indexes: small=%d big=%d, want 0 and 1", stat["small"].Indexes, stat["big"].Indexes)
	}

	// The 1 KB payload must dominate the estimate.
	if stat["big"].ApproxBytes <= stat["small"].ApproxBytes {
		t.Fatalf("big (%d) should estimate larger than small (%d)", stat["big"].ApproxBytes, stat["small"].ApproxBytes)
	}

	// Store-wide totals roll up both tables.
	if m.TotalRows != 2*n {
		t.Fatalf("TotalRows=%d, want %d", m.TotalRows, 2*n)
	}
	if want := stat["small"].ApproxBytes + stat["big"].ApproxBytes; m.TotalApproxBytes != want {
		t.Fatalf("TotalApproxBytes=%d, want %d", m.TotalApproxBytes, want)
	}
	if stat["big"].ApproxBytes < n*1000 {
		t.Fatalf("big estimate %d too low for %d×1KB rows", stat["big"].ApproxBytes, n)
	}

	// DROP removes the table from the overview.
	if _, err := db.Exec("DROP TABLE small"); err != nil {
		t.Fatal(err)
	}
	if m2 := db.MetaSnapshot(); m2.Tables != 1 {
		t.Fatalf("after DROP: Tables=%d, want 1", m2.Tables)
	}
}

// The byte size is now exact in the payload term, not sampled: for a known
// schema and string length it must equal the precise per-row formula (cells +
// fixed overhead), to the byte, regardless of row count — pins that a regression
// in the accounting is caught exactly. 300 rows is past the old 256-row sample
// window, so a reintroduced sample would diverge here.
func TestMetaExactBytes(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE t (id uuid primary key, body text)")
	const (
		rows   = 300
		bodyLn = 137
	)
	body := strings.Repeat("x", bodyLn)
	for i := 0; i < rows; i++ {
		if _, err := db.Exec("INSERT INTO t (id, body) VALUES (?, ?)", tid(i), body); err != nil {
			t.Fatal(err)
		}
	}
	// Per row: id (UUID = one Value) + body (one Value + bodyLn backing bytes) +
	// rowFixedOverhead; no secondary indexes.
	valSize := int(unsafe.Sizeof(Value{}))
	perRow := int64(valSize + (valSize + bodyLn) + rowFixedOverhead)
	want := perRow * rows
	if got := db.MetaSnapshot().TableStats[0].ApproxBytes; got != want {
		t.Fatalf("ApproxBytes=%d, want exact %d", got, want)
	}
}

// A deleted row must drop out of both the count and the byte total, and an empty
// table must report zero size.
func TestMetaSnapshotEmptyAndDelete(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE t (id uuid primary key, n int)")

	if ts := db.MetaSnapshot().TableStats[0]; ts.Rows != 0 || ts.ApproxBytes != 0 {
		t.Fatalf("empty table: rows=%d bytes=%d, want 0/0", ts.Rows, ts.ApproxBytes)
	}

	db.Exec("INSERT INTO t (id, n) VALUES (?, ?)", tid(1), 1)
	db.Exec("INSERT INTO t (id, n) VALUES (?, ?)", tid(2), 2)
	twoRows := db.MetaSnapshot().TableStats[0].ApproxBytes // tid(1), tid(2) live
	db.Exec("DELETE FROM t WHERE id = ?", tid(1))

	ts := db.MetaSnapshot().TableStats[0]
	if ts.Rows != 1 {
		t.Fatalf("after one delete: rows=%d, want 1", ts.Rows)
	}
	// Two int rows cost the same, so deleting one must halve the byte total.
	if ts.ApproxBytes != twoRows/2 {
		t.Fatalf("after delete: bytes=%d, want %d (half of %d)", ts.ApproxBytes, twoRows/2, twoRows)
	}
}

// MetaJSON is the wire shape the Caddy /meta route and the PHP hazedb_meta
// function both emit: it must round-trip back to the same StoreMeta and use the
// snake_case keys the adapters document.
func TestMetaJSON(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE t (id uuid primary key, body text, INDEX (body))")
	db.Exec("INSERT INTO t (id, body) VALUES (?, ?)", tid(1), "hello")

	raw := db.MetaJSON()

	// Snake_case keys are the documented contract — assert on the bytes, not just
	// the decoded struct, so a tag rename can't pass silently.
	for _, key := range []string{`"tables"`, `"total_rows"`, `"total_approx_bytes"`, `"table_stats"`, `"name"`, `"rows"`, `"columns"`, `"indexes"`, `"approx_bytes"`} {
		if !strings.Contains(string(raw), key) {
			t.Fatalf("MetaJSON missing key %s: %s", key, raw)
		}
	}

	var got StoreMeta
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("MetaJSON not valid JSON: %v", err)
	}
	if got.Tables != 1 || len(got.TableStats) != 1 {
		t.Fatalf("decoded %d tables / %d stats, want 1/1", got.Tables, len(got.TableStats))
	}
	if ts := got.TableStats[0]; ts.Name != "t" || ts.Rows != 1 || ts.Columns != 2 || ts.Indexes != 1 {
		t.Fatalf("decoded stat = %+v", ts)
	}
	// The store-wide totals must equal the sum of the per-table lines.
	if got.TotalRows != 1 || got.TotalApproxBytes != got.TableStats[0].ApproxBytes {
		t.Fatalf("totals: rows=%d bytes=%d, want 1 and %d", got.TotalRows, got.TotalApproxBytes, got.TableStats[0].ApproxBytes)
	}
}

// reconcileBytes asserts every table's running byte tally (the per-shard
// counters) equals a full walk of its live rows using the same rowCost the
// counters use. A mutation path that forgets to adjust the tally shows up here
// as a mismatch — the safety net that lets the counter touch many call sites.
func reconcileBytes(t *testing.T, db *DB) {
	t.Helper()
	for _, rt := range db.cat.Load().byName {
		tbl := rt.table
		nIdx := len(tbl.indexes)
		var counter, walk int64
		for i := range tbl.shards {
			s := &tbl.shards[i]
			s.mu.RLock()
			counter += s.bytes
			for _, r := range s.rows {
				if r != nil {
					walk += rowCost(r, nIdx)
				}
			}
			s.mu.RUnlock()
		}
		if counter != walk {
			t.Fatalf("table %s: byte tally %d != full-walk %d", rt.name(), counter, walk)
		}
	}
}

// The per-shard byte tally must stay exact across every insert and delete path —
// PK, indexed-candidate, full-scan, and partitioned — measured against a
// full-walk oracle. (Size-changing UPDATEs join this once the update paths track
// the delta; this pins insert/delete.)
func TestByteTallyReconcilesInsertDelete(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE u (id uuid primary key, n int, body text, INDEX (body))")
	db.Exec("CREATE TABLE msgs (id uuid primary key, thread uuid partition key, body text)")
	for i := 0; i < 500; i++ {
		if _, err := db.Exec("INSERT INTO u (id, n, body) VALUES (?, ?, ?)", tid(i), i%10, strings.Repeat("x", i%50)); err != nil {
			t.Fatal(err)
		}
		db.Exec("INSERT INTO msgs (id, thread, body) VALUES (?, ?, ?)", tid(10000+i), tid(i%8), "m")
	}
	reconcileBytes(t, db)

	for i := 0; i < 100; i++ { // PK delete
		db.Exec("DELETE FROM u WHERE id = ?", tid(i))
	}
	reconcileBytes(t, db)

	db.Exec("DELETE FROM u WHERE body = ?", strings.Repeat("x", 7)) // indexed-candidate delete
	db.Exec("DELETE FROM u WHERE n = ?", 3)                         // full-scan delete (n not indexed)
	reconcileBytes(t, db)

	db.Exec("DELETE FROM msgs WHERE id = ?", tid(10000)) // partitioned PK delete
	db.Exec("DELETE FROM msgs WHERE body = ?", "m")      // partitioned full-scan delete
	reconcileBytes(t, db)
}

// Replay rebuilds rows through the same insert/delete paths, so the tally must be
// exact again after a reopen from the WAL — proves accounting lives low enough to
// ride recovery, not just live writes.
func TestByteTallyReconcilesAfterReopen(t *testing.T) {
	dir := t.TempDir()
	open := func() *DB {
		db, err := Open(Options{WALLevel: WALPeriodic, WALPath: dir})
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	db := open()
	db.Exec("CREATE TABLE u (id uuid primary key, body text)")
	for i := 0; i < 200; i++ {
		db.Exec("INSERT INTO u (id, body) VALUES (?, ?)", tid(i), strings.Repeat("y", i%40))
	}
	for i := 0; i < 50; i++ {
		db.Exec("DELETE FROM u WHERE id = ?", tid(i))
	}
	reconcileBytes(t, db)
	if err := db.FlushWAL(); err != nil {
		t.Fatal(err)
	}
	db.Close()

	db2 := open() // replays the WAL into memory via rt.insert / rt.deleteByPK
	defer db2.Close()
	reconcileBytes(t, db2)
	if got := db2.MetaSnapshot().TotalRows; got != 150 {
		t.Fatalf("after reopen TotalRows=%d, want 150", got)
	}
}

// BenchmarkMetaSnapshot sizes the snapshot read cost. It reads the per-shard
// running counters, so it is O(shards), independent of row count — /meta stays
// cheap no matter how large the store grows.
func BenchmarkMetaSnapshot(b *testing.B) {
	db := newBenchDB(b, 10000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = db.MetaSnapshot()
	}
}
