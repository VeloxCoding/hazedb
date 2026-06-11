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

// BenchmarkMetaSnapshot sizes the exact (full-walk) snapshot's read cost — it is
// O(live rows), so this is the number that says whether /meta stays cheap enough
// for a dashboard. It runs on a read path; no write-path code is touched.
func BenchmarkMetaSnapshot(b *testing.B) {
	db := newBenchDB(b, 10000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = db.MetaSnapshot()
	}
}
