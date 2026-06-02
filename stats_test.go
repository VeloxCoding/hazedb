package hazedb

import (
	"strings"
	"testing"
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

// A row deleted after sampling must drop out of the row count, and an empty
// table must report zero size without dividing by zero.
func TestMetaSnapshotEmptyAndDelete(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE t (id uuid primary key, n int)")

	if ts := db.MetaSnapshot().TableStats[0]; ts.Rows != 0 || ts.ApproxBytes != 0 {
		t.Fatalf("empty table: rows=%d bytes=%d, want 0/0", ts.Rows, ts.ApproxBytes)
	}

	db.Exec("INSERT INTO t (id, n) VALUES (?, ?)", tid(1), 1)
	db.Exec("INSERT INTO t (id, n) VALUES (?, ?)", tid(2), 2)
	db.Exec("DELETE FROM t WHERE id = ?", tid(1))

	if ts := db.MetaSnapshot().TableStats[0]; ts.Rows != 1 {
		t.Fatalf("after one delete: rows=%d, want 1", ts.Rows)
	}
}
