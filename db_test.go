package hazedb

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// TestMain runs the whole test binary from a temp working directory. The
// companion defaults to "hazedb.db" in the working directory when there is no
// WAL, so this keeps those real files out of the repo (WAL tests get their own
// companion inside their per-test temp WALPath). No companion is ever in-memory.
func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "hazedb-test")
	if err != nil {
		panic(err)
	}
	if err := os.Chdir(tmp); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = os.RemoveAll(tmp)
	os.Exit(code)
}

// testSchema returns a small schema used across most tests. The PK is a
// UUID (hard requirement since M4); tests build deterministic ordered PKs
// with tid(n) so query strings and ORDER BY id semantics carry over from the
// old integer-PK tests unchanged.
func testSchema() Schema {
	return Schema{
		Tables: []TableDef{
			{
				Name: "users",
				Columns: []ColumnDef{
					{Name: "id", Type: TypeUUID, PK: true},
					{Name: "name", Type: TypeString},
					{Name: "age", Type: TypeInt},
					{Name: "active", Type: TypeBool, Nullable: true},
				},
			},
		},
	}
}

// tid builds a deterministic, byte-ordered, valid-v7 UUID from an int — the
// stand-in for the integer PKs the tests used before M4. Monotonic in n
// (n lives in the 48-bit timestamp field), so ORDER BY id == order by n.
func tid(n int) UUID {
	var u UUID
	u[0] = byte(n >> 40)
	u[1] = byte(n >> 32)
	u[2] = byte(n >> 24)
	u[3] = byte(n >> 16)
	u[4] = byte(n >> 8)
	u[5] = byte(n)
	u[6] = 0x70 // version 7
	u[8] = 0x80 // variant 10
	return u
}

func openMem(t *testing.T) *DB {
	t.Helper()
	db, err := Open(Options{Schema: testSchema()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func openDBWithWAL(t *testing.T) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	// drainInterval -1 disables the background drain so these WAL tests keep their
	// segments on disk (several inspect or tamper with them); recovery still works
	// via WAL replay. The file companion default is exercised by companion_test.go.
	db, err := Open(Options{Schema: testSchema(), WALPath: path, drainInterval: -1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, path
}

// walSegmentFile returns the active (highest-numbered) segment file inside a
// segmented WAL directory. Tests that corrupt or truncate raw WAL bytes target
// this rather than WALPath, which is now a directory of segments.
func walSegmentFile(t *testing.T, dir string) string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, "seg-*.wal"))
	if err != nil || len(m) == 0 {
		t.Fatalf("no WAL segment in %s: %v", dir, err)
	}
	return m[len(m)-1]
}

func TestInsertAndSelect(t *testing.T) {
	db := openMem(t)
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "bob", 25); err != nil {
		t.Fatalf("insert: %v", err)
	}

	cols, rows, err := db.Query("SELECT id, name, age FROM users")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(cols) != 3 || cols[0] != "id" || cols[1] != "name" || cols[2] != "age" {
		t.Errorf("cols: %v", cols)
	}
	if len(rows) != 2 {
		t.Fatalf("rows: got %d", len(rows))
	}
	sort.Slice(rows, func(i, j int) bool { c, _ := rows[i][0].Compare(rows[j][0]); return c < 0 })
	if rows[0][1].Str() != "alice" || rows[0][2].Int() != 30 {
		t.Errorf("row 0: %v", rows[0])
	}
	if rows[1][1].Str() != "bob" || rows[1][2].Int() != 25 {
		t.Errorf("row 1: %v", rows[1])
	}
}

func TestSelectStar(t *testing.T) {
	db := openMem(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	cols, rows, err := db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(cols) != 4 {
		t.Fatalf("cols: %v", cols)
	}
	if len(rows) != 1 || rows[0][0].UUID() != tid(1) || rows[0][1].Str() != "alice" {
		t.Errorf("row: %v", rows[0])
	}
}

func TestSelectWhere(t *testing.T) {
	db := openMem(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "bob", 25)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(3), "carol", 40)

	_, rows, err := db.Query("SELECT id FROM users WHERE age > ?", 28)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows))
	}

	_, rows, err = db.Query("SELECT id FROM users WHERE name = ?", "alice")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 1 || rows[0][0].UUID() != tid(1) {
		t.Errorf("expected alice row, got %v", rows)
	}

	_, rows, err = db.Query("SELECT id FROM users WHERE age >= ? AND age <= ?", 25, 30)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("range: got %d rows", len(rows))
	}
}

func TestSelectOrderByLimit(t *testing.T) {
	db := openMem(t)
	for i, name := range []string{"alice", "bob", "carol", "dave"} {
		db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i+1), name, 20+i*5)
	}
	_, rows, err := db.Query("SELECT id, age FROM users ORDER BY age DESC LIMIT 2")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("limit: %d", len(rows))
	}
	if rows[0][1].Int() != 35 || rows[1][1].Int() != 30 {
		t.Errorf("order desc: got %v", rows)
	}

	_, rows, _ = db.Query("SELECT id, age FROM users ORDER BY age LIMIT 1")
	if len(rows) != 1 || rows[0][1].Int() != 20 {
		t.Errorf("order asc default: %v", rows)
	}
}

// LIMIT without ORDER BY takes the scan-and-stop path (stops at the limit,
// projects under the lock). Order is undefined, so assert only counts.
func TestSelectLimitNoOrderBy(t *testing.T) {
	db := openMem(t)
	for i := 0; i < 10; i++ {
		db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i+1), "u", 20+i)
	}
	cases := []struct {
		sql  string
		args []any
		want int
	}{
		{"SELECT id FROM users LIMIT 3", nil, 3},                      // stops early
		{"SELECT id FROM users LIMIT 100", nil, 10},                   // limit > rows → all
		{"SELECT id FROM users LIMIT 0", nil, 0},                      // empty
		{"SELECT id FROM users WHERE age >= ? LIMIT 2", []any{20}, 2}, // WHERE + LIMIT
	}
	for _, c := range cases {
		_, rows, err := db.Query(c.sql, c.args...)
		if err != nil {
			t.Fatalf("%q: %v", c.sql, err)
		}
		if len(rows) != c.want {
			t.Errorf("%q: got %d rows, want %d", c.sql, len(rows), c.want)
		}
	}
}

func TestSelectLimitRowsDoNotAppendAlias(t *testing.T) {
	db := openMem(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "a", 10)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "b", 20)

	_, rows, err := db.Query("SELECT age FROM users LIMIT 2")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows", len(rows))
	}
	second := rows[1][0]
	rows[0] = append(rows[0], Int(999))
	if !rows[1][0].Equal(second) {
		t.Fatalf("append to first row changed second row: %v", rows)
	}
}

// ages extracts column `col` from every row as int64s — a small oracle helper
// for OFFSET window assertions.
func ages(rows []Row, col int) []int64 {
	out := make([]int64, len(rows))
	for i, r := range rows {
		out[i] = r[col].Int()
	}
	return out
}

func eqInts(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// OFFSET on the ORDER BY paths (top-N heap when LIMIT is present, gather+sort
// when it is not). Deterministic order lets us assert exact contents.
func TestSelectOffsetOrderBy(t *testing.T) {
	db := openMem(t)
	for i, name := range []string{"alice", "bob", "carol", "dave", "erin"} {
		db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i+1), name, 20+i*5) // 20,25,30,35,40
	}
	cases := []struct {
		sql  string
		want []int64
	}{
		{"SELECT age FROM users ORDER BY age LIMIT 2 OFFSET 1", []int64{25, 30}},      // top-N + offset
		{"SELECT age FROM users ORDER BY age DESC LIMIT 2 OFFSET 1", []int64{35, 30}}, // desc
		{"SELECT age FROM users ORDER BY age OFFSET 2", []int64{30, 35, 40}},          // offset, no limit
		{"SELECT age FROM users ORDER BY age LIMIT 2 OFFSET 0", []int64{20, 25}},      // offset 0 == none
		{"SELECT age FROM users ORDER BY age LIMIT 10 OFFSET 4", []int64{40}},         // tail
		{"SELECT age FROM users ORDER BY age LIMIT 5 OFFSET 10", nil},                 // beyond end → empty
		{"SELECT age FROM users ORDER BY age LIMIT 0 OFFSET 1", nil},                  // limit 0 → empty
	}
	for _, c := range cases {
		_, rows, err := db.Query(c.sql)
		if err != nil {
			t.Fatalf("%q: %v", c.sql, err)
		}
		if got := ages(rows, 0); !eqInts(got, c.want) {
			t.Errorf("%q: got %v, want %v", c.sql, got, c.want)
		}
	}
}

// OFFSET on the no-ORDER-BY scan paths (scan-and-stop with LIMIT, gather without)
// and the PK fast path. Scan order is undefined, so assert the offset window is
// the matching slice of the full unoffset result, plus row counts.
func TestSelectOffsetNoOrderByAndPK(t *testing.T) {
	db := openMem(t)
	for i := 0; i < 10; i++ {
		db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i+1), "u", 20+i)
	}
	_, all, _ := db.Query("SELECT age FROM users") // scan order, stable for this table

	// OFFSET with no LIMIT == all[offset:].
	_, off, _ := db.Query("SELECT age FROM users OFFSET 4")
	if !eqInts(ages(off, 0), ages(all, 0)[4:]) {
		t.Errorf("OFFSET 4: got %v, want %v", ages(off, 0), ages(all, 0)[4:])
	}
	// LIMIT + OFFSET == all[offset:offset+limit].
	_, win, _ := db.Query("SELECT age FROM users LIMIT 3 OFFSET 5")
	if !eqInts(ages(win, 0), ages(all, 0)[5:8]) {
		t.Errorf("LIMIT 3 OFFSET 5: got %v, want %v", ages(win, 0), ages(all, 0)[5:8])
	}
	// OFFSET past the end → empty.
	if _, r, _ := db.Query("SELECT age FROM users LIMIT 3 OFFSET 100"); len(r) != 0 {
		t.Errorf("OFFSET 100: got %d rows, want 0", len(r))
	}

	// PK lookup: at most one row, so any OFFSET drops it; OFFSET 0 keeps it.
	if _, r, _ := db.Query("SELECT age FROM users WHERE id = ? OFFSET 1", tid(3)); len(r) != 0 {
		t.Errorf("PK OFFSET 1: got %d rows, want 0", len(r))
	}
	if _, r, _ := db.Query("SELECT age FROM users WHERE id = ? OFFSET 0", tid(3)); len(r) != 1 {
		t.Errorf("PK OFFSET 0: got %d rows, want 1", len(r))
	}
	if _, row, _ := db.QueryRow("SELECT age FROM users WHERE id = ? OFFSET 1", tid(3)); row != nil {
		t.Errorf("QueryRow PK OFFSET 1: got %v, want nil", row)
	}
}

// Streaming reads (QueryEach / QueryJSON) must honour OFFSET identically to the
// materialized Query, on both the scan and the no-ORDER-BY indexed paths.
func TestSelectOffsetStreaming(t *testing.T) {
	db := openMem(t)
	for i := 0; i < 8; i++ {
		db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i+1), "u", 20+i)
	}
	const sql = "SELECT age FROM users LIMIT 3 OFFSET 2"
	_, want, _ := db.Query(sql)

	var got []int64
	if err := db.QueryEach(sql, nil, func(_ []string, row Row) bool {
		got = append(got, row[0].Int())
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if !eqInts(got, ages(want, 0)) {
		t.Errorf("QueryEach OFFSET: got %v, want %v", got, ages(want, 0))
	}
}

// The parser accepts LIMIT ... OFFSET ... and bare OFFSET, and rejects a
// non-integer offset.
func TestParseOffset(t *testing.T) {
	db := openMem(t)
	sel := func(sql string) *selectStmt {
		t.Helper()
		pl, err := db.prepare(sql, db.cat.Load())
		if err != nil {
			t.Fatalf("prepare %q: %v", sql, err)
		}
		return pl.st.(*selectStmt)
	}
	if st := sel("SELECT id FROM users LIMIT 5 OFFSET 3"); st.offset != 3 || st.limit != 5 {
		t.Errorf("LIMIT 5 OFFSET 3: limit/offset = %d/%d, want 5/3", st.limit, st.offset)
	}
	if st := sel("SELECT id FROM users OFFSET 7"); st.offset != 7 || st.limit != -1 {
		t.Errorf("bare OFFSET: limit/offset = %d/%d, want -1/7", st.limit, st.offset)
	}
	if _, err := db.prepare("SELECT id FROM users OFFSET x", db.cat.Load()); err == nil {
		t.Error("OFFSET x should be a parse error")
	}
}

// A []byte passed to INSERT/UPDATE must be cloned at the write boundary, so a
// caller that mutates its slice after the call cannot corrupt stored state
// (which would also diverge from the already-written WAL record).
// QueryRow returns a single row (nil if none) without the []Row result slice.
func TestQueryRow(t *testing.T) {
	db := openMem(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "bob", 40)

	cols, row, err := db.QueryRow("SELECT name, age FROM users WHERE id = ?", tid(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 2 || row == nil || row[0].Str() != "alice" || row[1].Int() != 30 {
		t.Fatalf("PK hit: cols=%v row=%v", cols, row)
	}

	if _, row, err := db.QueryRow("SELECT name FROM users WHERE id = ?", tid(999)); err != nil || row != nil {
		t.Fatalf("PK miss should give nil row: row=%v err=%v", row, err)
	}

	if _, row, _ := db.QueryRow("SELECT * FROM users WHERE id = ?", tid(2)); row == nil || row[0].UUID() != tid(2) || row[1].Str() != "bob" {
		t.Fatalf("SELECT *: %v", row)
	}

	// Non-PK: first matching row.
	if _, row, err := db.QueryRow("SELECT name FROM users WHERE age > ? LIMIT 1", 25); err != nil || row == nil {
		t.Fatalf("non-PK: row=%v err=%v", row, err)
	}

	if _, _, err := db.QueryRow("INSERT INTO users (id) VALUES (?)", tid(3)); err == nil {
		t.Fatal("QueryRow on non-SELECT should error")
	}
}

func TestWriteClonesByteInput(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE b (id uuid primary key, data bytes)")
	id := tid(1)

	buf := []byte("hello")
	if _, err := db.Exec("INSERT INTO b (id, data) VALUES (?, ?)", id, buf); err != nil {
		t.Fatal(err)
	}
	buf[0] = 'X' // mutate the caller slice after the insert
	_, rows, _ := db.Query("SELECT data FROM b WHERE id = ?", id)
	if len(rows) != 1 || string(rows[0][0].Bytes()) != "hello" {
		t.Fatalf("insert aliased caller slice: got %q", rows[0][0].Bytes())
	}

	ubuf := []byte("world")
	db.Exec("UPDATE b SET data = ? WHERE id = ?", ubuf, id)
	ubuf[0] = 'Y' // mutate after the update
	_, rows, _ = db.Query("SELECT data FROM b WHERE id = ?", id)
	if string(rows[0][0].Bytes()) != "world" {
		t.Fatalf("update aliased caller slice: got %q", rows[0][0].Bytes())
	}
}

// The replay-apply mutator (update) must reject a mutate that changes the PK
// or returns nil, leaving the row + index intact. This is what turns a
// PK-changing WAL update record into ErrWALCorrupt on replay (caller maps the
// false return) instead of a silently corrupt index. (Not triggerable via the
// public API — the live plan rejects PK updates — so exercised directly.)
func TestReplayUpdateRejectsBadMutate(t *testing.T) {
	db := openMem(t)
	id := tid(1)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", id, "alice", 30)
	rt := db.cat.Load().byName["users"]
	pkOrd := rt.def.pkOrdinal
	ageOrd := rt.def.colByName["age"]

	if rt.update(id, func(r Row) Row { nr := r.Clone(); nr[pkOrd] = UUIDVal(tid(999)); return nr }) {
		t.Fatal("update accepted a PK-changing mutate")
	}
	if rt.update(id, func(Row) Row { return nil }) {
		t.Fatal("update accepted a nil result")
	}
	// Row untouched, still found under the original PK.
	_, rows, _ := db.Query("SELECT name FROM users WHERE id = ?", id)
	if len(rows) != 1 || rows[0][0].Str() != "alice" {
		t.Fatalf("rejected update corrupted the row: %v", rows)
	}
	// A legitimate non-PK mutate still applies.
	if !rt.update(id, func(r Row) Row { r[ageOrd] = Int(31); return r }) {
		t.Fatal("legitimate non-PK update was rejected")
	}
	if _, rows, _ := db.Query("SELECT age FROM users WHERE id = ?", id); rows[0][0].Int() != 31 {
		t.Fatalf("legitimate update did not apply: %v", rows)
	}
}

// A CRC-valid but tampered/wrong-typed WAL mutation must fail closed on replay:
// every cell is type-checked against the schema and every PK kind-checked before
// it reaches typed storage. Exercised directly (the live API can't produce a
// mistyped record), mirroring TestReplayUpdateRejectsBadMutate.
func TestReplayRejectsMistypedMutations(t *testing.T) {
	db := openMem(t)
	rt := db.cat.Load().byName["users"]
	ageOrd := rt.def.colByName["age"]

	corrupt := func(name string, err error) {
		t.Helper()
		if !errors.Is(err, ErrWALCorrupt) {
			t.Errorf("%s: got %v, want ErrWALCorrupt", name, err)
		}
	}

	// INSERT, non-UUID PK cell (id column gets an Int).
	corrupt("insert non-UUID pk",
		db.applyMutation(rt, opInsert, encodeRow(nil, Row{Int(1), Str("n"), Int(30), Bool(true)})))
	// INSERT, wrong-typed non-PK cell (name column gets an Int).
	corrupt("insert mistyped cell",
		db.applyMutation(rt, opInsert, encodeRow(nil, Row{UUIDVal(tid(1)), Int(5), Int(30), Bool(true)})))

	// UPDATE, non-UUID PK (SET cell valid).
	updPK := make(Row, 4)
	updPK[ageOrd] = Int(31)
	corrupt("update non-UUID pk",
		db.applyMutation(rt, opUpdate, encodeUpdateMutation(nil, rt.tableID, Int(1), []int{ageOrd}, updPK)[3:]))
	// UPDATE, wrong-typed SET cell (age column gets a String).
	updSet := make(Row, 4)
	updSet[ageOrd] = Str("oops")
	corrupt("update mistyped set cell",
		db.applyMutation(rt, opUpdate, encodeUpdateMutation(nil, rt.tableID, UUIDVal(tid(1)), []int{ageOrd}, updSet)[3:]))

	// DELETE, non-UUID PK.
	corrupt("delete non-UUID pk", db.applyMutation(rt, opDelete, encodeCell(nil, Int(1))))

	// Positive control: a well-formed insert still replays and is queryable.
	if err := db.applyMutation(rt, opInsert, encodeRow(nil, Row{UUIDVal(tid(2)), Str("alice"), Int(30), Bool(true)})); err != nil {
		t.Fatalf("valid insert rejected: %v", err)
	}
	if _, rows, _ := db.Query("SELECT name FROM users WHERE id = ?", tid(2)); len(rows) != 1 || rows[0][0].Str() != "alice" {
		t.Fatalf("valid replay insert not stored: %v", rows)
	}
}

// scanShardsBatched (the streaming join driver) must yield every surviving row
// exactly once even when the background compaction sweeper renumbers rowIDs
// between chunks. All rows are forced into ONE shard so the arena order is
// deterministic; mid-walk we delete the first arena slot and compact, pulling
// every later row down one index. An arena-index walk resumes at index 1 and
// skips the row now at index 0; a PK-snapshot walk still yields it.
func TestScanShardsBatchedSurvivesCompaction(t *testing.T) {
	db := openMem(t)
	rt := db.cat.Load().byName["users"]
	pkOrd := rt.def.pkOrdinal

	// 8 ids that all hash to one shard → a single arena in insert order.
	target := rt.shardIdxOf(tid(1))
	var ids []UUID
	for i := 1; len(ids) < 8; i++ {
		if rt.shardIdxOf(tid(i)) == target {
			ids = append(ids, tid(i))
		}
	}
	for k, id := range ids {
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", id, "u", k); err != nil {
			t.Fatal(err)
		}
	}

	seen := map[UUID]int{}
	compacted := false
	rt.scanShardsBatched(1, func(Row) bool { return true }, func(batch []Row) bool {
		for _, r := range batch {
			seen[r[pkOrd].UUID()]++
		}
		if !compacted { // after the first row, delete it and renumber the shard
			compacted = true
			if _, err := db.Exec("DELETE FROM users WHERE id = ?", ids[0]); err != nil {
				t.Fatal(err)
			}
			rt.compactShard(int(target))
		}
		return false
	})

	// Every id except the deleted ids[0] must be yielded exactly once.
	for _, id := range ids[1:] {
		if seen[id] != 1 {
			t.Errorf("id %v yielded %d times after compaction, want 1", id, seen[id])
		}
	}
}

// A PK-pinned UPDATE that touches no indexed column must NOT enter the dirty
// overlay (it leaves every index entry valid) — otherwise readDirtyCount grows
// per update and slows the next indexed lookup. An indexed-column update must
// still mark dirty. Covers both PK paths: single-column (updateByPKOneJournaled)
// and multi-column (updateByPKJournaled).
func TestNonIndexedPKUpdateSkipsDirty(t *testing.T) {
	db, err := Open(Options{
		Schema: Schema{Tables: []TableDef{{
			Name: "t",
			Columns: []ColumnDef{
				{Name: "id", Type: TypeUUID, PK: true},
				{Name: "name", Type: TypeString},
				{Name: "score", Type: TypeInt},
				{Name: "extra", Type: TypeInt},
			},
			Indexes: []IndexDef{{Name: "by_name", Columns: []string{"name"}}},
		}}},
		indexMergeInterval: -1, // keep the dirty overlay so the test can observe it
		compactInterval:    -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("INSERT INTO t (id, name, score, extra) VALUES (?, ?, ?, ?)", tid(1), "a", 1, 1); err != nil {
		t.Fatal(err)
	}
	rt := db.cat.Load().byName["t"]
	base := rt.readDirtyCount.Load()

	// Non-indexed updates: single column (updateByPKOneJournaled) then multi
	// column (updateByPKJournaled). Neither touches the name index.
	for i := 0; i < 3; i++ {
		if _, err := db.Exec("UPDATE t SET score=? WHERE id=?", i, tid(1)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := db.Exec("UPDATE t SET score=?, extra=? WHERE id=?", i, i, tid(1)); err != nil {
			t.Fatal(err)
		}
	}
	if got := rt.readDirtyCount.Load(); got != base {
		t.Errorf("non-indexed PK updates grew readDirtyCount %d -> %d, want unchanged", base, got)
	}

	// Indexed-column updates still mark dirty: single + multi column = +2.
	if _, err := db.Exec("UPDATE t SET name=? WHERE id=?", "b", tid(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE t SET name=?, score=? WHERE id=?", "c", 9, tid(1)); err != nil {
		t.Fatal(err)
	}
	if got := rt.readDirtyCount.Load(); got != base+2 {
		t.Errorf("indexed-column updates: readDirtyCount=%d, want %d", got, base+2)
	}

	// Same rule inside a transaction (txReplaceLocked).
	before := rt.readDirtyCount.Load()
	if err := db.Transaction(func(tx *Tx) error {
		_, e := tx.Exec("UPDATE t SET score=? WHERE id=?", 42, tid(1)) // non-indexed
		return e
	}); err != nil {
		t.Fatal(err)
	}
	if got := rt.readDirtyCount.Load(); got != before {
		t.Errorf("txn non-indexed update grew readDirtyCount %d -> %d, want unchanged", before, got)
	}
	if err := db.Transaction(func(tx *Tx) error {
		_, e := tx.Exec("UPDATE t SET name=? WHERE id=?", "z", tid(1)) // indexed
		return e
	}); err != nil {
		t.Fatal(err)
	}
	if got := rt.readDirtyCount.Load(); got != before+1 {
		t.Errorf("txn indexed update: readDirtyCount=%d, want %d", got, before+1)
	}
}

func TestUpdate(t *testing.T) {
	db := openMem(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "bob", 25)

	n, err := db.Exec("UPDATE users SET age = ? WHERE id = ?", 31, tid(1))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if n != 1 {
		t.Errorf("n=%d", n)
	}
	_, rows, _ := db.Query("SELECT age FROM users WHERE id = ?", tid(1))
	if rows[0][0].Int() != 31 {
		t.Errorf("got %v", rows[0][0])
	}

	n, _ = db.Exec("UPDATE users SET age = ?", 99)
	if n != 2 {
		t.Errorf("bulk update n=%d", n)
	}
}

func TestUpdatePKRefused(t *testing.T) {
	db := openMem(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	if _, err := db.Exec("UPDATE users SET id = ? WHERE id = ?", tid(9), tid(1)); err == nil {
		t.Error("expected error on PK update")
	}
}

func TestDelete(t *testing.T) {
	db := openMem(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "bob", 25)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(3), "carol", 40)

	n, err := db.Exec("DELETE FROM users WHERE age < ?", 30)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if n != 1 {
		t.Errorf("n=%d (expected 1: bob)", n)
	}

	_, rows, _ := db.Query("SELECT id FROM users")
	if len(rows) != 2 {
		t.Errorf("rows after delete: %d", len(rows))
	}

	n, _ = db.Exec("DELETE FROM users")
	if n != 2 {
		t.Errorf("full delete n=%d", n)
	}
}

func TestDuplicatePK(t *testing.T) {
	db := openMem(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice2", 31); err == nil {
		t.Error("expected duplicate PK error")
	}
}

func TestNullHandling(t *testing.T) {
	db := openMem(t)
	if _, err := db.Exec("INSERT INTO users (id, name, age, active) VALUES (?, ?, ?, ?)", tid(1), "alice", 30, nil); err != nil {
		t.Fatalf("insert null: %v", err)
	}
	_, rows, _ := db.Query("SELECT active FROM users WHERE active IS NULL")
	if len(rows) != 1 {
		t.Errorf("IS NULL: %d", len(rows))
	}

	db.Exec("UPDATE users SET active = ? WHERE id = ?", true, tid(1))
	_, rows, _ = db.Query("SELECT active FROM users WHERE active IS NOT NULL")
	if len(rows) != 1 || rows[0][0].Int() != 1 {
		t.Errorf("IS NOT NULL: %v", rows)
	}
}

func TestWALRoundTrip(t *testing.T) {
	db, path := openDBWithWAL(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "bob", 25)
	db.Exec("UPDATE users SET age = ? WHERE id = ?", 31, tid(1))
	db.Exec("DELETE FROM users WHERE id = ?", tid(2))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(Options{Schema: testSchema(), WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	_, rows, _ := db2.Query("SELECT id, age FROM users")
	if len(rows) != 1 || rows[0][0].UUID() != tid(1) || rows[0][1].Int() != 31 {
		t.Errorf("after replay: got %v", rows)
	}
}

func TestWALPartialTail(t *testing.T) {
	db, path := openDBWithWAL(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	db.Close()

	// Append garbage that looks like the start of a record but is
	// truncated. Replay must tolerate the dangling tail.
	f, err := os.OpenFile(walSegmentFile(t, path), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0x10, 0x00, 0x00, 0x00, 0x01}) // says len=16, but body is 1 byte
	f.Close()

	db2, err := Open(Options{Schema: testSchema(), WALPath: path})
	if err != nil {
		t.Fatalf("partial tail should be tolerated, got %v", err)
	}
	defer db2.Close()
	_, rows, _ := db2.Query("SELECT id FROM users")
	if len(rows) != 1 {
		t.Errorf("expected 1 surviving row, got %d", len(rows))
	}
}

func TestParseErrors(t *testing.T) {
	db := openMem(t)
	cases := []string{
		"SELECT FROM users",
		"INSERT INTO users (id) VALUES",
		"UPDATE users WHERE id = 1",
		"DELETE users",
		"SELECT bogus FROM users",
		"SELECT * FROM nonexistent",
	}
	for _, q := range cases {
		if _, _, err := db.Query(q); err == nil {
			if _, err := db.Exec(q); err == nil {
				t.Errorf("expected error for %q", q)
			}
		}
	}
}

// After Close, every public verb must fail with ErrClosed — a use-after-close
// cannot reach the torn-down WAL/companion. Covers a statement prepared before
// Close too (Stmt.bound checks closed). Second Close is a no-op.
func TestClosedRejectsAllVerbs(t *testing.T) {
	dir := t.TempDir()
	sqPath := filepath.Join(t.TempDir(), "m.db")
	db, err := Open(Options{Schema: testSchema(), WALPath: dir, CompanionPath: sqPath, drainInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "a", 1); err != nil {
		t.Fatal(err)
	}
	stmt, err := db.Prepare("SELECT id, name FROM users WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close should be a no-op, got %v", err)
	}

	want := func(name string, err error) {
		t.Helper()
		if !errors.Is(err, ErrClosed) {
			t.Errorf("%s after Close: got %v, want ErrClosed", name, err)
		}
	}

	_, e1 := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "b", 2)
	want("Exec", e1)
	_, e2 := db.ExecValues("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", UUIDVal(tid(3)), Str("c"), Int(3))
	want("ExecValues", e2)
	_, _, e3 := db.Query("SELECT id FROM users")
	want("Query", e3)
	_, _, e4 := db.QueryValues("SELECT id FROM users")
	want("QueryValues", e4)
	_, _, e5 := db.QueryRow("SELECT id FROM users WHERE id = ?", tid(1))
	want("QueryRow", e5)
	_, _, e6 := db.QueryRowValues("SELECT id FROM users WHERE id = ?", UUIDVal(tid(1)))
	want("QueryRowValues", e6)
	_, _, e7 := db.QueryRowJSONByPK(nil, "SELECT id, name FROM users WHERE id = ?", tid(1))
	want("QueryRowJSONByPK", e7)
	e8 := db.QueryEach("SELECT id FROM users", nil, func([]string, Row) bool { return true })
	want("QueryEach", e8)
	_, _, e9 := db.QueryJSON("SELECT id FROM users")
	want("QueryJSON", e9)
	want("FlushWAL", db.FlushWAL())
	_, e10 := db.Prepare("SELECT id FROM users")
	want("Prepare", e10)
	want("Transaction", db.Transaction(func(*Tx) error { return nil }))

	// Statement prepared before Close also rejects, via Stmt.bound.
	_, _, e11 := stmt.Query(tid(1))
	want("Stmt.Query", e11)
	_, _, e12 := stmt.QueryRow(tid(1))
	want("Stmt.QueryRow", e12)
	_, _, e13 := stmt.QueryRowByPK(tid(1), nil)
	want("Stmt.QueryRowByPK", e13)
}
