package hazedb

// Drain fidelity — exhaustive checks that the SQLite mirror reproduces the
// engine's in-memory state EXACTLY after a heavy insert/update/delete workload.
//
// Each variant drives a randomized but seeded (reproducible) workload through a
// "reference model" kept in lockstep in Go, then rotates + drains and asserts
// three-way equality: reference == engine == SQLite. The reference model is the
// independent oracle — it catches a bug that happened to corrupt BOTH the engine
// and the mirror in the same way, which an engine-vs-mirror check alone would miss.
//
// Shared helpers (renderVal/renderAny/engineRows/sqliteRows/compareMaps) live in
// drain_test.go. tid lives in db_test.go.

import (
	"math/rand"
	"path/filepath"
	"strings"
	"testing"
)

// itemsSchema is a wide table exercising every column type plus nullability:
// UUID PK, non-null INT, and nullable STRING / BYTES / BOOL / UUID.
func itemsSchema() Schema {
	return Schema{Tables: []TableDef{{
		Name: "items",
		Columns: []ColumnDef{
			{Name: "id", Type: TypeUUID, PK: true},
			{Name: "n", Type: TypeInt},
			{Name: "label", Type: TypeString, Nullable: true},
			{Name: "blob", Type: TypeBytes, Nullable: true},
			{Name: "flag", Type: TypeBool, Nullable: true},
			{Name: "ref", Type: TypeUUID, Nullable: true},
		},
	}}}
}

const (
	itemsInsertSQL = `INSERT INTO items (id, n, label, blob, flag, ref) VALUES (?, ?, ?, ?, ?, ?)`
	itemsSelect    = `SELECT id, n, label, blob, flag, ref FROM items`
	itemsSelectSQL = `SELECT "id","n","label","blob","flag","ref" FROM "items"`
)

// itemCols are the mutable (non-PK) columns by name; the row index of column
// itemCols[i] is i+1 (row[0] is the PK).
var itemCols = []string{"n", "label", "blob", "flag", "ref"}

// wl is a workload driver: it issues writes to the engine and mirrors every
// mutation into ref, so ref is always the exact expected current state.
type wl struct {
	db   *DB
	rng  *rand.Rand
	ref  map[UUID]Row // pk -> current row (column order)
	live []UUID       // pks currently present (for picking update/delete targets)
	next int          // monotonic PK source; never reused, even after delete

	nIns, nUpd, nDel int
}

func newWL(db *DB, seed int64) *wl {
	return &wl{db: db, rng: rand.New(rand.NewSource(seed)), ref: map[UUID]Row{}, next: 1}
}

func (w *wl) randStr() string {
	// A mix of empty, unicode, separator/quote chars, and random alphanumerics —
	// anything that round-trips as UTF-8 TEXT.
	switch w.rng.Intn(3) {
	case 0:
		s := []string{"", "a", "café", "日本語", "with space", "pipe|inside", `quote"x`, "tab\tend"}
		return s[w.rng.Intn(len(s))]
	default:
		const alpha = "abcdefghijklmnopqrstuvwxyz0123456789"
		b := make([]byte, w.rng.Intn(16))
		for i := range b {
			b[i] = alpha[w.rng.Intn(len(alpha))]
		}
		return string(b)
	}
}

func (w *wl) randBytes() []byte {
	b := make([]byte, w.rng.Intn(20)) // includes 0-length and arbitrary bytes (incl 0x00)
	for i := range b {
		b[i] = byte(w.rng.Intn(256))
	}
	return b
}

// genCol produces a fresh value for column index ci (0=n,1=label,2=blob,3=flag,
// 4=ref). n is never NULL (the column is non-nullable); the rest are sometimes.
func (w *wl) genCol(ci int) Value {
	switch ci {
	case 0:
		return Int(int64(w.rng.Intn(2_000_001) - 1_000_000)) // negatives + large
	case 1:
		if w.rng.Intn(7) == 0 {
			return Null()
		}
		return Str(w.randStr())
	case 2:
		if w.rng.Intn(7) == 0 {
			return Null()
		}
		return Bytes(w.randBytes())
	case 3:
		if w.rng.Intn(7) == 0 {
			return Null()
		}
		return Bool(w.rng.Intn(2) == 0)
	case 4:
		if w.rng.Intn(3) == 0 {
			return Null()
		}
		return UUIDVal(tid(1_000_000 + w.rng.Intn(1_000_000)))
	}
	return Null()
}

func (w *wl) insert(t *testing.T) {
	t.Helper()
	id := tid(w.next)
	w.next++
	row := Row{UUIDVal(id), w.genCol(0), w.genCol(1), w.genCol(2), w.genCol(3), w.genCol(4)}
	if _, err := w.db.ExecValues(itemsInsertSQL, row...); err != nil {
		t.Fatalf("insert: %v", err)
	}
	w.ref[id] = row
	w.live = append(w.live, id)
	w.nIns++
}

func (w *wl) update(t *testing.T) {
	t.Helper()
	if len(w.live) == 0 {
		return
	}
	id := w.live[w.rng.Intn(len(w.live))]
	k := 1 + w.rng.Intn(len(itemCols))
	perm := w.rng.Perm(len(itemCols))[:k]
	newRow := append(Row(nil), w.ref[id]...) // copy then mutate selected cols
	set := make([]string, 0, k)
	args := make([]Value, 0, k+1)
	for _, ci := range perm {
		nv := w.genCol(ci)
		set = append(set, itemCols[ci]+" = ?")
		args = append(args, nv)
		newRow[ci+1] = nv
	}
	args = append(args, UUIDVal(id))
	sql := "UPDATE items SET " + strings.Join(set, ", ") + " WHERE id = ?"
	if _, err := w.db.ExecValues(sql, args...); err != nil {
		t.Fatalf("update: %v", err)
	}
	w.ref[id] = newRow
	w.nUpd++
}

func (w *wl) delete(t *testing.T) {
	t.Helper()
	if len(w.live) == 0 {
		return
	}
	i := w.rng.Intn(len(w.live))
	id := w.live[i]
	if _, err := w.db.ExecValues("DELETE FROM items WHERE id = ?", UUIDVal(id)); err != nil {
		t.Fatalf("delete: %v", err)
	}
	delete(w.ref, id)
	w.live[i] = w.live[len(w.live)-1]
	w.live = w.live[:len(w.live)-1]
	w.nDel++
}

// step runs n random ops weighted toward inserts (so the table grows), with
// updates and deletes interleaved.
func (w *wl) step(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		switch r := w.rng.Intn(100); {
		case len(w.live) == 0 || r < 55:
			w.insert(t)
		case r < 85:
			w.update(t)
		default:
			w.delete(t)
		}
	}
}

// verify seals + drains everything, then asserts reference == engine == SQLite.
func (w *wl) verify(t *testing.T, label string) {
	t.Helper()
	if err := w.db.wal.flush(); err != nil {
		t.Fatalf("%s: rotate: %v", label, err)
	}
	if err := w.db.drainOnce(); err != nil {
		t.Fatalf("%s: drain: %v", label, err)
	}
	ref := refToMap(w.ref)
	eng := engineRows(t, w.db, itemsSelect)
	sq := sqliteRows(t, w.db.sq.sdb, itemsSelectSQL)
	compareMaps(t, label+" ref-vs-engine", ref, eng)
	compareMaps(t, label+" ref-vs-sqlite", ref, sq)
	compareMaps(t, label+" engine-vs-sqlite", eng, sq)
	t.Logf("%s: live=%d  inserts=%d updates=%d deletes=%d", label, len(w.ref), w.nIns, w.nUpd, w.nDel)
}

func refToMap(ref map[UUID]Row) map[string]string {
	out := make(map[string]string, len(ref))
	for _, r := range ref {
		parts := make([]string, len(r))
		for i, v := range r {
			parts[i] = renderVal(v)
		}
		out[parts[0]] = strings.Join(parts, "|")
	}
	return out
}

func openItemsDrainDB(t *testing.T, dir, sqPath string) *DB {
	t.Helper()
	db, err := Open(Options{
		Schema:     itemsSchema(),
		WALPath:    dir,
		SQLitePath: sqPath,
		// rotate manually in verify()
		drainInterval: -1, // drain manually in verify()
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

// --- Variants ---------------------------------------------------------------

// 1. High-volume insert-only: every inserted row must appear verbatim in SQLite.
func TestFidelity_InsertOnlyVolume(t *testing.T) {
	dir := t.TempDir()
	sqPath := filepath.Join(t.TempDir(), "m.db")
	db := openItemsDrainDB(t, dir, sqPath)
	defer db.Close()
	w := newWL(db, 1)
	for i := 0; i < 5000; i++ {
		w.insert(t)
	}
	w.verify(t, "insert-only")
}

// 2. Randomized insert/update/delete mix at volume — the general case.
func TestFidelity_RandomMix(t *testing.T) {
	dir := t.TempDir()
	sqPath := filepath.Join(t.TempDir(), "m.db")
	db := openItemsDrainDB(t, dir, sqPath)
	defer db.Close()
	w := newWL(db, 2)
	w.step(t, 8000)
	w.verify(t, "random-mix")
}

//  3. Update churn: the same rows are updated many times. SQLite is INSERT OR
//     REPLACE per record, so it must collapse to each row's FINAL value — never
//     a stale intermediate.
func TestFidelity_UpdateChurn(t *testing.T) {
	dir := t.TempDir()
	sqPath := filepath.Join(t.TempDir(), "m.db")
	db := openItemsDrainDB(t, dir, sqPath)
	defer db.Close()
	w := newWL(db, 3)
	for i := 0; i < 1000; i++ { // seed a fixed row set
		w.insert(t)
	}
	for i := 0; i < 10000; i++ { // hammer updates only
		w.update(t)
	}
	w.verify(t, "update-churn")
}

//  4. All types + NULLs: a small, dense pass that guarantees every type and a
//     NULL in every nullable column reaches the mirror at least once.
func TestFidelity_AllTypesAndNulls(t *testing.T) {
	dir := t.TempDir()
	sqPath := filepath.Join(t.TempDir(), "m.db")
	db := openItemsDrainDB(t, dir, sqPath)
	defer db.Close()
	w := newWL(db, 4)
	for i := 0; i < 600; i++ {
		w.insert(t)
		if i%3 == 0 {
			w.update(t)
		}
	}
	w.verify(t, "all-types")
}

//  5. Transactions: multi-mutation commits must drain with the same fidelity as
//     autocommit writes (the WAL records a TXN envelope; the drain replays it).
func TestFidelity_Transactions(t *testing.T) {
	dir := t.TempDir()
	sqPath := filepath.Join(t.TempDir(), "m.db")
	db := openItemsDrainDB(t, dir, sqPath)
	defer db.Close()
	w := newWL(db, 5)
	for b := 0; b < 300; b++ {
		// Each transaction inserts a few rows and updates one earlier row.
		ids := make([]UUID, 0, 4)
		rows := make([]Row, 0, 4)
		for j := 0; j < 1+w.rng.Intn(4); j++ {
			id := tid(w.next)
			w.next++
			row := Row{UUIDVal(id), w.genCol(0), w.genCol(1), w.genCol(2), w.genCol(3), w.genCol(4)}
			ids = append(ids, id)
			rows = append(rows, row)
		}
		var updID UUID
		var updRow Row
		haveUpd := len(w.live) > 0
		if haveUpd {
			updID = w.live[w.rng.Intn(len(w.live))]
			updRow = append(Row(nil), w.ref[updID]...)
			updRow[1] = w.genCol(0) // bump n
		}
		err := db.Transaction(func(tx *Tx) error {
			for k, id := range ids {
				r := rows[k]
				if _, err := tx.Exec(itemsInsertSQL, id, r[1].Int(), strOrNil(r[2]), bytesOrNil(r[3]), boolOrNil(r[4]), uuidOrNil(r[5])); err != nil {
					return err
				}
			}
			if haveUpd {
				if _, err := tx.Exec("UPDATE items SET n = ? WHERE id = ?", updRow[1].Int(), updID); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("txn %d: %v", b, err)
		}
		// Reflect the committed txn into the reference model.
		for k, id := range ids {
			w.ref[id] = rows[k]
			w.live = append(w.live, id)
			w.nIns++
		}
		if haveUpd {
			w.ref[updID] = updRow
			w.nUpd++
		}
	}
	w.verify(t, "transactions")
}

//  6. Incremental drain: rotate + drain between many batches, so the mirror is
//     assembled across dozens of sealed segments (each deleted after commit).
//     The final state must still be exact.
func TestFidelity_IncrementalDrain(t *testing.T) {
	dir := t.TempDir()
	sqPath := filepath.Join(t.TempDir(), "m.db")
	db := openItemsDrainDB(t, dir, sqPath)
	defer db.Close()
	w := newWL(db, 6)
	for batch := 0; batch < 20; batch++ {
		w.step(t, 600)
		if err := db.wal.flush(); err != nil {
			t.Fatalf("rotate: %v", err)
		}
		if err := db.drainOnce(); err != nil {
			t.Fatalf("drain: %v", err)
		}
	}
	w.verify(t, "incremental-drain")
}

//  7. Recovery round-trip: drain, close, reopen (load from SQLite + replay the
//     undrained tail), and confirm the recovered engine equals the reference.
//     Then keep writing on the recovered DB and verify again.
func TestFidelity_RecoveryRoundTrip(t *testing.T) {
	dir := t.TempDir()
	sqPath := filepath.Join(t.TempDir(), "m.db")
	db := openItemsDrainDB(t, dir, sqPath)
	w := newWL(db, 7)
	w.step(t, 4000)
	w.verify(t, "pre-restart")

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2 := openItemsDrainDB(t, dir, sqPath)
	defer db2.Close()
	w.db = db2 // continue on the recovered handle (live/ref survive in memory)

	// Recovered state (loaded from SQLite) must equal the reference exactly.
	compareMaps(t, "recovered ref-vs-engine", refToMap(w.ref), engineRows(t, db2, itemsSelect))
	t.Logf("recovery: %d rows reloaded from SQLite", len(w.ref))

	// And the engine must keep mirroring correctly after recovery.
	w.step(t, 3000)
	w.verify(t, "post-restart")
}

// --- typed-arg helpers for the transaction variant (tx.Exec takes ...any) ----

func strOrNil(v Value) any {
	if v.IsNull() {
		return nil
	}
	return v.Str()
}
func bytesOrNil(v Value) any {
	if v.IsNull() {
		return nil
	}
	return v.Bytes()
}
func boolOrNil(v Value) any {
	if v.IsNull() {
		return nil
	}
	return v.Bool()
}
func uuidOrNil(v Value) any {
	if v.IsNull() {
		return nil
	}
	return v.UUID()
}
