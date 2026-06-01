package hazedb

import "fmt"

// Transactions — v1 scope (deliberately narrow; see the RFC "Transactions"
// section). A transaction is a Go closure handed a *Tx; statements are staged,
// not applied, until the closure returns nil, at which point all staged
// mutations commit atomically under one TXN WAL envelope.
//
//   - tx.Exec only — no tx.Query. v1 transactions are write-only at the API,
//     so user code cannot read committed rows and branch on them (that is the
//     lost-update trap). Read-modify-write is expressed as arithmetic SET
//     (balance = balance - ?), evaluated under the commit locks.
//   - PK-pinned writes only (WHERE id = ?). The affected rows — and thus the
//     table — are known up front; no predicate re-evaluation at commit.
//   - Single table per transaction.
//   - Staged overlay gives read-your-writes: statement N sees 1..N-1.
//   - Poison-on-first-error: once any tx.Exec errors, later tx.Exec is a no-op
//     returning the sticky error, and the commit is forced to roll back even
//     if the closure returns nil.
//
// "Write-only" forbids user reads, not engine reads: arithmetic SET and
// uniqueness/existence checks DO read live rows — under the commit locks, at
// commit time, so the values journaled are the true committed-time results and
// replay is deterministic.

// Tx is the transaction handle passed to the db.Transaction closure.
type Tx struct {
	db     *DB
	cat    *catalog // snapshot taken at tx start; plans bind against it
	rt     *tableRT // the single table; set on the first staged statement
	staged []stagedMut
	err    error // sticky: the first tx.Exec error poisons the whole tx
}

// stagedMut is one buffered statement. INSERT carries the fully resolved row
// (PK auto-generated if omitted). UPDATE carries the plan + args so the SET
// expressions can be (re-)evaluated against the locked live/overlay row at
// commit. DELETE carries only the PK.
type stagedMut struct {
	kind uint8 // opInsert / opUpdate / opDelete
	pk   UUID
	row  Row     // opInsert
	pl   *plan   // opUpdate
	args []Value // opUpdate
}

// Transaction runs fn as a transaction. Returning nil from fn commits all
// staged statements atomically; returning an error (or any tx.Exec having
// failed) discards everything — nothing is written to the store or the WAL.
func (db *DB) Transaction(fn func(*Tx) error) error {
	tx := &Tx{db: db, cat: db.cat.Load()}
	cerr := fn(tx)
	if tx.err != nil {
		return tx.err // poisoned by a failed tx.Exec → roll back regardless of cerr
	}
	if cerr != nil {
		return cerr // explicit rollback
	}
	return tx.commit()
}

// Exec stages one INSERT/UPDATE/DELETE. It does not touch the live store. The
// returned count is the optimistic affected-row count (1); the real effect is
// decided at commit. A poisoned transaction no-ops and returns the sticky error.
func (tx *Tx) Exec(sql string, args ...any) (int, error) {
	if tx.err != nil {
		return 0, tx.err
	}
	n, err := tx.stage(sql, args...)
	if err != nil {
		tx.err = err // poison
		return 0, err
	}
	return n, nil
}

func (tx *Tx) stage(sql string, args ...any) (int, error) {
	pl, err := tx.db.prepare(sql, tx.cat)
	if err != nil {
		return 0, err
	}
	switch pl.st.(type) {
	case *selectStmt:
		return 0, fmt.Errorf("%w: SELECT (transactions are write-only — use db.Query outside the tx)", ErrTxUnsupported)
	case *createStmt, *dropStmt:
		return 0, fmt.Errorf("%w: DDL (CREATE/DROP must run outside a transaction)", ErrTxUnsupported)
	}
	// One table per transaction.
	if tx.rt == nil {
		tx.rt = pl.rt
	} else if tx.rt.tableID != pl.rt.tableID {
		return 0, fmt.Errorf("%w: spans two tables (%q and %q)", ErrTxUnsupported, tx.rt.name(), pl.rt.name())
	}
	vargs, err := toValues(args)
	if err != nil {
		return 0, err
	}
	switch pl.st.(type) {
	case *insertStmt:
		row, err := tx.db.buildInsertRow(pl, vargs)
		if err != nil {
			return 0, err
		}
		tx.staged = append(tx.staged, stagedMut{kind: opInsert, pk: row[pl.rt.def.pkOrdinal].UUID(), row: row})
		return 1, nil
	case *updateStmt:
		if !pl.pkLookup {
			return 0, fmt.Errorf("%w: UPDATE must be PK-pinned (WHERE id = ?)", ErrTxUnsupported)
		}
		pk, err := tx.resolvePK(pl, vargs)
		if err != nil {
			return 0, err
		}
		tx.staged = append(tx.staged, stagedMut{kind: opUpdate, pk: pk, pl: pl, args: vargs})
		return 1, nil
	case *deleteStmt:
		if !pl.pkLookup {
			return 0, fmt.Errorf("%w: DELETE must be PK-pinned (WHERE id = ?)", ErrTxUnsupported)
		}
		pk, err := tx.resolvePK(pl, vargs)
		if err != nil {
			return 0, err
		}
		tx.staged = append(tx.staged, stagedMut{kind: opDelete, pk: pk})
		return 1, nil
	}
	return 0, fmt.Errorf("internal: unhandled statement type in transaction")
}

// resolvePK evaluates the PK-equality source of a pinned UPDATE/DELETE.
func (tx *Tx) resolvePK(pl *plan, args []Value) (UUID, error) {
	v, err := evalExpr(pl.pkSource, &evalCtx{args: args})
	if err != nil {
		return UUID{}, err
	}
	if v.IsNull() {
		return UUID{}, fmt.Errorf("%w: NULL primary key", ErrTxUnsupported)
	}
	return coerceToUUID(v)
}

// txAct is a resolved, ready-to-apply effect produced by the commit walk.
type txAct struct {
	kind uint8
	pk   UUID
	row  Row // opInsert: full row; opUpdate: full post-update row
}

// commit applies every staged mutation atomically. It holds the table's
// pkDirectory write lock (partitioned tables) and only the shards the
// transaction touches, in the global lock order (pkDirectory → shards
// ascending). Locking the touched subset rather than every shard is safe
// against the all-shard acquirers (updateWhereAll / predicate writes) because
// both acquire in ascending shard-index order, so no lock cycle can form.
//
// Walk the staged list in order against an overlay (read-your-writes,
// arithmetic SET evaluated against the locked live/overlay row), validate, then
// journal the whole group as ONE TXN envelope BEFORE applying — so a WAL
// failure aborts with nothing applied, and a committed envelope always replays
// cleanly.
func (tx *Tx) commit() error {
	if len(tx.staged) == 0 {
		return nil
	}
	t := tx.rt.table
	if t.pkDir != nil {
		t.pkDir.mu.Lock()
		defer t.pkDir.mu.Unlock()
	}
	// Lock only the touched shards, ascending (deadlock-safe). For partitioned
	// tables the shard set is read from the pkDirectory, which we hold — so the
	// set is stable across the commit. The stack buffer keeps the common
	// small-transaction case allocation-free.
	var shardBuf [16]uint32
	shards := tx.collectShards(t, shardBuf[:0])
	for _, idx := range shards {
		t.shards[idx].mu.Lock()
	}
	defer func() {
		for i := len(shards) - 1; i >= 0; i-- {
			t.shards[shards[i]].mu.Unlock()
		}
	}()

	overlay := make(map[UUID]Row, len(tx.staged)) // present row, or nil = deleted in-tx
	// present resolves a PK against the overlay first, then the live store.
	present := func(pk UUID) (Row, bool) {
		if r, ok := overlay[pk]; ok {
			return r, r != nil
		}
		return t.txGetLocked(pk)
	}

	resolved := make([][]byte, 0, len(tx.staged))
	acts := make([]txAct, 0, len(tx.staged))
	pkOrd := t.def.pkOrdinal

	for _, m := range tx.staged {
		switch m.kind {
		case opInsert:
			if _, ok := present(m.pk); ok {
				return fmt.Errorf("%w: %v", ErrDuplicatePK, m.pk)
			}
			overlay[m.pk] = m.row
			resolved = append(resolved, encodeInsertMutation(nil, tx.rt.tableID, m.row))
			acts = append(acts, txAct{kind: opInsert, pk: m.pk, row: m.row})
		case opUpdate:
			cur, ok := present(m.pk)
			if !ok {
				continue // UPDATE of an absent row: 0 rows, not an error
			}
			nr := cur.Clone()
			vals, err := computeSets(m.pl, m.args, nr)
			if err != nil {
				return err
			}
			for i, ord := range m.pl.updateOrdinals {
				nr[ord] = vals[i]
			}
			overlay[m.pk] = nr
			resolved = append(resolved, encodeUpdateMutation(nil, tx.rt.tableID, nr[pkOrd], m.pl.updateOrdinals, nr))
			acts = append(acts, txAct{kind: opUpdate, pk: m.pk, row: nr})
		case opDelete:
			if _, ok := present(m.pk); !ok {
				continue // DELETE of an absent row: 0 rows, not an error
			}
			overlay[m.pk] = nil
			resolved = append(resolved, encodeDeleteMutation(nil, tx.rt.tableID, UUIDVal(m.pk)))
			acts = append(acts, txAct{kind: opDelete, pk: m.pk})
		}
	}
	if len(resolved) == 0 {
		return nil // every statement no-op'd
	}

	if tx.db.wal != nil {
		body := encodeTxn(tx.db.scratch.get(), resolved)
		werr := tx.db.wal.writeRecord(recTxn, body)
		tx.db.scratch.put(body)
		if werr != nil {
			return werr // nothing applied yet
		}
	}

	// Apply in statement order; an earlier INSERT is physically present before
	// a later UPDATE/DELETE of the same PK references it.
	for _, a := range acts {
		switch a.kind {
		case opInsert:
			t.txInsertLocked(a.row)
		case opUpdate:
			t.txReplaceLocked(a.pk, a.row)
		case opDelete:
			t.txDeleteLocked(a.pk)
		}
	}
	return nil
}

// collectShards appends the distinct shard indices the staged mutations hit
// into out (a caller stack buffer), deduplicated and sorted ascending. For a
// non-partitioned table the shard is the PK's hash, known directly. For a
// partitioned table an INSERT routes by its row's PartitionKey value, while an
// UPDATE/DELETE routes by the current pkDirectory location (caller holds the
// directory lock, so the lookup is stable); an absent PK contributes no shard
// (the statement no-ops at apply). A later UPDATE of a row inserted earlier in
// the same tx needs no extra shard: it resolves to the same partition value,
// whose shard the INSERT already added.
//
// Dedup is a linear scan and the sort is an insertion sort — both trivial at
// the handful of shards a transaction touches, and allocation-free (no map, no
// sort.Slice closure) so a small transaction allocates nothing here.
func (tx *Tx) collectShards(t *table, out []uint32) []uint32 {
	for _, m := range tx.staged {
		var idx uint32
		switch {
		case t.pkDir == nil:
			idx = t.shardIdxOf(m.pk)
		case m.kind == opInsert:
			idx = t.shardIdxOf(m.row[t.def.partitionOrdinal].UUID())
		default:
			loc, found := t.pkDir.idx[m.pk]
			if !found {
				continue
			}
			idx = loc.shard
		}
		dup := false
		for _, x := range out {
			if x == idx {
				dup = true
				break
			}
		}
		if !dup {
			out = append(out, idx)
		}
	}
	for i := 1; i < len(out); i++ { // insertion sort, ascending (tiny n)
		v := out[i]
		j := i - 1
		for j >= 0 && out[j] > v {
			out[j+1] = out[j]
			j--
		}
		out[j+1] = v
	}
	return out
}

// computeSets evaluates an UPDATE's SET expressions against row (which carries
// the committed-time + in-transaction state), validating each against its
// column. Mirrors execUpdate's evalSet but takes the row explicitly.
func computeSets(pl *plan, args []Value, row Row) ([]Value, error) {
	st := pl.st.(*updateStmt)
	ctx := &evalCtx{cols: pl.table.colByName, row: row, args: args}
	cols := pl.table.def.Columns
	out := make([]Value, len(st.sets))
	for i, a := range st.sets {
		v, err := evalExpr(a.val, ctx)
		if err != nil {
			return nil, err
		}
		if err := validateValue(cols[pl.updateOrdinals[i]], v); err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// --- lock-free apply/read helpers for transaction commit ---
//
// commit holds the pkDirectory write lock (partitioned) and every shard lock,
// so these take NO lock themselves — they are the lock-free bodies of the
// corresponding store methods. Replace/Delete assume the PK exists (commit's
// overlay walk guarantees it).

// txGetLocked returns the live row for pk without cloning (caller is under all
// locks). The caller clones before mutating.
func (t *table) txGetLocked(pk UUID) (Row, bool) {
	if t.pkDir != nil {
		loc, ok := t.pkDir.idx[pk]
		if !ok {
			return nil, false
		}
		s := &t.shards[loc.shard]
		if loc.rowID >= uint64(len(s.rows)) {
			return nil, false
		}
		r := s.rows[loc.rowID]
		if r == nil || r[t.def.pkOrdinal].UUID() != pk {
			return nil, false
		}
		return r, true
	}
	s := t.shardOf(pk)
	rowID, ok := s.pk[pk]
	if !ok {
		return nil, false
	}
	r := s.rows[rowID]
	if r == nil {
		return nil, false
	}
	return r, true
}

// txInsertLocked appends a row and indexes it (per-table directory + tails for
// partitioned, per-shard pk map otherwise).
func (t *table) txInsertLocked(row Row) {
	if t.pkDir != nil {
		pk := row[t.def.pkOrdinal].UUID()
		part := row[t.def.partitionOrdinal].UUID()
		idx := t.shardIdxOf(part)
		s := &t.shards[idx]
		rowID := uint64(len(s.rows))
		s.rows = append(s.rows, row)
		s.live++
		s.tails[part] = append(s.tails[part], rowID)
		t.pkDir.idx[pk] = rowLocation{shard: idx, rowID: rowID}
		return
	}
	pk := row[t.def.pkOrdinal].UUID()
	s := t.shardOf(pk)
	rowID := uint64(len(s.rows))
	s.rows = append(s.rows, row)
	s.pk[pk] = rowID
	s.live++
	t.markDirtyLocked(s, pk)
}

// txReplaceLocked overwrites the row at pk with nr in place (PK + PartitionKey
// are immutable, so the location never changes).
func (t *table) txReplaceLocked(pk UUID, nr Row) {
	if t.pkDir != nil {
		loc := t.pkDir.idx[pk]
		t.shards[loc.shard].rows[loc.rowID] = nr
		return
	}
	s := t.shardOf(pk)
	s.rows[s.pk[pk]] = nr
	t.markDirtyLocked(s, pk)
}

// txDeleteLocked tombstones the row at pk and removes its index entry.
func (t *table) txDeleteLocked(pk UUID) {
	if t.pkDir != nil {
		loc := t.pkDir.idx[pk]
		s := &t.shards[loc.shard]
		s.rows[loc.rowID] = nil
		s.live--
		delete(t.pkDir.idx, pk)
		return
	}
	s := t.shardOf(pk)
	rowID := s.pk[pk]
	s.rows[rowID] = nil
	delete(s.pk, pk)
	s.live--
	t.markDelDirtyLocked(s, pk)
}
