package hazedb

import "fmt"

// mutJournal journals one mutation for the PK-pinned live write lanes. It is
// passed BY VALUE through the store's journaled methods — a stack copy, so a
// WAL-on write builds no per-call closure (a func() error closure would heap-
// allocate one per insert/update/delete). The zero value (db nil) journals
// nothing: memory-only DBs and the replay path use it. The store invokes the
// op method under the shard lock, preserving journal-before-apply.
type mutJournal struct {
	db      *DB
	tableID uint16
	// UPDATE lane: the changed ordinals (plan-owned, read-only) and the PK
	// ordinal to read from the post-update row image.
	ords  []int
	pkOrd int
	// DELETE lane: the resolved PK cell (the UUID, never the raw arg).
	pkVal Value
}

// live reports whether mutations must be journaled. The update lanes use it to
// skip the save/revert bookkeeping when there is no WAL.
func (j mutJournal) live() bool { return j.db != nil }

// insert appends an opInsert record for row. No-op on the zero value.
func (j mutJournal) insert(row Row) error {
	if j.db == nil {
		return nil
	}
	bp := j.db.scratch.get()
	*bp = encodeInsertMutation(*bp, j.tableID, row)
	werr := j.db.wal.writeRecord(recMutation, *bp)
	j.db.scratch.put(bp)
	return werr
}

// update appends an opUpdate record carrying nr's PK + the j.ords cells.
func (j mutJournal) update(nr Row) error {
	if j.db == nil {
		return nil
	}
	bp := j.db.scratch.get()
	*bp = encodeUpdateMutation(*bp, j.tableID, nr[j.pkOrd], j.ords, nr)
	werr := j.db.wal.writeRecord(recMutation, *bp)
	j.db.scratch.put(bp)
	return werr
}

// delete appends an opDelete record for j.pkVal.
func (j mutJournal) delete() error {
	if j.db == nil {
		return nil
	}
	bp := j.db.scratch.get()
	*bp = encodeDeleteMutation(*bp, j.tableID, j.pkVal)
	werr := j.db.wal.writeRecord(recMutation, *bp)
	j.db.scratch.put(bp)
	return werr
}

// buildRowFromTmpl materialises one row from a tuple's plan-time cell template:
// copies params from args (with API-boundary string→UUID coercion + validation),
// drops pre-validated literals straight in, and auto-generates the PK when
// omitted. Omitted columns stay zero, which is KindNull. NOT NULL on omitted
// columns is enforced at plan time, not here. The resolved row is what both the
// single-statement path and the transaction path journal, so replay reproduces
// the exact same row (including any auto-generated UUID).
func (db *DB) buildRowFromTmpl(pl *plan, tmpl []insCell, args []Value) (Row, error) {
	return db.buildRowFromTmplInto(pl, tmpl, args, make(Row, len(pl.rt.def.def.Columns)))
}

// buildRowFromTmplInto is buildRowFromTmpl writing into a caller-provided row
// slice (len == column count, zeroed) instead of allocating one. A multi-row
// INSERT carves all its rows from a single backing []Value, so the batch costs
// one allocation instead of one per row.
func (db *DB) buildRowFromTmplInto(pl *plan, tmpl []insCell, args []Value, row Row) (Row, error) {
	tbl := pl.rt
	cols := tbl.def.def.Columns
	pkOrd := tbl.def.pkOrdinal
	pkProvided := false
	// Stack-allocated (never escapes); referenced only by the fallback
	// expression branch below. Declaring it unconditionally keeps args off the
	// heap — a lazily-assigned *evalCtx makes escape analysis spill args.
	ctx := &evalCtx{args: args}
	for i := range tmpl {
		c := &tmpl[i]
		ord := c.ord
		var v Value
		if c.arg >= 0 {
			if c.arg >= len(args) {
				return nil, fmt.Errorf("%w: param index %d out of range", ErrParamMismatch, c.arg)
			}
			v = args[c.arg]
		} else { // insCellExpr: arithmetic etc., evaluated per insert
			ev, err := evalExpr(c.expr, ctx)
			if err != nil {
				return nil, err
			}
			v = ev
		}
		col := cols[ord]
		// API-boundary coercion: a string destined for a UUID column is
		// parsed into a UUID — storage only ever sees [16]byte.
		if col.Type == TypeUUID && v.Kind == KindString {
			u, perr := ParseUUID(v.Str())
			if perr != nil {
				return nil, perr
			}
			v = UUIDVal(u)
		}
		if err := validateValue(col, v); err != nil {
			return nil, err
		}
		row[ord] = v
		if ord == pkOrd {
			pkProvided = true
		}
	}
	// Auto-generate the PK when omitted. The resolved UUID is placed in the
	// row before the WAL record is written, so replay reproduces it exactly.
	if !pkProvided {
		row[pkOrd] = UUIDVal(NewUUIDv7())
	}
	return row, nil
}

// buildInsertRow builds the row for a single-row INSERT (tuple 0); execInsert is
// its only caller. Multi-row INSERT and the transaction path build their per-tuple
// rows via buildRowFromTmpl directly.
func (db *DB) buildInsertRow(pl *plan, args []Value) (Row, error) {
	return db.buildRowFromTmpl(pl, pl.insertTmpl[0], args)
}

// execInsert builds the row(s) and appends. Returns the count and an error.
// A multi-row INSERT (VALUES with >1 tuple) commits all rows atomically as one
// transaction — a duplicate PK anywhere fails the whole statement.
func (db *DB) execInsert(pl *plan, args []Value) (int, error) {
	if len(pl.insertTmpl) > 1 {
		return db.execInsertBatch(pl, args)
	}
	tbl := pl.rt
	row, err := db.buildInsertRow(pl, args)
	if err != nil {
		return 0, err
	}
	// PK uniqueness + WAL append + apply run atomically under the shard
	// lock (see insertJournaled). Ordering matters: a duplicate PK must be
	// rejected before anything is journaled, and a WAL failure must abort
	// before the row is applied.
	var j mutJournal
	if db.wal != nil {
		j = mutJournal{db: db, tableID: tbl.tableID}
	}
	if err := tbl.insertJournaled(row, j); err != nil {
		return 0, err
	}
	return 1, nil
}

// execInsertBatch builds every VALUES tuple's row, then commits them as one
// atomic transaction: a single TXN WAL envelope, each touched shard locked
// once, and intra-batch + against-store PK uniqueness enforced under those
// locks. This amortises the per-row envelope/lock/bufio overhead a sequence of
// single-row inserts would each pay. Reuses the transaction commit machinery.
func (db *DB) execInsertBatch(pl *plan, args []Value) (int, error) {
	tbl := pl.rt
	if len(pl.insertTmpl) > maxTxnMutations {
		return 0, fmt.Errorf("%w: INSERT of %d rows exceeds the %d-row limit; split into smaller INSERTs",
			ErrBatchTooLarge, len(pl.insertTmpl), maxTxnMutations)
	}
	pkOrd := tbl.def.pkOrdinal
	// If the INSERT column list omits the PK, every row's PK is auto-generated
	// (UUIDv7) and thus unique — no intra-batch duplicate is possible — so commit
	// can skip the read-your-writes overlay and its per-row dup-check. The template
	// columns are identical across tuples, so tuple 0 decides it.
	autoPK := true
	for i := range pl.insertTmpl[0] {
		if pl.insertTmpl[0][i].ord == pkOrd {
			autoPK = false
			break
		}
	}
	// Carve every row from one backing slice: each row is an ncols-wide, capped
	// window into it, so the batch allocates the row storage once instead of per
	// row. Capped so an in-place cell write or a later append never bleeds into a
	// neighbouring row.
	ncols := len(tbl.def.def.Columns)
	backing := make([]Value, len(pl.insertTmpl)*ncols)
	staged := make([]stagedMut, len(pl.insertTmpl))
	for r := range pl.insertTmpl {
		off := r * ncols
		row, err := db.buildRowFromTmplInto(pl, pl.insertTmpl[r], args, backing[off:off+ncols:off+ncols])
		if err != nil {
			return 0, err
		}
		staged[r] = stagedMut{kind: opInsert, pk: row[pkOrd].UUID(), row: row}
	}
	tx := &Tx{db: db, rt: tbl, staged: staged, skipDup: autoPK}
	if err := tx.commit(); err != nil {
		return 0, err
	}
	return len(staged), nil
}

// --- execUpdate / execDelete: shared dispatch, locking & journaling ----------
//
// Both evaluate their per-row work, then dispatch on the WHERE shape:
//
//   - PK-pinned    one shard, mutated/tombstoned in place under its lock,
//                  journal-before-apply (the hot, allocation-free path).
//   - Index-pinned the candidate set (index bucket ∪ dirty overlay) re-checked
//                  against the FULL WHERE; dirtyTooDenseForScan falls back to a
//                  scan when the overlay would cost more than scanning it.
//   - Unpinned     every shard lock held across journal+apply, the batch written
//                  as ONE TXN envelope so the statement is atomic on WAL failure
//                  or crash (a one-shard-at-a-time form diverges on replay).
//
// The candidate re-check uses a direct ctx predicate, not rowMatcher's compiled
// closure: that closure captures ctx and would force execPlan's argument buffer
// onto the heap on every PK/indexed write. The full-scan fallback instead copies
// args into an owned slice and uses the compiled matcher. encode/journalAll are
// nil for a memory-only DB, and journaling is always performed by the store under
// the lock(s) — never as a side effect of the match predicate.

// execUpdate evaluates the SET values once, then dispatches on the WHERE shape.
func (db *DB) execUpdate(pl *plan, args []Value) (int, error) {
	st := pl.st.(*updateStmt)
	tbl := pl.rt
	ctx := &evalCtx{cols: tbl.def.colByName, args: args}

	// Common hot path: one-column PK update. Avoid materialising a []Value
	// SET buffer; compute and apply the single cell directly under the shard
	// lock.
	if pl.pkLookup && len(pl.updateOrdinals) == 1 {
		ord := pl.updateOrdinals[0]
		col := tbl.def.def.Columns[ord]
		computeOne := func(r Row) (Value, error) {
			ctx.row = r
			v, err := evalExpr(st.sets[0].val, ctx)
			if err != nil {
				return Value{}, err
			}
			if err := validateValue(col, v); err != nil {
				return Value{}, err
			}
			return v, nil
		}
		if !pl.setRowDependent {
			v, err := computeOne(nil)
			if err != nil {
				return 0, err
			}
			computeOne = func(Row) (Value, error) { return v, nil }
		}
		keyVal, err := evalExpr(pl.pkSource, &evalCtx{args: args})
		if err != nil {
			return 0, err
		}
		if keyVal.IsNull() {
			return 0, nil
		}
		var j mutJournal
		if db.wal != nil {
			j = mutJournal{db: db, tableID: tbl.tableID, ords: pl.updateOrdinals, pkOrd: tbl.def.pkOrdinal}
		}
		pk, err := coerceToUUID(keyVal)
		if err != nil {
			return 0, err
		}
		ok, err := tbl.updateByPKOneJournaled(pk, ord, computeOne, j)
		if err != nil {
			return 0, err
		}
		if ok {
			return 1, nil
		}
		return 0, nil
	}

	// SET right-hand sides evaluate into a reused buffer. evalSet validates
	// each result against its column. For constant SETs (no column ref) we
	// evaluate once up front and hand back the same buffer for every row
	// (allocation-free hot path); for row-dependent SETs (col = col - ?) we
	// re-evaluate per row with the row in context.
	buf := make([]Value, len(st.sets))
	cols := tbl.def.def.Columns
	evalSet := func(r Row) ([]Value, error) {
		ctx.row = r
		for i, a := range st.sets {
			v, err := evalExpr(a.val, ctx)
			if err != nil {
				return nil, err
			}
			if err := validateValue(cols[pl.updateOrdinals[i]], v); err != nil {
				return nil, err
			}
			buf[i] = v
		}
		return buf, nil
	}
	var compute func(Row) ([]Value, error)
	if pl.setRowDependent {
		compute = evalSet
	} else {
		if _, err := evalSet(nil); err != nil { // constant: evaluate once
			return 0, err
		}
		compute = func(Row) ([]Value, error) { return buf, nil }
	}

	// PK-pinned fast path (updateByPKJournaled).
	if pl.pkLookup {
		keyVal, err := evalExpr(pl.pkSource, &evalCtx{args: args})
		if err != nil {
			return 0, err
		}
		if keyVal.IsNull() {
			return 0, nil
		}
		var j mutJournal
		if db.wal != nil {
			j = mutJournal{db: db, tableID: tbl.tableID, ords: pl.updateOrdinals, pkOrd: tbl.def.pkOrdinal}
		}
		pk, err := coerceToUUID(keyVal)
		if err != nil {
			return 0, err
		}
		ok, err := tbl.updateByPKJournaled(pk, pl.updateOrdinals, compute, j)
		if err != nil {
			return 0, err
		}
		if ok {
			return 1, nil
		}
		return 0, nil
	}

	// encode/journalAll for the candidate + full-scan paths (nil = memory-only).
	var encode func(Row) []byte
	var journalAll func([][]byte) error
	if db.wal != nil {
		encode = func(nr Row) []byte {
			return encodeUpdateMutation(nil, tbl.tableID, nr[tbl.def.pkOrdinal], pl.updateOrdinals, nr)
		}
		journalAll = db.journalTxnBodies
	}
	// Index-pinned: re-check the candidate set against the full WHERE.
	if pl.idxLookup && !tbl.dirtyTooDenseForScan() {
		cand, ok, err := db.idxCandidates(pl, ctx)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, nil
		}
		// Use the index bucket directly when there is no dirty overlay; only
		// materialise + dedup the candidate set when dirty candidates may overlap it.
		pks := cand.pks
		if len(cand.dirty) > 0 {
			pks = nil
			cand.emit(func(pk UUID) bool { pks = append(pks, pk); return false })
		}
		// Re-check the full WHERE on each candidate with a direct ctx predicate.
		match := func(r Row) bool {
			if st.where == nil {
				return true
			}
			ctx.row = r
			v, err := evalExpr(st.where, ctx)
			return err == nil && truthy(v)
		}
		// Fast path — exactly one resolved candidate + single-column SET: update in
		// place, no full-row clone (the bulk of the cost), mirroring the PK
		// single-column path. This does NOT require a unique index; it activates
		// whenever the candidate set happens to hold a single row (the full WHERE is
		// still re-checked on it below).
		if len(pks) == 1 && len(pl.updateOrdinals) == 1 {
			ord := pl.updateOrdinals[0]
			computeOne := func(r Row) (Value, error) {
				v, cerr := compute(r)
				if cerr != nil {
					return Value{}, cerr
				}
				return v[0], nil
			}
			var j mutJournal
			if db.wal != nil {
				j = mutJournal{db: db, tableID: tbl.tableID, ords: pl.updateOrdinals, pkOrd: tbl.def.pkOrdinal}
			}
			return tbl.updateOneByCandidate(pks[0], ord, match, computeOne, j)
		}
		return tbl.updateByCandidates(pks, match, pl.updateOrdinals, compute, encode, journalAll)
	}
	// PK pinned inside an AND-chain with no index path: resolve the single PK and
	// re-check the full WHERE on it via the same candidate machinery, instead of
	// scanning. One candidate, so a single-column SET takes the update-in-place lane.
	if pl.pkProbe != nil {
		keyVal, err := evalExpr(pl.pkProbe, ctx)
		if err != nil {
			return 0, err
		}
		if keyVal.IsNull() {
			return 0, nil
		}
		pk, err := coerceToUUID(keyVal)
		if err != nil {
			return 0, err
		}
		match := func(r Row) bool {
			ctx.row = r
			v, err := evalExpr(st.where, ctx)
			return err == nil && truthy(v)
		}
		var j mutJournal
		if db.wal != nil {
			j = mutJournal{db: db, tableID: tbl.tableID, ords: pl.updateOrdinals, pkOrd: tbl.def.pkOrdinal}
		}
		if len(pl.updateOrdinals) == 1 {
			ord := pl.updateOrdinals[0]
			computeOne := func(r Row) (Value, error) {
				v, cerr := compute(r)
				if cerr != nil {
					return Value{}, cerr
				}
				return v[0], nil
			}
			return tbl.updateOneByCandidate(pk, ord, match, computeOne, j)
		}
		return tbl.updateByCandidates([]UUID{pk}, match, pl.updateOrdinals, compute, encode, journalAll)
	}
	// Full-scan fallback: compiled matcher over an owned args copy.
	matchArgs := make([]Value, len(args))
	copy(matchArgs, args)
	match := rowMatcher(st.where, &evalCtx{cols: tbl.def.colByName, args: matchArgs})
	return tbl.updateWhereAll(match, pl.updateOrdinals, compute, encode, journalAll)
}

// execDelete dispatches on the WHERE shape. deleteByPKJournaled's PK path also
// closes a getByPK→deleteByPK TOCTOU under the one shard lock.
func (db *DB) execDelete(pl *plan, args []Value) (int, error) {
	st := pl.st.(*deleteStmt)
	tbl := pl.rt
	ctx := &evalCtx{cols: tbl.def.colByName, args: args}

	// PK-pinned fast path (deleteByPKJournaled).
	if pl.pkLookup {
		keyVal, err := evalExpr(pl.pkSource, &evalCtx{args: args})
		if err != nil {
			return 0, err
		}
		if keyVal.IsNull() {
			return 0, nil
		}
		pk, err := coerceToUUID(keyVal)
		if err != nil {
			return 0, err
		}
		var j mutJournal
		if db.wal != nil {
			// journal the resolved UUID, not the raw arg
			j = mutJournal{db: db, tableID: tbl.tableID, pkVal: UUIDVal(pk)}
		}
		ok, err := tbl.deleteByPKJournaled(pk, j)
		if err != nil {
			return 0, err
		}
		if ok {
			return 1, nil
		}
		return 0, nil
	}

	// encode/journalAll for the candidate + full-scan paths (nil = memory-only).
	var encode func(Value) []byte
	var journalAll func([][]byte) error
	if db.wal != nil {
		encode = func(pk Value) []byte {
			return encodeDeleteMutation(nil, tbl.tableID, pk)
		}
		journalAll = db.journalTxnBodies
	}
	// Index-pinned: re-check the candidate set against the full WHERE.
	if pl.idxLookup && !tbl.dirtyTooDenseForScan() {
		// Re-check the full WHERE on each candidate with a direct ctx predicate.
		// Scoped to this branch so the PK and full-scan paths don't build it.
		match := func(r Row) bool {
			if st.where == nil {
				return true
			}
			ctx.row = r
			v, err := evalExpr(st.where, ctx)
			return err == nil && truthy(v)
		}
		// Single-row fast path: one indexed equality, no dirty overlay, exactly one
		// index hit → delete it directly (one shard lock, single MUTATION record like
		// the PK path), skipping the []UUID candidate slice + multi-shard machinery.
		// match still re-checks the full WHERE, so a residual conjunct is honoured.
		if len(pl.idxCols) == 1 && tbl.readDirtyCount.Load() == 0 {
			keyVal, err := evalExpr(pl.idxSrcs[0], ctx)
			if err != nil {
				return 0, err
			}
			if keyVal.IsNull() {
				return 0, nil
			}
			if si := tbl.indexFor(pl.idxCols[0]); si != nil {
				pk, found, one := si.lookupOne(keyOf(keyVal))
				if found && one {
					var j mutJournal
					if db.wal != nil {
						j = mutJournal{db: db, tableID: tbl.tableID, pkVal: UUIDVal(pk)}
					}
					ok, err := tbl.deleteOneByCandidate(pk, match, j)
					if err != nil {
						return 0, err
					}
					if ok {
						return 1, nil
					}
					return 0, nil
				}
				if !found {
					return 0, nil // no index hit and no dirty overlay → nothing matches
				}
				// found && !one: multi-hit bucket → fall through to the candidate path.
			}
		}
		cand, ok, err := db.idxCandidates(pl, ctx)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, nil
		}
		pks := cand.pks
		if len(cand.dirty) > 0 {
			pks = nil
			cand.emit(func(pk UUID) bool { pks = append(pks, pk); return false })
		}
		return tbl.deleteByCandidates(pks, match, encode, journalAll)
	}
	// PK pinned inside an AND-chain with no index path: resolve the single PK and
	// re-check the full WHERE on it (deleteOneByCandidate honours the residual),
	// instead of scanning.
	if pl.pkProbe != nil {
		keyVal, err := evalExpr(pl.pkProbe, ctx)
		if err != nil {
			return 0, err
		}
		if keyVal.IsNull() {
			return 0, nil
		}
		pk, err := coerceToUUID(keyVal)
		if err != nil {
			return 0, err
		}
		match := func(r Row) bool {
			ctx.row = r
			v, err := evalExpr(st.where, ctx)
			return err == nil && truthy(v)
		}
		var j mutJournal
		if db.wal != nil {
			j = mutJournal{db: db, tableID: tbl.tableID, pkVal: UUIDVal(pk)}
		}
		ok, err := tbl.deleteOneByCandidate(pk, match, j)
		if err != nil {
			return 0, err
		}
		if ok {
			return 1, nil
		}
		return 0, nil
	}
	// Full-scan fallback: compiled matcher over an owned args copy.
	matchArgs := make([]Value, len(args))
	copy(matchArgs, args)
	match := rowMatcher(st.where, &evalCtx{cols: tbl.def.colByName, args: matchArgs})
	return tbl.deleteWhereAll(match, encode, journalAll)
}
