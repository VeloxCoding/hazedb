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
	tbl := pl.rt
	cols := tbl.def.def.Columns
	row := make(Row, len(cols)) // omitted columns stay zero == Null()
	pkOrd := tbl.def.pkOrdinal
	pkProvided := false
	// Stack-allocated (never escapes); referenced only by the fallback
	// expression branch below. Declaring it unconditionally keeps args off the
	// heap — a lazily-assigned *evalCtx makes escape analysis spill args.
	ctx := &evalCtx{args: args}
	for i := range tmpl {
		c := &tmpl[i]
		ord := c.ord
		if c.arg == insCellLit {
			row[ord] = c.lit // pre-validated + pre-coerced at plan time
			if ord == pkOrd {
				pkProvided = true
			}
			continue
		}
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

// buildInsertRow builds the row for a single-row INSERT (tuple 0). Transaction
// staging and the single-row exec path use it; multi-row INSERT iterates the
// per-tuple templates directly via buildRowFromTmpl.
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
	staged := make([]stagedMut, len(pl.insertTmpl))
	for r := range pl.insertTmpl {
		row, err := db.buildRowFromTmpl(pl, pl.insertTmpl[r], args)
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

// execUpdate evaluates the SET values once, then dispatches on the WHERE
// shape. A PK-pinned update hits exactly one shard and mutates in place
// under that shard's lock (hot path, allocation-free). An unpinned predicate
// update can span shards and goes through updateWhereAll, which holds every
// shard lock across the journal+apply so the WAL order and in-memory order
// stay identical — the one-shard-at-a-time form is a replay-divergence bug.
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

	// Fast path: PK equality — single shard, journal-before-apply under that
	// shard's lock (updateByPKJournaled). The zero journal for memory-only
	// keeps this hot path allocation-free.
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

	// Multi-shard predicate path: updateWhereAll collects every matched row's
	// new image under all shard locks, journals the batch as ONE TXN envelope,
	// then applies — so the statement is atomic (all-or-nothing on WAL failure
	// or crash). encode/journalAll are nil for a memory-only DB.
	var encode func(Row) []byte
	var journalAll func([][]byte) error
	if db.wal != nil {
		encode = func(nr Row) []byte {
			return encodeUpdateMutation(nil, tbl.tableID, nr[tbl.def.pkOrdinal], pl.updateOrdinals, nr)
		}
		journalAll = db.journalTxnBodies
	}
	// Secondary-index WHERE: update only the index candidates (∪ dirty), re-checked
	// against the full WHERE, instead of scanning every row. Under a heavy write
	// burst the dirty overlay can outgrow the table, making the candidate walk
	// cost more than a scan — dirtyTooDenseForScan falls through to updateWhereAll.
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
		// Re-check the full WHERE on each candidate with a direct predicate. The
		// candidate set is narrow, so the compiled scan matcher is not worth its
		// ctx-capturing closure — which (returned from rowMatcher) would force
		// execPlan's argument buffer onto the heap for every indexed write.
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
	// Full-scan fallback: the compiled matcher pays off over many rows. Copy the
	// args into an owned slice so the matcher's captured ctx does not drag
	// execPlan's fixed argument buffer onto the heap (which would penalise every
	// PK/indexed write that never reaches this path).
	matchArgs := make([]Value, len(args))
	copy(matchArgs, args)
	match := rowMatcher(st.where, &evalCtx{cols: tbl.def.colByName, args: matchArgs})
	return tbl.updateWhereAll(match, pl.updateOrdinals, compute, encode, journalAll)
}

// execDelete dispatches on the WHERE shape, mirroring execUpdate. A
// PK-pinned delete hits one shard. An unpinned predicate delete goes
// through deleteWhereAll, which holds every shard lock across journal+apply
// (the one-shard-at-a-time form diverges on replay). Journaling is done by
// the store under the locks — never as a side effect of the match predicate.
func (db *DB) execDelete(pl *plan, args []Value) (int, error) {
	st := pl.st.(*deleteStmt)
	tbl := pl.rt
	ctx := &evalCtx{cols: tbl.def.colByName, args: args}

	// Fast path: PK equality — single shard, journal-before-tombstone under
	// the shard lock (deleteByPKJournaled, which also closes a
	// getByPK→deleteByPK TOCTOU). nil journal for memory-only.
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

	// Multi-shard predicate path: deleteWhereAll collects matched PKs under all
	// shard locks, journals the batch as ONE TXN envelope, then tombstones — so
	// the statement is atomic. encode/journalAll are nil for a memory-only DB.
	var encode func(Value) []byte
	var journalAll func([][]byte) error
	if db.wal != nil {
		encode = func(pk Value) []byte {
			return encodeDeleteMutation(nil, tbl.tableID, pk)
		}
		journalAll = db.journalTxnBodies
	}
	// Secondary-index WHERE: delete only the index candidates (∪ dirty), re-checked
	// against the full WHERE, instead of scanning every row. Under a heavy write
	// burst the dirty overlay can outgrow the table, making the candidate walk
	// cost more than a scan — dirtyTooDenseForScan falls through to deleteWhereAll.
	if pl.idxLookup && !tbl.dirtyTooDenseForScan() {
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
		// Direct WHERE re-check (see execUpdate: a narrow candidate set does not
		// warrant the ctx-capturing compiled matcher that escapes the arg buffer).
		match := func(r Row) bool {
			if st.where == nil {
				return true
			}
			ctx.row = r
			v, err := evalExpr(st.where, ctx)
			return err == nil && truthy(v)
		}
		return tbl.deleteByCandidates(pks, match, encode, journalAll)
	}
	// Full-scan fallback: compiled matcher over an owned args copy, so the captured
	// ctx does not drag execPlan's argument buffer onto the heap (see execUpdate).
	matchArgs := make([]Value, len(args))
	copy(matchArgs, args)
	match := rowMatcher(st.where, &evalCtx{cols: tbl.def.colByName, args: matchArgs})
	return tbl.deleteWhereAll(match, encode, journalAll)
}
