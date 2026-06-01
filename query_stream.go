package hazedb

import "fmt"

// Streaming reads — QueryEach / QueryJSON visit or encode result rows WITHOUT
// the []Row + per-row clone that Query/QueryValues build. For consumers that
// serialize each row immediately and discard it (the JSON/PHP adapters), the
// clone is pure waste: cloneValue copies BYTES and every row gets its own
// []Value slice, only to be re-encoded and thrown away.
//
// MEMORY-SAFETY CONTRACT. The row handed to a streaming callback is valid ONLY
// for the duration of that call. On the streamed (no-ORDER-BY) paths it aliases
// live arena storage and the callback runs under the row's shard read lock, so
// the callback MUST copy out anything it keeps (encode to bytes / a PHP zval)
// and MUST NOT retain the Row or its BYTES cells past return. Updates mutate the
// arena in place and BYTES backings are reused, so a retained alias is a
// use-after-free, not just staleness. Use Query when you need owned rows.

// selectEach runs a SELECT plan and calls visit once per result row, in order,
// honoring WHERE / projection / ORDER BY / LIMIT exactly like execSelect. visit
// returns false to stop early. Returns the result column names.
//
// No-ORDER-BY scan and indexed-equality reads STREAM: visit sees the live row
// (projected into a reused scratch — Value headers only, no clone) under its
// shard lock. ORDER BY, ordered-index walks, and PK lookups fall back to the
// materialized path and visit owned clones — they must buffer (to sort) or are
// trivially one row, so there is nothing to stream.
func (db *DB) selectEach(pl *plan, args []Value, visit func(row Row) bool) ([]string, error) {
	st := pl.st.(*selectStmt)
	tbl := pl.rt
	colNames := pl.colNames

	if pl.orderOrdinal >= 0 || pl.orderWalk || pl.pkLookup || pl.joinPlan != nil {
		_, rows, err := db.execSelect(pl, args)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			if !visit(r) {
				break
			}
		}
		return colNames, nil
	}
	if st.limit == 0 {
		return colNames, nil
	}

	ctx := evalCtx{cols: tbl.def.colByName, args: args}
	var scratch Row
	if !st.starAll {
		scratch = make(Row, len(pl.projOrdinals))
	}
	n := 0
	skipped := 0
	// consume reads a LIVE row under its shard lock: WHERE-filter, skip the first
	// OFFSET matches, project into the reused scratch (Value headers only — no
	// clone, valid for this call only), hand to visit, apply LIMIT. A WHERE-eval
	// error skips the row, as in the materialized scan. Returns true to STOP.
	consume := func(r Row) bool {
		if st.where != nil {
			ctx.row = r
			v, err := evalExpr(st.where, &ctx)
			if err != nil || !truthy(v) {
				return false
			}
		}
		if skipped < st.offset { // skip the first offset matched rows
			skipped++
			return false
		}
		row := r
		if !st.starAll {
			for j, ord := range pl.projOrdinals {
				scratch[j] = r[ord]
			}
			row = scratch
		}
		if !visit(row) {
			return true
		}
		n++
		return st.limit >= 0 && n >= st.limit
	}

	// Indexed equality: visit each candidate's live row under its shard lock
	// (locked per row, like offerLiveRow).
	if pl.idxLookup {
		cand, ok, err := db.idxCandidates(pl, &ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			return colNames, nil
		}
		cand.emit(func(pk UUID) bool {
			s := tbl.shardOf(pk)
			s.mu.RLock()
			stop := false
			if rowID, ok := s.pk[pk]; ok {
				if r := s.rows[rowID]; r != nil {
					stop = consume(r)
				}
			}
			s.mu.RUnlock()
			return stop
		})
		return colNames, nil
	}

	// Scan: scanAll / scanPartition call the callback under each shard's read
	// lock, so consume runs on live rows. (A pinned partition scans only its
	// rows; otherwise every shard.)
	scan := tbl.scanAll
	if pl.partLookup {
		pv, err := evalExpr(pl.partSource, &ctx)
		if err != nil {
			return nil, err
		}
		if pv.IsNull() {
			return colNames, nil
		}
		part, err := coerceToUUID(pv)
		if err != nil {
			return nil, err
		}
		scan = func(fn func(Row) bool) { tbl.scanPartition(part, fn) }
	}
	scan(func(r Row) bool { return !consume(r) })
	return colNames, nil
}

// QueryEach runs a SELECT and calls visit(cols, row) once per result row, in
// order. It is the low-allocation counterpart to QueryValues for consumers that
// serialize each row immediately (e.g. building a PHP array). visit returns
// false to stop early. cols is the same slice on every call.
//
// The row obeys the streaming memory-safety contract above: valid only during
// the call; copy out anything kept. Use QueryValues when you need owned rows.
func (db *DB) QueryEach(sql string, args []Value, visit func(cols []string, row Row) bool) error {
	pl, err := db.prepare(sql, db.cat.Load())
	if err != nil {
		return err
	}
	if _, ok := pl.st.(*selectStmt); !ok {
		return fmt.Errorf("fastsql: QueryEach used with non-SELECT — use Exec instead")
	}
	cols := pl.colNames
	_, err = db.selectEach(pl, args, func(row Row) bool { return visit(cols, row) })
	return err
}

// QueryJSON runs a SELECT and returns the result as a JSON array of objects
// ([{"col":val,...},...]) — the same shape as RowsToJSONObjects, but encoded
// straight from the live rows into one buffer, with no intermediate []Row and
// no per-row clone. The returned bytes are the caller's to keep or copy. For a
// read-and-forward path (an HTTP/JSON response) this avoids materializing rows
// only to re-encode and discard them.
func (db *DB) QueryJSON(sql string, args ...Value) ([]string, []byte, error) {
	return db.QueryJSONInto(make([]byte, 0, 256), sql, args...)
}

// QueryJSONInto is QueryJSON that appends into the caller-supplied buffer dst,
// returning the columns and the grown slice. Pass dst[:0] to reuse a pooled
// scratch buffer: a hot read-and-forward caller (the PHP/HTTP fetchall_json
// path) that keeps one buffer per worker and copies the result out immediately
// pays no per-call allocation for the JSON envelope — only the amortised growth
// to the high-water mark. The returned slice may share dst's backing array.
func (db *DB) QueryJSONInto(dst []byte, sql string, args ...Value) ([]string, []byte, error) {
	pl, err := db.prepare(sql, db.cat.Load())
	if err != nil {
		return nil, nil, err
	}
	if _, ok := pl.st.(*selectStmt); !ok {
		return nil, nil, fmt.Errorf("fastsql: QueryJSONInto used with non-SELECT — use Exec instead")
	}
	cols := pl.colNames
	prefix := pl.colJSONPrefix
	buf := append(dst, '[')
	first := true
	if _, err = db.selectEach(pl, args, func(row Row) bool {
		if !first {
			buf = append(buf, ',')
		}
		first = false
		buf = appendRowJSONObjectPre(buf, prefix, row)
		return true
	}); err != nil {
		return nil, nil, err
	}
	return cols, append(buf, ']'), nil
}
