package hazedb

import (
	"fmt"
	"sort"
)

// assignParamIndices walks the AST and replaces every paramRef.index
// = -1 with a running count, so positional args bind in source order.
// Called once after parse, before plan.
func assignParamIndices(st stmt) {
	var n int
	var walk func(expr) expr
	walk = func(e expr) expr {
		switch x := e.(type) {
		case nil:
			return nil
		case *paramRef:
			x.index = n
			n++
			return x
		case *binOp:
			x.lhs = walk(x.lhs)
			x.rhs = walk(x.rhs)
			return x
		case *notExpr:
			x.e = walk(x.e)
			return x
		case *isNullExpr:
			x.e = walk(x.e)
			return x
		}
		return e
	}
	switch s := st.(type) {
	case *selectStmt:
		s.where = walk(s.where)
	case *insertStmt:
		for i := range s.vals {
			s.vals[i] = walk(s.vals[i])
		}
	case *updateStmt:
		for i := range s.sets {
			s.sets[i].val = walk(s.sets[i].val)
		}
		s.where = walk(s.where)
	case *deleteStmt:
		s.where = walk(s.where)
	}
}

// plan resolves table/column names and validates them. Produces a
// minimal plan with the resolved table pointer + column ordinals
// baked in, so the runtime path does no map lookups.
type plan struct {
	st    stmt
	table *resolvedTable
	// SELECT projection: ordinals into the row, in output order. nil if
	// SELECT *.
	projOrdinals []int
	// SELECT ORDER BY: ordinal of the order column. -1 if none.
	orderOrdinal int
	// INSERT column ordinals matching the values list.
	insertOrdinals []int
	// UPDATE SET column ordinals matching the sets list.
	updateOrdinals []int
	// setRowDependent is true when any SET right-hand side references a
	// column (e.g. col = col - ?), so the value must be evaluated per row
	// instead of once up front.
	setRowDependent bool

	// pkLookup is true when the WHERE clause is a single equality of
	// the PK column against a literal or parameter. The executor can
	// then go straight through tableShard.getByPK and skip the full
	// scan. pkSource is the expression that yields the key.
	pkLookup bool
	pkSource expr
}

func (db *DB) plan(st stmt) (*plan, error) {
	pl := &plan{st: st, orderOrdinal: -1}
	tname := ""
	switch s := st.(type) {
	case *selectStmt:
		tname = s.table
	case *insertStmt:
		tname = s.table
	case *updateStmt:
		tname = s.table
	case *deleteStmt:
		tname = s.table
	}
	rt, ok := db.tables[tname]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownTable, tname)
	}
	pl.table = rt
	switch s := st.(type) {
	case *selectStmt:
		if !s.starAll {
			pl.projOrdinals = make([]int, 0, len(s.cols))
			for _, c := range s.cols {
				ord, ok := rt.colByName[c.col]
				if !ok {
					return nil, fmt.Errorf("%w: %q.%q", ErrUnknownColumn, tname, c.col)
				}
				pl.projOrdinals = append(pl.projOrdinals, ord)
			}
		}
		if s.orderCol != "" {
			ord, ok := rt.colByName[s.orderCol]
			if !ok {
				return nil, fmt.Errorf("%w: %q.%q (ORDER BY)", ErrUnknownColumn, tname, s.orderCol)
			}
			pl.orderOrdinal = ord
		}
		if err := validateExpr(s.where, rt); err != nil {
			return nil, err
		}
		if s.where != nil {
			if ok, src := detectPKEq(s.where, rt); ok {
				pl.pkLookup = true
				pl.pkSource = src
			}
		}
	case *insertStmt:
		pl.insertOrdinals = make([]int, 0, len(s.cols))
		for _, c := range s.cols {
			ord, ok := rt.colByName[c]
			if !ok {
				return nil, fmt.Errorf("%w: %q.%q (INSERT)", ErrUnknownColumn, tname, c)
			}
			pl.insertOrdinals = append(pl.insertOrdinals, ord)
		}
	case *updateStmt:
		pl.updateOrdinals = make([]int, 0, len(s.sets))
		for _, a := range s.sets {
			ord, ok := rt.colByName[a.col]
			if !ok {
				return nil, fmt.Errorf("%w: %q.%q (UPDATE SET)", ErrUnknownColumn, tname, a.col)
			}
			if ord == rt.pkOrdinal {
				return nil, fmt.Errorf("%w: %q.%q", ErrPKUpdate, tname, a.col)
			}
			if rt.def.Columns[ord].Immutable {
				return nil, fmt.Errorf("%w: %q.%q is immutable", ErrPKUpdate, tname, a.col)
			}
			pl.updateOrdinals = append(pl.updateOrdinals, ord)
			// Validate the right-hand side (catches unknown columns in
			// arithmetic like SET x = bogus - ?) and note row dependence.
			if err := validateExpr(a.val, rt); err != nil {
				return nil, err
			}
			if exprRefsColumn(a.val) {
				pl.setRowDependent = true
			}
		}
		if err := validateExpr(s.where, rt); err != nil {
			return nil, err
		}
		if s.where != nil {
			if ok, src := detectPKEq(s.where, rt); ok {
				pl.pkLookup = true
				pl.pkSource = src
			}
		}
	case *deleteStmt:
		if err := validateExpr(s.where, rt); err != nil {
			return nil, err
		}
		if s.where != nil {
			if ok, src := detectPKEq(s.where, rt); ok {
				pl.pkLookup = true
				pl.pkSource = src
			}
		}
	}
	return pl, nil
}

// detectPKEq returns (true, valueSide) when e is a single binOp =
// between the PK column and a literal/parameter expression. Returns
// (false, nil) otherwise. Walks across AND chains in a future pass —
// v1 only accepts the bare equality.
func detectPKEq(e expr, rt *resolvedTable) (bool, expr) {
	bop, ok := e.(*binOp)
	if !ok || bop.op != tkEq {
		return false, nil
	}
	pkName := rt.def.Columns[rt.pkOrdinal].Name
	if cr, ok := bop.lhs.(*colRef); ok && cr.name == pkName {
		if isLitOrParam(bop.rhs) {
			return true, bop.rhs
		}
	}
	if cr, ok := bop.rhs.(*colRef); ok && cr.name == pkName {
		if isLitOrParam(bop.lhs) {
			return true, bop.lhs
		}
	}
	return false, nil
}

func isLitOrParam(e expr) bool {
	switch e.(type) {
	case *litValue, *paramRef:
		return true
	}
	return false
}

// coerceToUUID turns a PK-lookup value into a UUID: a UUID passes through; a
// string is parsed (API-boundary convenience). Anything else is an error.
func coerceToUUID(v Value) (UUID, error) {
	switch v.Kind {
	case KindUUID:
		return v.U, nil
	case KindString:
		return ParseUUID(v.S)
	}
	return UUID{}, fmt.Errorf("%w: PK lookup expects UUID, got kind %d", ErrTypeMismatch, v.Kind)
}

// exprRefsColumn reports whether e reads any column (vs only literals and
// parameters). Used to decide if a SET value must be evaluated per row.
func exprRefsColumn(e expr) bool {
	switch x := e.(type) {
	case *colRef:
		return true
	case *binOp:
		return exprRefsColumn(x.lhs) || exprRefsColumn(x.rhs)
	case *notExpr:
		return exprRefsColumn(x.e)
	case *isNullExpr:
		return exprRefsColumn(x.e)
	}
	return false
}

func validateExpr(e expr, rt *resolvedTable) error {
	if e == nil {
		return nil
	}
	switch x := e.(type) {
	case *colRef:
		if _, ok := rt.colByName[x.name]; !ok {
			return fmt.Errorf("%w: %q.%q", ErrUnknownColumn, rt.def.Name, x.name)
		}
	case *binOp:
		if err := validateExpr(x.lhs, rt); err != nil {
			return err
		}
		if err := validateExpr(x.rhs, rt); err != nil {
			return err
		}
	case *notExpr:
		return validateExpr(x.e, rt)
	case *isNullExpr:
		return validateExpr(x.e, rt)
	}
	return nil
}

// evalCtx carries the per-call state expression eval needs.
type evalCtx struct {
	row  Row
	cols map[string]int // points into resolvedTable.colByName
	args []Value
}

func evalExpr(e expr, ctx *evalCtx) (Value, error) {
	switch x := e.(type) {
	case nil:
		return Bool(true), nil
	case *colRef:
		return ctx.row[ctx.cols[x.name]], nil
	case *litValue:
		return x.v, nil
	case *paramRef:
		if x.index < 0 || x.index >= len(ctx.args) {
			return Value{}, fmt.Errorf("%w: param index %d out of range", ErrParamMismatch, x.index)
		}
		return ctx.args[x.index], nil
	case *binOp:
		switch x.op {
		case tkAnd:
			lv, err := evalExpr(x.lhs, ctx)
			if err != nil {
				return Value{}, err
			}
			if !truthy(lv) {
				return Bool(false), nil
			}
			rv, err := evalExpr(x.rhs, ctx)
			if err != nil {
				return Value{}, err
			}
			return Bool(truthy(rv)), nil
		case tkOr:
			lv, err := evalExpr(x.lhs, ctx)
			if err != nil {
				return Value{}, err
			}
			if truthy(lv) {
				return Bool(true), nil
			}
			rv, err := evalExpr(x.rhs, ctx)
			if err != nil {
				return Value{}, err
			}
			return Bool(truthy(rv)), nil
		}
		lv, err := evalExpr(x.lhs, ctx)
		if err != nil {
			return Value{}, err
		}
		rv, err := evalExpr(x.rhs, ctx)
		if err != nil {
			return Value{}, err
		}
		switch x.op {
		case tkPlus, tkMinus, tkStar:
			// Integer arithmetic. NULL propagates (any null operand -> null),
			// matching SQL semantics. int64 wraps on overflow.
			if lv.IsNull() || rv.IsNull() {
				return Null(), nil
			}
			a, err := lv.AsInt()
			if err != nil {
				return Value{}, err
			}
			b, err := rv.AsInt()
			if err != nil {
				return Value{}, err
			}
			switch x.op {
			case tkPlus:
				return Int(a + b), nil
			case tkMinus:
				return Int(a - b), nil
			default:
				return Int(a * b), nil
			}
		case tkEq:
			return Bool(lv.Equal(rv)), nil
		case tkNeq:
			return Bool(!lv.Equal(rv)), nil
		}
		cmp, ok := lv.Compare(rv)
		if !ok {
			return Bool(false), nil
		}
		switch x.op {
		case tkLt:
			return Bool(cmp < 0), nil
		case tkLte:
			return Bool(cmp <= 0), nil
		case tkGt:
			return Bool(cmp > 0), nil
		case tkGte:
			return Bool(cmp >= 0), nil
		}
	case *notExpr:
		v, err := evalExpr(x.e, ctx)
		if err != nil {
			return Value{}, err
		}
		return Bool(!truthy(v)), nil
	case *isNullExpr:
		v, err := evalExpr(x.e, ctx)
		if err != nil {
			return Value{}, err
		}
		isn := v.IsNull()
		if x.not {
			return Bool(!isn), nil
		}
		return Bool(isn), nil
	}
	return Value{}, fmt.Errorf("internal: unknown expr type %T", e)
}

func truthy(v Value) bool {
	if v.Kind == KindBool {
		return v.I == 1
	}
	if v.Kind == KindInt {
		return v.I != 0
	}
	return false
}

// execSelect runs the SELECT plan. Returns the columns and a slice of
// projected rows. Rows are deep-cloned before returning so the caller
// may mutate them without affecting storage.
func (db *DB) execSelect(pl *plan, args []Value) ([]string, []Row, error) {
	st := pl.st.(*selectStmt)
	tbl := db.t[pl.table.def.Name]

	colNames := make([]string, 0, len(tbl.def.def.Columns))
	if st.starAll {
		for _, c := range tbl.def.def.Columns {
			colNames = append(colNames, c.Name)
		}
	} else {
		for _, c := range st.cols {
			colNames = append(colNames, c.col)
		}
	}

	ctx := &evalCtx{cols: tbl.def.colByName, args: args}

	// Fast path: PK equality — single map lookup, no scan, no sort.
	// Project directly into the result row to skip the matched-list
	// allocation and the full-row clone.
	if pl.pkLookup {
		keyVal, err := evalExpr(pl.pkSource, ctx)
		if err != nil {
			return nil, nil, err
		}
		if keyVal.IsNull() {
			return colNames, nil, nil
		}
		pk, err := coerceToUUID(keyVal)
		if err != nil {
			return nil, nil, err
		}
		r, ok := tbl.getByPK(pk)
		if !ok {
			return colNames, nil, nil
		}
		if st.starAll {
			return colNames, []Row{r.Clone()}, nil
		}
		pr := make(Row, len(pl.projOrdinals))
		for j, ord := range pl.projOrdinals {
			v := r[ord]
			// Defensive copy for KindBytes; strings/ints are value-safe.
			if v.Kind == KindBytes && v.B != nil {
				cp := make([]byte, len(v.B))
				copy(cp, v.B)
				v.B = cp
			}
			pr[j] = v
		}
		return colNames, []Row{pr}, nil
	}

	// Collect matching rows from the storage layer.
	var matched []Row
	tbl.scanAll(func(r Row) bool {
		if st.where != nil {
			ctx.row = r
			v, err := evalExpr(st.where, ctx)
			if err != nil || !truthy(v) {
				return true
			}
		}
		matched = append(matched, r.Clone())
		return true
	})

	// ORDER BY
	if pl.orderOrdinal >= 0 {
		ord := pl.orderOrdinal
		desc := st.orderDesc
		sort.SliceStable(matched, func(i, j int) bool {
			c, ok := matched[i][ord].Compare(matched[j][ord])
			if !ok {
				return false
			}
			if desc {
				return c > 0
			}
			return c < 0
		})
	}

	// LIMIT
	if st.limit >= 0 && st.limit < len(matched) {
		matched = matched[:st.limit]
	}

	// Projection
	if st.starAll {
		return colNames, matched, nil
	}
	out := make([]Row, len(matched))
	for i, r := range matched {
		pr := make(Row, len(pl.projOrdinals))
		for j, ord := range pl.projOrdinals {
			pr[j] = r[ord]
		}
		out[i] = pr
	}
	return colNames, out, nil
}

// execInsert builds the row and appends. Returns the count (1 if
// inserted) and an error.
func (db *DB) execInsert(pl *plan, args []Value) (int, error) {
	st := pl.st.(*insertStmt)
	tbl := db.t[pl.table.def.Name]
	row := make(Row, len(tbl.def.def.Columns))
	for i := range row {
		row[i] = Null()
	}
	ctx := &evalCtx{args: args}
	pkOrd := tbl.def.pkOrdinal
	pkProvided := false
	for i, ord := range pl.insertOrdinals {
		v, err := evalExpr(st.vals[i], ctx)
		if err != nil {
			return 0, err
		}
		col := tbl.def.def.Columns[ord]
		// API-boundary coercion: a string destined for a UUID column is
		// parsed into a UUID — storage only ever sees [16]byte.
		if col.Type == TypeUUID && v.Kind == KindString {
			u, perr := ParseUUID(v.S)
			if perr != nil {
				return 0, perr
			}
			v = UUIDVal(u)
		}
		if err := validateValue(col, v); err != nil {
			return 0, err
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
	// Check NOT NULL on omitted columns.
	for ord, c := range tbl.def.def.Columns {
		if row[ord].IsNull() && !c.Nullable {
			return 0, fmt.Errorf("column %q is NOT NULL", c.Name)
		}
	}
	// PK uniqueness + WAL append + apply run atomically under the shard
	// lock (see insertJournaled). Ordering matters: a duplicate PK must be
	// rejected before anything is journaled, and a WAL failure must abort
	// before the row is applied.
	err := tbl.insertJournaled(row, func() error {
		if db.wal == nil {
			return nil
		}
		body := encodeRow(db.scratch.get(), row)
		werr := db.wal.writeRecord(opInsert, tbl.tableID, body)
		db.scratch.put(body)
		return werr
	})
	if err != nil {
		return 0, err
	}
	return 1, nil
}

// execUpdate evaluates the SET values once, then dispatches on the WHERE
// shape. A PK-pinned update hits exactly one shard and mutates in place
// under that shard's lock (hot path, allocation-free). An unpinned predicate
// update can span shards and goes through updateWhereAll, which holds every
// shard lock across the journal+apply so the WAL order and in-memory order
// stay identical — the one-shard-at-a-time form is a replay-divergence bug.
func (db *DB) execUpdate(pl *plan, args []Value) (int, error) {
	st := pl.st.(*updateStmt)
	tbl := db.t[pl.table.def.Name]
	ctx := &evalCtx{cols: tbl.def.colByName, args: args}

	match := func(r Row) bool {
		if st.where == nil {
			return true
		}
		ctx.row = r
		v, err := evalExpr(st.where, ctx)
		if err != nil {
			return false
		}
		return truthy(v)
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
	// shard's lock (updateByPKJournaled). nil journal for memory-only keeps
	// this hot path allocation-free.
	if pl.pkLookup {
		keyVal, err := evalExpr(pl.pkSource, &evalCtx{args: args})
		if err != nil {
			return 0, err
		}
		if keyVal.IsNull() {
			return 0, nil
		}
		var journal func(Row) error
		if db.wal != nil {
			journal = func(nr Row) error {
				body := encodeRow(db.scratch.get(), nr)
				werr := db.wal.writeRecord(opUpdate, tbl.tableID, body)
				db.scratch.put(body)
				return werr
			}
		}
		pk, err := coerceToUUID(keyVal)
		if err != nil {
			return 0, err
		}
		ok, err := tbl.updateByPKJournaled(pk, pl.updateOrdinals, compute, journal)
		if err != nil {
			return 0, err
		}
		if ok {
			return 1, nil
		}
		return 0, nil
	}

	// Multi-shard predicate path: updateWhereAll computes each row's new
	// values, clones, journals the image, then stores it — all under every
	// shard lock.
	journal := func(nr Row) error {
		if db.wal == nil {
			return nil
		}
		body := encodeRow(db.scratch.get(), nr)
		err := db.wal.writeRecord(opUpdate, tbl.tableID, body)
		db.scratch.put(body)
		return err
	}
	return tbl.updateWhereAll(match, pl.updateOrdinals, compute, journal)
}

// execDelete dispatches on the WHERE shape, mirroring execUpdate. A
// PK-pinned delete hits one shard. An unpinned predicate delete goes
// through deleteWhereAll, which holds every shard lock across journal+apply
// (the one-shard-at-a-time form diverges on replay). Journaling is done by
// the store under the locks — never as a side effect of the match predicate.
func (db *DB) execDelete(pl *plan, args []Value) (int, error) {
	st := pl.st.(*deleteStmt)
	tbl := db.t[pl.table.def.Name]
	ctx := &evalCtx{cols: tbl.def.colByName, args: args}

	// Fast path: PK equality — single shard, journal-before-tombstone under
	// the shard lock (deleteByPKJournaled, which also closes the old
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
		pkVal := UUIDVal(pk) // journal the resolved UUID, not the raw arg
		var journal func() error
		if db.wal != nil {
			journal = func() error {
				body := encodePK(db.scratch.get(), pkVal)
				werr := db.wal.writeRecord(opDelete, tbl.tableID, body)
				db.scratch.put(body)
				return werr
			}
		}
		ok, err := tbl.deleteByPKJournaled(pk, journal)
		if err != nil {
			return 0, err
		}
		if ok {
			return 1, nil
		}
		return 0, nil
	}

	// Multi-shard predicate path: match is pure; deleteWhereAll journals
	// each PK before tombstoning, under every shard lock.
	match := func(r Row) bool {
		if st.where == nil {
			return true
		}
		ctx.row = r
		v, err := evalExpr(st.where, ctx)
		if err != nil {
			return false
		}
		return truthy(v)
	}
	journal := func(pk Value) error {
		if db.wal == nil {
			return nil
		}
		body := encodePK(db.scratch.get(), pk)
		err := db.wal.writeRecord(opDelete, tbl.tableID, body)
		db.scratch.put(body)
		return err
	}
	return tbl.deleteWhereAll(match, journal)
}
