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
			pl.updateOrdinals = append(pl.updateOrdinals, ord)
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
		r, ok := tbl.getByPK(keyVal.AsString())
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
	for i, ord := range pl.insertOrdinals {
		v, err := evalExpr(st.vals[i], ctx)
		if err != nil {
			return 0, err
		}
		col := tbl.def.def.Columns[ord]
		if err := validateValue(col, v); err != nil {
			return 0, err
		}
		row[ord] = v
	}
	// Check NOT NULL on omitted columns.
	for ord, c := range tbl.def.def.Columns {
		if row[ord].IsNull() && !c.Nullable {
			return 0, fmt.Errorf("column %q is NOT NULL", c.Name)
		}
	}
	// WAL
	if db.wal != nil {
		body := encodeRow(db.scratch.get(), row)
		err := db.wal.writeRecord(opInsert, tbl.tableID, body)
		db.scratch.put(body)
		if err != nil {
			return 0, err
		}
	}
	if err := tbl.insert(row); err != nil {
		return 0, err
	}
	return 1, nil
}

// execUpdate evaluates SET values once per matched row, mutates rows
// in place under each shard's write lock, and journals each change.
//
// Note on WAL ordering: each row update goes through writeRecord
// inside the shard write lock, so the WAL ordering within one shard
// matches the storage order. Cross-shard ordering is undefined (same
// as Insert).
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

	// Pre-evaluate SET values that don't depend on the row (literals
	// and parameters). Re-evaluating per row would be wasteful since
	// v1 SET expressions don't reference other columns.
	setVals := make([]Value, len(st.sets))
	for i, a := range st.sets {
		v, err := evalExpr(a.val, &evalCtx{args: args})
		if err != nil {
			return 0, err
		}
		col := tbl.def.def.Columns[pl.updateOrdinals[i]]
		if err := validateValue(col, v); err != nil {
			return 0, err
		}
		setVals[i] = v
	}

	mutate := func(r Row) Row {
		// Mutate in place. Storage layer expects a Row back even if the
		// reference is the same.
		for i, ord := range pl.updateOrdinals {
			r[ord] = setVals[i]
		}
		if db.wal != nil {
			body := encodeRow(db.scratch.get(), r)
			db.wal.writeRecord(opUpdate, tbl.tableID, body)
			db.scratch.put(body)
		}
		return r
	}

	// Fast path: PK equality — go straight through tableShard.update.
	if pl.pkLookup {
		keyVal, err := evalExpr(pl.pkSource, &evalCtx{args: args})
		if err != nil {
			return 0, err
		}
		if keyVal.IsNull() {
			return 0, nil
		}
		if tbl.update(keyVal.AsString(), mutate) {
			return 1, nil
		}
		return 0, nil
	}

	n := tbl.updateWhere(match, mutate)
	return n, nil
}

// execDelete tombstones matching rows and journals each one.
func (db *DB) execDelete(pl *plan, args []Value) (int, error) {
	st := pl.st.(*deleteStmt)
	tbl := db.t[pl.table.def.Name]
	ctx := &evalCtx{cols: tbl.def.colByName, args: args}

	pkOrd := tbl.def.pkOrdinal

	// Fast path: PK equality — single map lookup, no scan.
	if pl.pkLookup {
		keyVal, err := evalExpr(pl.pkSource, &evalCtx{args: args})
		if err != nil {
			return 0, err
		}
		if keyVal.IsNull() {
			return 0, nil
		}
		pkStr := keyVal.AsString()
		// Look up the row first so we can journal its PK before deletion.
		// Concurrent deletes are fine: storage layer's deleteByPK is
		// idempotent (returns false on absent).
		if _, ok := tbl.getByPK(pkStr); !ok {
			return 0, nil
		}
		if db.wal != nil {
			body := encodePK(db.scratch.get(), keyVal)
			db.wal.writeRecord(opDelete, tbl.tableID, body)
			db.scratch.put(body)
		}
		if tbl.deleteByPK(pkStr) {
			return 1, nil
		}
		return 0, nil
	}

	match := func(r Row) bool {
		if st.where == nil {
			return true
		}
		ctx.row = r
		v, err := evalExpr(st.where, ctx)
		if err != nil {
			return false
		}
		if !truthy(v) {
			return false
		}
		if db.wal != nil {
			body := encodePK(db.scratch.get(), r[pkOrd])
			db.wal.writeRecord(opDelete, tbl.tableID, body)
			db.scratch.put(body)
		}
		return true
	}
	n := tbl.deleteWhere(match)
	return n, nil
}
