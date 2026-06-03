package hazedb

// Compiled row filters. A full-table scan evaluates the WHERE on every row; the
// generic evalExpr walks the expression tree per row (type switches, recursive
// operand eval, a per-row parameter-bounds check). For the common simple shapes
// — col = ?, col < ?, col IS NULL, and AND/OR/NOT of those — compileFilter builds
// a closure ONCE (parameters resolved up front) that tests the row directly,
// e.g. row[ord].Equal(arg). It reuses the SAME Value.Equal / Value.Compare /
// Value.IsNull primitives evalExpr uses, so a compiled predicate is bit-identical
// to the interpreted one. Anything not recognized returns ok=false and the caller
// falls back to evalExpr — so correctness never depends on coverage.

// compileFilter returns a fast predicate for e, or (nil, false) to fall back.
// args are the query parameters, resolved into the closure once (not per row).
func compileFilter(e expr, args []Value) (func(Row) bool, bool) {
	switch x := e.(type) {
	case *binOp:
		switch x.op {
		case tkAnd:
			l, lok := compileFilter(x.lhs, args)
			r, rok := compileFilter(x.rhs, args)
			if lok && rok {
				return func(row Row) bool { return l(row) && r(row) }, true
			}
			return nil, false
		case tkOr:
			l, lok := compileFilter(x.lhs, args)
			r, rok := compileFilter(x.rhs, args)
			if lok && rok {
				return func(row Row) bool { return l(row) || r(row) }, true
			}
			return nil, false
		case tkEq, tkNeq, tkLt, tkLte, tkGt, tkGte:
			return compileCmp(x, args)
		}
	case *notExpr:
		if inner, ok := compileFilter(x.e, args); ok {
			return func(row Row) bool { return !inner(row) }, true
		}
	case *isNullExpr:
		if cr, ok := x.e.(*colRef); ok && cr.ord >= 0 {
			ord, not := cr.ord, x.not
			return func(row Row) bool { return row[ord].IsNull() != not }, true
		}
	}
	return nil, false
}

// compileCmp handles `col OP value` where col is on the left (the common form)
// and value is a parameter or literal. Reversed operands, column-vs-column, or
// arithmetic operands fall back. Mirrors evalExpr's comparison branch exactly:
// tkEq/tkNeq via Value.Equal; the ordered ops via Value.Compare, treating an
// incomparable pair (ok=false) as no match — identical to evalExpr's Bool(false).
func compileCmp(x *binOp, args []Value) (func(Row) bool, bool) {
	cr, ok := x.lhs.(*colRef)
	if !ok || cr.ord < 0 {
		return nil, false
	}
	rhs, ok := resolveOperand(x.rhs, args)
	if !ok {
		return nil, false
	}
	ord := cr.ord
	switch x.op {
	case tkEq:
		return func(row Row) bool { return row[ord].Equal(rhs) }, true
	case tkNeq:
		return func(row Row) bool { return !row[ord].Equal(rhs) }, true
	case tkLt:
		return func(row Row) bool { c, ok := row[ord].Compare(rhs); return ok && c < 0 }, true
	case tkLte:
		return func(row Row) bool { c, ok := row[ord].Compare(rhs); return ok && c <= 0 }, true
	case tkGt:
		return func(row Row) bool { c, ok := row[ord].Compare(rhs); return ok && c > 0 }, true
	case tkGte:
		return func(row Row) bool { c, ok := row[ord].Compare(rhs); return ok && c >= 0 }, true
	}
	return nil, false
}

// resolveOperand resolves a literal or parameter to its Value once. A param index
// out of range falls back (ok=false) so evalExpr produces the proper error rather
// than the compiled path silently masking it.
func resolveOperand(e expr, args []Value) (Value, bool) {
	switch v := e.(type) {
	case *litValue:
		return v.v, true
	case *paramRef:
		if v.index < 0 || v.index >= len(args) {
			return Value{}, false
		}
		return args[v.index], true
	}
	return Value{}, false
}

// rowMatcher returns the predicate a scan applies per row: the compiled fast path
// when the WHERE is a recognized shape, otherwise an evalExpr fallback. A nil
// WHERE matches every row. The returned func is built once per query, before the
// scan; ctx is reused across rows (the scan is single-threaded per shard).
func rowMatcher(where expr, ctx *evalCtx) func(Row) bool {
	if where == nil {
		return func(Row) bool { return true }
	}
	if fast, ok := compileFilter(where, ctx.args); ok {
		return fast
	}
	return func(row Row) bool {
		ctx.row = row
		v, err := evalExpr(where, ctx)
		return err == nil && truthy(v)
	}
}
