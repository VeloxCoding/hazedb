package hazedb

import "fmt"

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
		if x.ord >= 0 {
			return ctx.row[x.ord], nil
		}
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
		return v.Int() == 1
	}
	if v.Kind == KindInt {
		return v.Int() != 0
	}
	return false
}

// evalLitOrParamValue resolves a literal or positional parameter to its Value.
// Used to evaluate a PK-lookup key from already-converted args (every Query
// entry point converts to []Value before routing — see queryPlanV).
func evalLitOrParamValue(e expr, args []Value) (Value, error) {
	switch x := e.(type) {
	case *litValue:
		return x.v, nil
	case *paramRef:
		if x.index < 0 || x.index >= len(args) {
			return Value{}, fmt.Errorf("%w: param index %d out of range", ErrParamMismatch, x.index)
		}
		return args[x.index], nil
	default:
		return Value{}, fmt.Errorf("internal: expected literal or parameter, got %T", e)
	}
}
