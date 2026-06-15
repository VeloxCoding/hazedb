package hazedb

import (
	"fmt"
	"strings"
)

// Write verbs and arg conversion. Every write enters through Exec/ExecValues,
// converts its args to typed Value cells, then funnels through execWrite to the
// per-statement executor. See db.go for the gateway contract.

// ExecScript runs a TRUSTED, operator-supplied multi-statement script — a boot /
// migration / seed .sql file. It splits on top-level ';' with the lexer, so a ';'
// inside a string literal does NOT split the script, and runs each statement in
// order. Unlike Exec it permits inline value literals: a seed file writes constant
// data and has no ? args to bind. For that reason it must NEVER be fed request or
// otherwise untrusted input — that reopens the SQL-injection path Exec closes.
// Statements are not cached (one-shot boot work), so an unchecked literal plan can
// never reach a later Exec through a cache hit. Returns the summed affected-row
// count; a failing statement stops the script and is named in the error.
func (db *DB) ExecScript(script string) (int, error) {
	if db.closed.Load() {
		return 0, ErrClosed
	}
	toks, err := tokenize(script)
	if err != nil {
		return 0, err
	}
	total, start := 0, 0
	run := func(end int) error {
		s := strings.TrimSpace(script[start:end])
		start = end + 1
		if s == "" {
			return nil
		}
		pl, err := db.prepareTrusted(s, db.cat.Load())
		if err != nil {
			return fmt.Errorf("statement %q: %w", s, err)
		}
		n, err := db.execPlanValues(pl, nil)
		if err != nil {
			return fmt.Errorf("statement %q: %w", s, err)
		}
		total += n
		return nil
	}
	for _, t := range toks {
		if t.kind == tkSemi {
			if err := run(t.pos); err != nil {
				return total, err
			}
		}
	}
	if err := run(len(script)); err != nil { // trailing statement (no final ';')
		return total, err
	}
	return total, nil
}

// Exec runs an INSERT, UPDATE, DELETE, CREATE TABLE, or DROP TABLE. Returns
// the affected row count (0 for DDL).
func (db *DB) Exec(sql string, args ...any) (int, error) {
	pl, err := db.prepare(sql, db.cat.Load())
	if err != nil {
		return 0, err
	}
	return db.execPlan(pl, args)
}

// ExecValues runs a write with pre-typed Value arguments, skipping the []any
// arg-conversion layer Exec uses (no JSON decode, no interface boxing, no
// per-arg type switch). It is the in-process fast path for callers that already
// hold typed Values — notably the PHP extension reading a native zend_array via
// cgo, which avoids the json_encode/json.Decode round-trip the string args form
// pays. Returns the affected row count (0 for DDL).
func (db *DB) ExecValues(sql string, args ...Value) (int, error) {
	pl, err := db.prepare(sql, db.cat.Load())
	if err != nil {
		return 0, err
	}
	return db.execPlanValues(pl, args)
}

// execPlan runs a non-SELECT plan against raw args. Shared by Exec (which looks
// the plan up by SQL each call) and *Stmt.Exec (which holds a compiled plan).
func (db *DB) execPlan(pl *plan, args []any) (int, error) {
	// Convert into a stack buffer for the common small-arg write (like
	// execPlanValues): the converted slice does not outlive this call, so a
	// statement with <= 8 params binds its args without a heap allocation.
	var argBuf [8]Value
	vargs, err := toValuesInto(args, argBuf[:])
	if err != nil {
		return 0, err
	}
	return db.execWrite(pl, vargs)
}

// execPlanValues is execPlan for pre-typed args: it clones each arg with
// cloneValue (a no-op except for KindBytes, which must not alias caller memory
// across the write boundary — the same guarantee toValue gives the []any path)
// and dispatches to the same write executors.
func (db *DB) execPlanValues(pl *plan, args []Value) (int, error) {
	var argBuf [8]Value
	var vargs []Value
	if len(args) <= len(argBuf) {
		vargs = argBuf[:len(args)]
	} else {
		vargs = make([]Value, len(args))
	}
	for i, a := range args {
		vargs[i] = cloneValue(a)
	}
	return db.execWrite(pl, vargs)
}

// checkArgs rejects an arg count that does not match the statement's parameter
// count. Standard drivers fail loud on a count mismatch in either direction.
func (pl *plan) checkArgs(n int) error {
	if n != pl.nparams {
		return fmt.Errorf("%w: got %d args, statement has %d parameters", ErrParamMismatch, n, pl.nparams)
	}
	return nil
}

// execWrite dispatches a write plan to its executor. Shared by execPlan (any
// args) and execPlanValues (pre-typed Value args) once each has converted its
// args; DDL ignores vargs.
func (db *DB) execWrite(pl *plan, vargs []Value) (int, error) {
	if err := pl.checkArgs(len(vargs)); err != nil {
		return 0, err
	}
	if err := coerceParams(pl, vargs); err != nil {
		return 0, err
	}
	switch s := pl.st.(type) {
	case *createStmt:
		return 0, db.createTable(s.def)
	case *dropStmt:
		return 0, db.dropTable(s.name)
	case *insertStmt:
		return db.execInsert(pl, vargs)
	case *updateStmt:
		return db.execUpdate(pl, vargs)
	case *deleteStmt:
		return db.execDelete(pl, vargs)
	}
	return 0, fmt.Errorf("hazedb: Exec used with SELECT — use Query instead")
}

// journalTxnBodies writes a group of pre-encoded mutation bodies as ONE atomic
// TXN WAL envelope. The broad UPDATE/DELETE paths use it so a multi-row
// statement is all-or-nothing: the single record is written before any row is
// applied (a WAL failure leaves nothing applied), and a torn envelope is
// discarded whole on replay. Caller guarantees db.wal != nil.
func (db *DB) journalTxnBodies(bodies [][]byte) error {
	bp := db.scratch.get()
	*bp = encodeTxn(*bp, bodies)
	err := db.wal.writeRecord(recTxn, *bp)
	db.scratch.put(bp)
	return err
}

// toValues converts variadic args into Value cells. Supports int, int64,
// string, []byte, bool, nil, and Value pass-through.
func toValues(args []any) ([]Value, error) {
	return toValuesInto(args, nil)
}

func toValuesInto(args []any, scratch []Value) ([]Value, error) {
	var out []Value
	if len(args) <= len(scratch) {
		out = scratch[:len(args)]
	} else {
		out = make([]Value, len(args))
	}
	for i, a := range args {
		v, err := toValue(a, i)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func toValue(a any, index int) (Value, error) {
	switch x := a.(type) {
	case nil:
		return Null(), nil
	case int:
		return Int(int64(x)), nil
	case int64:
		return Int(x), nil
	case int32:
		return Int(int64(x)), nil
	case string:
		// Stored by reference: a string is immutable, so the cell aliases the caller's
		// backing rather than copying it (keeps the write path lean — no per-string
		// alloc). The caller owns the backing-size contract: a substring of a large
		// buffer (e.g. a field sliced out of a whole file) pins that entire buffer
		// while rowCost charges only the substring length, so strings.Clone it first
		// if the parent should be freed. []byte below is cloned instead, because a
		// caller can mutate it after the call.
		return Str(x), nil
	case []byte:
		// Clone at the write boundary: storage must not alias a caller slice
		// the caller can mutate after the call returns — that would corrupt the
		// stored row and diverge from the (already-written) WAL record.
		return Bytes(cloneBytes(x)), nil
	case bool:
		return Bool(x), nil
	case UUID:
		return UUIDVal(x), nil
	case Value:
		// A caller-built Value can also carry an aliased []byte; deep-copy it.
		return cloneValue(x), nil
	default:
		return Value{}, fmt.Errorf("%w: unsupported arg type %T at %d", ErrTypeMismatch, a, index)
	}
}
