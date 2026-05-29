package hazedb

// AST node types for the SELECT/INSERT/UPDATE/DELETE subset.

type stmt interface{ isStmt() }

type selectStmt struct {
	cols      []resultCol // empty means *
	starAll   bool
	table     string
	where     expr
	orderCol  string // empty means no ORDER BY
	orderDesc bool
	limit     int // -1 means no LIMIT
}

type insertStmt struct {
	table string
	cols  []string
	vals  []expr
}

type updateStmt struct {
	table string
	sets  []setAssign
	where expr
}

type deleteStmt struct {
	table string
	where expr
}

// createStmt / dropStmt are the runtime DDL statements.
type createStmt struct{ def TableDef }
type dropStmt struct{ name string }

type setAssign struct {
	col string
	val expr
}

type resultCol struct {
	col string
}

func (*selectStmt) isStmt() {}
func (*insertStmt) isStmt() {}
func (*updateStmt) isStmt() {}
func (*deleteStmt) isStmt() {}
func (*createStmt) isStmt() {}
func (*dropStmt) isStmt()   {}

// Expression AST. Kept tiny for v1: column refs, literals, parameters,
// comparison ops, AND/OR/NOT, IS NULL, IS NOT NULL.
type expr interface{ isExpr() }

// colRef names a column; ord is its row ordinal, bound at plan time by
// validateExpr (-1 until bound) so evalExpr can index the row directly instead
// of a per-row name→ordinal map lookup. ord < 0 falls back to the name lookup.
type colRef struct {
	name string
	ord  int
}
type litValue struct{ v Value }
type paramRef struct{ index int } // zero-based into args slice

type binOp struct {
	op       tokenKind // tkEq, tkNeq, tkLt, tkLte, tkGt, tkGte, tkAnd, tkOr
	lhs, rhs expr
}

type notExpr struct{ e expr }

type isNullExpr struct {
	e   expr
	not bool
}

func (*colRef) isExpr()     {}
func (*litValue) isExpr()   {}
func (*paramRef) isExpr()   {}
func (*binOp) isExpr()      {}
func (*notExpr) isExpr()    {}
func (*isNullExpr) isExpr() {}
