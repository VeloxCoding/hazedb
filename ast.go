package hazedb

// AST node types for the SELECT/INSERT/UPDATE/DELETE subset.

type stmt interface{ isStmt() }

type selectStmt struct {
	cols      []resultCol // empty means *
	starAll   bool
	countStar bool // SELECT COUNT(*) — the sole aggregate; cols is empty
	table     string
	alias     string       // FROM table alias ("" = none; the table name still qualifies)
	joins     []joinClause // v1: at most one (two-table join)
	where     expr
	orderCol  string // empty means no ORDER BY
	orderQual string // ORDER BY column qualifier (table/alias), "" = unqualified
	orderDesc bool
	limit     int // -1 means no LIMIT
	offset    int // 0 means no OFFSET; skips the first offset matched rows
}

// joinClause is one `[INNER|LEFT] JOIN table [alias] ON l.col = r.col`. v1
// supports a single equi-join on one column; lref/rref are the two sides of the
// ON equality (each a qualified column ref). typ is tkInner or tkLeft.
type joinClause struct {
	typ        tokenKind
	table      string
	alias      string
	lref, rref colRef
}

type insertStmt struct {
	table string
	cols  []string
	rows  [][]expr // one or more VALUES tuples; each tuple has len(cols) exprs
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
	qual string // table/alias qualifier ("" = unqualified)
	col  string
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
	qual string // table/alias qualifier ("" = unqualified); used only at resolve time
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
