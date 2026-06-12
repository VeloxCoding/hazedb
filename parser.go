package hazedb

import (
	"fmt"
	"strconv"
)

// Parse turns one SQL statement into a stmt AST. Trailing semicolon
// allowed but optional. Only one statement per call.
func parseSQL(sql string) (stmt, error) {
	toks, err := tokenize(sql)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	st, err := p.parseStmt()
	if err != nil {
		return nil, err
	}
	if p.peek().kind == tkSemi {
		p.advance()
	}
	if p.peek().kind != tkEOF {
		return nil, fmt.Errorf("%w: trailing tokens after statement at %d", ErrParse, p.peek().pos)
	}
	return st, nil
}

type parser struct {
	toks  []token
	pos   int
	depth int // current expression-nesting depth; guarded by enter/leave
}

// maxExprDepth bounds expression nesting (parentheses and NOT) so a crafted query
// cannot drive the recursive-descent parser past the goroutine stack limit. A
// stack overflow is a fatal runtime error — recover() cannot catch it — so an
// unbounded parse is a one-request remote kill of the whole process (and the
// in-memory DB). Real queries nest only a handful deep; AND/OR/arithmetic chains
// iterate rather than recurse, so they cost no depth. 256 is far above any
// genuine query and far below the overflow point. The cap lives in the parser
// (not an adapter) so the cgo PHP path — which has no net against a fatal error
// either — is protected too.
const maxExprDepth = 256

// enter deepens the nesting counter and rejects beyond maxExprDepth with a normal
// parse error (→ 400 / PHP -1), never a crash. Pair every enter with a leave.
func (p *parser) enter() error {
	p.depth++
	if p.depth > maxExprDepth {
		return fmt.Errorf("%w: expression nesting too deep (limit %d)", ErrParse, maxExprDepth)
	}
	return nil
}

func (p *parser) leave() { p.depth-- }

func (p *parser) peek() token { return p.toks[p.pos] }
func (p *parser) advance() token {
	t := p.toks[p.pos]
	if p.pos < len(p.toks)-1 {
		p.pos++
	}
	return t
}
func (p *parser) expect(k tokenKind, what string) (token, error) {
	if p.peek().kind != k {
		return token{}, fmt.Errorf("%w: expected %s at %d", ErrParse, what, p.peek().pos)
	}
	return p.advance(), nil
}

func (p *parser) parseStmt() (stmt, error) {
	switch p.peek().kind {
	case tkSelect:
		return p.parseSelect()
	case tkInsert:
		return p.parseInsert()
	case tkUpdate:
		return p.parseUpdate()
	case tkDelete:
		return p.parseDelete()
	case tkCreate:
		return p.parseCreate()
	case tkDrop:
		return p.parseDrop()
	}
	return nil, fmt.Errorf("%w: unexpected token at %d", ErrParse, p.peek().pos)
}

// parseCreate parses CREATE TABLE name (col TYPE [constraints], ...). Types:
// int, text/string, bool, bytes/blob, uuid. Constraints: PRIMARY KEY,
// PARTITION KEY, IMMUTABLE, NULL (nullable; columns are NOT NULL by default).
func (p *parser) parseCreate() (*createStmt, error) {
	p.advance() // CREATE
	if _, err := p.expect(tkTable, "TABLE"); err != nil {
		return nil, err
	}
	tn, err := p.expect(tkIdent, "table name")
	if err != nil {
		return nil, err
	}
	st := &createStmt{def: TableDef{Name: tn.text}}
	if _, err := p.expect(tkLParen, "("); err != nil {
		return nil, err
	}
	for {
		// Table-level index declaration ([ORDERED] INDEX [name] (col)) instead of
		// a column. "index"/"ordered" are thus reserved as leading words here.
		if t := p.peek(); t.kind == tkIdent && (t.text == "index" || t.text == "ordered") {
			ix, err := p.parseIndexClause()
			if err != nil {
				return nil, err
			}
			st.def.Indexes = append(st.def.Indexes, ix)
			if p.peek().kind != tkComma {
				break
			}
			p.advance()
			continue
		}
		cn, err := p.expect(tkIdent, "column name")
		if err != nil {
			return nil, err
		}
		tt, err := p.expect(tkIdent, "column type")
		if err != nil {
			return nil, err
		}
		ct, err := parseColType(tt.text)
		if err != nil {
			return nil, err
		}
		col := ColumnDef{Name: cn.text, Type: ct}
		for k := p.peek().kind; k != tkComma && k != tkRParen && k != tkEOF; k = p.peek().kind {
			t := p.advance()
			switch {
			case t.kind == tkNull:
				col.Nullable = true
			case t.kind == tkNot: // NOT NULL — the default; consume optional NULL
				if p.peek().kind == tkNull {
					p.advance()
				}
			case t.text == "primary":
				if w := p.advance(); w.text != "key" {
					return nil, fmt.Errorf("%w: expected KEY after PRIMARY at %d", ErrParse, w.pos)
				}
				col.PK = true
			case t.text == "partition":
				if w := p.advance(); w.text != "key" {
					return nil, fmt.Errorf("%w: expected KEY after PARTITION at %d", ErrParse, w.pos)
				}
				col.PartitionKey = true
			case t.text == "immutable":
				col.Immutable = true
			default:
				return nil, fmt.Errorf("%w: unexpected column constraint %q at %d", ErrParse, t.text, t.pos)
			}
		}
		st.def.Columns = append(st.def.Columns, col)
		if p.peek().kind != tkComma {
			break
		}
		p.advance()
	}
	if _, err := p.expect(tkRParen, ")"); err != nil {
		return nil, err
	}
	return st, nil
}

// parseIndexClause parses a table-level index declaration:
//
//	[ORDERED] INDEX [name] (col [, col...])
//
// ORDERED makes it a sorted index (equality + ranges + ORDER BY); the default
// is a hash index (equality only). A composite (multi-column) index must be
// ORDERED: the hash form would only serve exact whole-tuple equality (no better
// than the bucket intersection the planner already does), while the ordered form
// serves prefix equality + ORDER BY on the trailing column — the reason
// composite exists here.
func (p *parser) parseIndexClause() (IndexDef, error) {
	var ix IndexDef
	for { // optional modifiers before INDEX
		if p.peek().text == "ordered" {
			p.advance()
			ix.Ordered = true
			continue
		}
		break
	}
	if w := p.advance(); w.text != "index" {
		return ix, fmt.Errorf("%w: expected INDEX at %d", ErrParse, w.pos)
	}
	if p.peek().kind == tkIdent { // optional index name before the '('
		ix.Name = p.advance().text
	}
	if _, err := p.expect(tkLParen, "("); err != nil {
		return ix, err
	}
	for {
		cn, err := p.expect(tkIdent, "index column")
		if err != nil {
			return ix, err
		}
		ix.Columns = append(ix.Columns, cn.text)
		if p.peek().kind != tkComma {
			break
		}
		p.advance() // consume the comma; another column follows
	}
	if _, err := p.expect(tkRParen, ")"); err != nil {
		return ix, err
	}
	if len(ix.Columns) > 1 && !ix.Ordered {
		return ix, fmt.Errorf("%w: composite index must be ORDERED (hash composite unsupported)", ErrParse)
	}
	return ix, nil
}

func parseColType(s string) (ColumnType, error) {
	switch s {
	case "int":
		return TypeInt, nil
	case "text", "string":
		return TypeString, nil
	case "bool":
		return TypeBool, nil
	case "bytes", "blob":
		return TypeBytes, nil
	case "uuid":
		return TypeUUID, nil
	}
	return 0, fmt.Errorf("%w: unknown column type %q", ErrParse, s)
}

func (p *parser) parseDrop() (*dropStmt, error) {
	p.advance() // DROP
	if _, err := p.expect(tkTable, "TABLE"); err != nil {
		return nil, err
	}
	tn, err := p.expect(tkIdent, "table name")
	if err != nil {
		return nil, err
	}
	return &dropStmt{name: tn.text}, nil
}

// parseColName parses an optionally-qualified column reference: ident [ . ident ].
// Returns (qualifier, name); qualifier is "" when unqualified.
func (p *parser) parseColName() (string, string, error) {
	id, err := p.expect(tkIdent, "column name")
	if err != nil {
		return "", "", err
	}
	if p.peek().kind == tkDot {
		p.advance()
		c, err := p.expect(tkIdent, "column name after '.'")
		if err != nil {
			return "", "", err
		}
		return id.text, c.text, nil
	}
	return "", id.text, nil
}

// parseJoins parses zero or more `[INNER|LEFT] JOIN table [alias] ON l.c = r.c`
// clauses onto st. Bare JOIN means INNER. v1 accepts the chain syntactically;
// the planner enforces the two-table (single-join) limit.
func (p *parser) parseJoins(st *selectStmt) error {
	for {
		k := p.peek().kind
		joinType := tkInner
		switch k {
		case tkJoin:
			// bare JOIN = INNER
		case tkInner, tkLeft, tkRight:
			joinType = k
			p.advance()
			if (k == tkLeft || k == tkRight) && p.peek().kind == tkOuter {
				p.advance() // LEFT/RIGHT [OUTER] JOIN — OUTER is an optional noise word
			}
			if _, err := p.expect(tkJoin, "JOIN"); err != nil {
				return err
			}
		default:
			return nil // no (more) joins
		}
		if k == tkJoin {
			p.advance()
		}
		jc := joinClause{typ: joinType}
		tn, err := p.expect(tkIdent, "join table name")
		if err != nil {
			return err
		}
		jc.table = tn.text
		if p.peek().kind == tkIdent { // optional join-table alias
			jc.alias = p.advance().text
		}
		if _, err := p.expect(tkOn, "ON"); err != nil {
			return err
		}
		lq, ln, err := p.parseColName()
		if err != nil {
			return err
		}
		if _, err := p.expect(tkEq, "= in JOIN ... ON (only equi-joins are supported)"); err != nil {
			return err
		}
		rq, rn, err := p.parseColName()
		if err != nil {
			return err
		}
		jc.lref = colRef{qual: lq, name: ln, ord: -1}
		jc.rref = colRef{qual: rq, name: rn, ord: -1}
		st.joins = append(st.joins, jc)
	}
}

func (p *parser) parseSelect() (*selectStmt, error) {
	p.advance() // SELECT
	st := &selectStmt{limit: -1}

	if p.peek().kind == tkStar {
		p.advance()
		st.starAll = true
	} else {
		for {
			q, name, err := p.parseColName()
			if err != nil {
				return nil, err
			}
			st.cols = append(st.cols, resultCol{qual: q, col: name})
			if p.peek().kind != tkComma {
				break
			}
			p.advance()
		}
	}

	if _, err := p.expect(tkFrom, "FROM"); err != nil {
		return nil, err
	}
	tn, err := p.expect(tkIdent, "table name")
	if err != nil {
		return nil, err
	}
	st.table = tn.text
	if p.peek().kind == tkIdent { // optional FROM alias (clause keywords are not idents)
		st.alias = p.advance().text
	}
	if err := p.parseJoins(st); err != nil {
		return nil, err
	}

	if p.peek().kind == tkWhere {
		p.advance()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		st.where = e
	}

	if p.peek().kind == tkOrder {
		p.advance()
		if _, err := p.expect(tkBy, "BY"); err != nil {
			return nil, err
		}
		q, name, err := p.parseColName()
		if err != nil {
			return nil, err
		}
		st.orderQual = q
		st.orderCol = name
		switch p.peek().kind {
		case tkAsc:
			p.advance()
		case tkDesc:
			p.advance()
			st.orderDesc = true
		}
	}

	if p.peek().kind == tkLimit {
		p.advance()
		lt, err := p.expect(tkInt, "LIMIT integer")
		if err != nil {
			return nil, err
		}
		n, err := strconv.Atoi(lt.text)
		if err != nil {
			return nil, fmt.Errorf("%w: bad LIMIT integer: %v", ErrParse, err)
		}
		st.limit = n
	}

	// OFFSET m skips the first m matched rows. Standard order is LIMIT ... OFFSET
	// ...; OFFSET alone (no LIMIT) is also valid — skip m, return the rest.
	if p.peek().kind == tkOffset {
		p.advance()
		ot, err := p.expect(tkInt, "OFFSET integer")
		if err != nil {
			return nil, err
		}
		m, err := strconv.Atoi(ot.text)
		if err != nil {
			return nil, fmt.Errorf("%w: bad OFFSET integer: %v", ErrParse, err)
		}
		st.offset = m
	}

	return st, nil
}

func (p *parser) parseInsert() (*insertStmt, error) {
	p.advance() // INSERT
	if _, err := p.expect(tkInto, "INTO"); err != nil {
		return nil, err
	}
	tn, err := p.expect(tkIdent, "table name")
	if err != nil {
		return nil, err
	}
	st := &insertStmt{table: tn.text}
	if _, err := p.expect(tkLParen, "("); err != nil {
		return nil, err
	}
	for {
		id, err := p.expect(tkIdent, "column name")
		if err != nil {
			return nil, err
		}
		st.cols = append(st.cols, id.text)
		if p.peek().kind != tkComma {
			break
		}
		p.advance()
	}
	if _, err := p.expect(tkRParen, ")"); err != nil {
		return nil, err
	}
	if _, err := p.expect(tkValues, "VALUES"); err != nil {
		return nil, err
	}
	// One or more comma-separated tuples: VALUES (...), (...), ...
	for {
		if _, err := p.expect(tkLParen, "("); err != nil {
			return nil, err
		}
		var tuple []expr
		for {
			v, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			tuple = append(tuple, v)
			if p.peek().kind != tkComma {
				break
			}
			p.advance()
		}
		if _, err := p.expect(tkRParen, ")"); err != nil {
			return nil, err
		}
		if len(tuple) != len(st.cols) {
			return nil, fmt.Errorf("%w: column count (%d) != value count (%d)", ErrParse, len(st.cols), len(tuple))
		}
		st.rows = append(st.rows, tuple)
		if p.peek().kind != tkComma {
			break
		}
		p.advance() // comma between tuples
	}
	return st, nil
}

func (p *parser) parseUpdate() (*updateStmt, error) {
	p.advance() // UPDATE
	tn, err := p.expect(tkIdent, "table name")
	if err != nil {
		return nil, err
	}
	st := &updateStmt{table: tn.text}
	if _, err := p.expect(tkSet, "SET"); err != nil {
		return nil, err
	}
	for {
		id, err := p.expect(tkIdent, "column name")
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tkEq, "="); err != nil {
			return nil, err
		}
		v, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		st.sets = append(st.sets, setAssign{col: id.text, val: v})
		if p.peek().kind != tkComma {
			break
		}
		p.advance()
	}
	if p.peek().kind == tkWhere {
		p.advance()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		st.where = e
	}
	return st, nil
}

func (p *parser) parseDelete() (*deleteStmt, error) {
	p.advance() // DELETE
	if _, err := p.expect(tkFrom, "FROM"); err != nil {
		return nil, err
	}
	tn, err := p.expect(tkIdent, "table name")
	if err != nil {
		return nil, err
	}
	st := &deleteStmt{table: tn.text}
	if p.peek().kind == tkWhere {
		p.advance()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		st.where = e
	}
	return st, nil
}

// Expression grammar, lowest precedence first:
//
//   or_expr  = and_expr (OR and_expr)*
//   and_expr = not_expr (AND not_expr)*
//   not_expr = NOT not_expr | cmp_expr
//   cmp_expr = add_expr (( = | != | <> | < | <= | > | >= | IS [NOT] NULL ) add_expr)?
//   add_expr = mul_expr ((+ | -) mul_expr)*
//   mul_expr = atom (* atom)*
//   atom     = '(' or_expr ')' | column | literal | parameter
//
// Arithmetic (+, -, *) binds tighter than comparison. It is what makes
// `UPDATE ... SET col = col - ?` work; it is also accepted in WHERE.

func (p *parser) parseExpr() (expr, error) {
	return p.parseOr()
}

func (p *parser) parseOr() (expr, error) {
	lhs, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.peek().kind == tkOr {
		p.advance()
		rhs, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		lhs = &binOp{op: tkOr, lhs: lhs, rhs: rhs}
	}
	return lhs, nil
}

func (p *parser) parseAnd() (expr, error) {
	lhs, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.peek().kind == tkAnd {
		p.advance()
		rhs, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		lhs = &binOp{op: tkAnd, lhs: lhs, rhs: rhs}
	}
	return lhs, nil
}

func (p *parser) parseNot() (expr, error) {
	if p.peek().kind == tkNot {
		p.advance()
		if err := p.enter(); err != nil { // chained NOT recurses here
			return nil, err
		}
		inner, err := p.parseNot()
		p.leave()
		if err != nil {
			return nil, err
		}
		return &notExpr{e: inner}, nil
	}
	return p.parseCmp()
}

func (p *parser) parseCmp() (expr, error) {
	lhs, err := p.parseAddSub()
	if err != nil {
		return nil, err
	}
	switch p.peek().kind {
	case tkEq, tkNeq, tkLt, tkLte, tkGt, tkGte:
		op := p.advance().kind
		rhs, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		return &binOp{op: op, lhs: lhs, rhs: rhs}, nil
	case tkIs:
		p.advance()
		not := false
		if p.peek().kind == tkNot {
			p.advance()
			not = true
		}
		if _, err := p.expect(tkNull, "NULL"); err != nil {
			return nil, err
		}
		return &isNullExpr{e: lhs, not: not}, nil
	}
	return lhs, nil
}

func (p *parser) parseAddSub() (expr, error) {
	lhs, err := p.parseMulDiv()
	if err != nil {
		return nil, err
	}
	for p.peek().kind == tkPlus || p.peek().kind == tkMinus {
		op := p.advance().kind
		rhs, err := p.parseMulDiv()
		if err != nil {
			return nil, err
		}
		lhs = &binOp{op: op, lhs: lhs, rhs: rhs}
	}
	return lhs, nil
}

func (p *parser) parseMulDiv() (expr, error) {
	lhs, err := p.parseAtom()
	if err != nil {
		return nil, err
	}
	for p.peek().kind == tkStar {
		p.advance()
		rhs, err := p.parseAtom()
		if err != nil {
			return nil, err
		}
		lhs = &binOp{op: tkStar, lhs: lhs, rhs: rhs}
	}
	return lhs, nil
}

func (p *parser) parseAtom() (expr, error) {
	t := p.peek()
	switch t.kind {
	case tkLParen:
		p.advance()
		if err := p.enter(); err != nil { // nested parens recurse into parseExpr here
			return nil, err
		}
		e, err := p.parseExpr()
		p.leave()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tkRParen, ")"); err != nil {
			return nil, err
		}
		return e, nil
	case tkIdent:
		p.advance()
		cr := &colRef{name: t.text, ord: -1}
		if p.peek().kind == tkDot { // qualified: alias.col
			p.advance()
			c, err := p.expect(tkIdent, "column name after '.'")
			if err != nil {
				return nil, err
			}
			cr.qual = t.text
			cr.name = c.text
		}
		return cr, nil
	case tkInt:
		p.advance()
		n, err := strconv.ParseInt(t.text, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: bad integer literal: %v", ErrParse, err)
		}
		return &litValue{v: Int(n)}, nil
	case tkString:
		p.advance()
		return &litValue{v: Str(t.text)}, nil
	case tkParam:
		p.advance()
		// Index is the running count of params seen so far; assigned in
		// a post-pass in plan() because the parser doesn't track it here.
		return &paramRef{index: -1}, nil
	case tkNull:
		p.advance()
		return &litValue{v: Null()}, nil
	case tkTrue:
		p.advance()
		return &litValue{v: Bool(true)}, nil
	case tkFalse:
		p.advance()
		return &litValue{v: Bool(false)}, nil
	}
	return nil, fmt.Errorf("%w: unexpected token at %d", ErrParse, t.pos)
}
