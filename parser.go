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
	toks []token
	pos  int
}

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
	}
	return nil, fmt.Errorf("%w: unexpected token at %d", ErrParse, p.peek().pos)
}

func (p *parser) parseSelect() (*selectStmt, error) {
	p.advance() // SELECT
	st := &selectStmt{limit: -1}

	if p.peek().kind == tkStar {
		p.advance()
		st.starAll = true
	} else {
		for {
			id, err := p.expect(tkIdent, "column name")
			if err != nil {
				return nil, err
			}
			st.cols = append(st.cols, resultCol{col: id.text})
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
		oc, err := p.expect(tkIdent, "ORDER BY column")
		if err != nil {
			return nil, err
		}
		st.orderCol = oc.text
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
	if _, err := p.expect(tkLParen, "("); err != nil {
		return nil, err
	}
	for {
		v, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		st.vals = append(st.vals, v)
		if p.peek().kind != tkComma {
			break
		}
		p.advance()
	}
	if _, err := p.expect(tkRParen, ")"); err != nil {
		return nil, err
	}
	if len(st.cols) != len(st.vals) {
		return nil, fmt.Errorf("%w: column count (%d) != value count (%d)", ErrParse, len(st.cols), len(st.vals))
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
//   cmp_expr = atom (( = | != | <> | < | <= | > | >= | IS [NOT] NULL ) atom)?
//   atom     = '(' or_expr ')' | column | literal | parameter

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
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &notExpr{e: inner}, nil
	}
	return p.parseCmp()
}

func (p *parser) parseCmp() (expr, error) {
	lhs, err := p.parseAtom()
	if err != nil {
		return nil, err
	}
	switch p.peek().kind {
	case tkEq, tkNeq, tkLt, tkLte, tkGt, tkGte:
		op := p.advance().kind
		rhs, err := p.parseAtom()
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

func (p *parser) parseAtom() (expr, error) {
	t := p.peek()
	switch t.kind {
	case tkLParen:
		p.advance()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tkRParen, ")"); err != nil {
			return nil, err
		}
		return e, nil
	case tkIdent:
		p.advance()
		return &colRef{name: t.text}, nil
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
