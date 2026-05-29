package hazedb

import (
	"fmt"
	"strings"
	"unicode"
)

// tokenKind is the lexical category.
type tokenKind uint8

const (
	tkEOF tokenKind = iota
	tkIdent
	tkInt
	tkString
	tkParam // ?
	tkComma
	tkLParen
	tkRParen
	tkSemi
	tkStar
	tkPlus  // +
	tkMinus // -
	tkEq    // =
	tkNeq   // != or <>
	tkLt
	tkLte
	tkGt
	tkGte
	// keywords
	tkSelect
	tkFrom
	tkWhere
	tkOrder
	tkBy
	tkAsc
	tkDesc
	tkLimit
	tkInsert
	tkInto
	tkValues
	tkUpdate
	tkSet
	tkDelete
	tkAnd
	tkOr
	tkNot
	tkNull
	tkTrue
	tkFalse
	tkIs
	tkCreate
	tkTable
	tkDrop
)

var keywords = map[string]tokenKind{
	"select": tkSelect,
	"from":   tkFrom,
	"where":  tkWhere,
	"order":  tkOrder,
	"by":     tkBy,
	"asc":    tkAsc,
	"desc":   tkDesc,
	"limit":  tkLimit,
	"insert": tkInsert,
	"into":   tkInto,
	"values": tkValues,
	"update": tkUpdate,
	"set":    tkSet,
	"delete": tkDelete,
	"and":    tkAnd,
	"or":     tkOr,
	"not":    tkNot,
	"null":   tkNull,
	"true":   tkTrue,
	"false":  tkFalse,
	"is":     tkIs,
	"create": tkCreate,
	"table":  tkTable,
	"drop":   tkDrop,
}

type token struct {
	kind tokenKind
	// text holds the raw lexeme for identifiers (lowercased), the string
	// body for tkString (unquoted, escapes decoded), and the digits for
	// tkInt. Empty otherwise.
	text string
	pos  int
}

// tokenize splits s into tokens. Whitespace and SQL comments
// (-- to end-of-line) are skipped. Identifiers are lowercased; string
// literals are single-quoted with ” as an escaped quote.
func tokenize(s string) ([]token, error) {
	var out []token
	i := 0
	for i < len(s) {
		c := s[i]
		// whitespace
		if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
			i++
			continue
		}
		// line comment
		if c == '-' && i+1 < len(s) && s[i+1] == '-' {
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		}
		switch c {
		case ',':
			out = append(out, token{kind: tkComma, pos: i})
			i++
			continue
		case '(':
			out = append(out, token{kind: tkLParen, pos: i})
			i++
			continue
		case ')':
			out = append(out, token{kind: tkRParen, pos: i})
			i++
			continue
		case ';':
			out = append(out, token{kind: tkSemi, pos: i})
			i++
			continue
		case '*':
			out = append(out, token{kind: tkStar, pos: i})
			i++
			continue
		case '+':
			out = append(out, token{kind: tkPlus, pos: i})
			i++
			continue
		case '-':
			// A '--' comment was already handled above; a lone '-' is the
			// subtraction operator (arithmetic SET, e.g. col - ?).
			out = append(out, token{kind: tkMinus, pos: i})
			i++
			continue
		case '?':
			out = append(out, token{kind: tkParam, pos: i})
			i++
			continue
		case '=':
			out = append(out, token{kind: tkEq, pos: i})
			i++
			continue
		case '!':
			if i+1 < len(s) && s[i+1] == '=' {
				out = append(out, token{kind: tkNeq, pos: i})
				i += 2
				continue
			}
			return nil, fmt.Errorf("%w: unexpected '!' at %d", ErrParse, i)
		case '<':
			if i+1 < len(s) && s[i+1] == '=' {
				out = append(out, token{kind: tkLte, pos: i})
				i += 2
				continue
			}
			if i+1 < len(s) && s[i+1] == '>' {
				out = append(out, token{kind: tkNeq, pos: i})
				i += 2
				continue
			}
			out = append(out, token{kind: tkLt, pos: i})
			i++
			continue
		case '>':
			if i+1 < len(s) && s[i+1] == '=' {
				out = append(out, token{kind: tkGte, pos: i})
				i += 2
				continue
			}
			out = append(out, token{kind: tkGt, pos: i})
			i++
			continue
		case '\'':
			// string literal — single-quoted, '' escapes a quote
			start := i
			i++
			var b strings.Builder
			for i < len(s) {
				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						b.WriteByte('\'')
						i += 2
						continue
					}
					i++
					out = append(out, token{kind: tkString, text: b.String(), pos: start})
					goto nextTok
				}
				b.WriteByte(s[i])
				i++
			}
			return nil, fmt.Errorf("%w: unterminated string at %d", ErrParse, start)
		nextTok:
			continue
		}

		// integers (positive; unary minus handled in parser)
		if c >= '0' && c <= '9' {
			start := i
			for i < len(s) && s[i] >= '0' && s[i] <= '9' {
				i++
			}
			out = append(out, token{kind: tkInt, text: s[start:i], pos: start})
			continue
		}

		// identifier or keyword
		if isIdentStart(c) {
			start := i
			for i < len(s) && isIdentPart(s[i]) {
				i++
			}
			word := strings.ToLower(s[start:i])
			if kw, ok := keywords[word]; ok {
				out = append(out, token{kind: kw, text: word, pos: start})
			} else {
				out = append(out, token{kind: tkIdent, text: word, pos: start})
			}
			continue
		}

		return nil, fmt.Errorf("%w: unexpected char %q at %d", ErrParse, c, i)
	}
	out = append(out, token{kind: tkEOF, pos: i})
	return out, nil
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c >= 0x80 && unicode.IsLetter(rune(c))
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}
