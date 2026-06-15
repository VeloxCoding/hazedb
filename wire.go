// wire.go — JSON wire helpers shared by the out-of-core adapters (the Caddy
// HTTP module and the FrankenPHP/PHP extension). Both need the same two
// translations: SQL args in as JSON, result rows out as JSON. Keeping the
// mapping here means one definition, one documented contract, no duplication
// across the two separate adapter modules.
//
// This is the interim home for the "protocol boundary" the RFC sketches as a
// future bridge/ package: db.go stays the Go API boundary; these helpers turn
// its Go types into bytes a non-Go caller can read.
//
// Value → JSON: null→null, INT→number, BOOL→bool, STRING→string,
// BYTES→base64 string, UUID→canonical string.
//
// JSON arg → Value: number→INT (integers only — hazedb has no float type),
// bool→BOOL, null→NULL, string→STRING. A string stays a STRING here regardless of
// shape; the type is NOT guessed at the arg boundary. A string destined for a UUID
// column is parsed into a UUID later, driven by the COLUMN type the planner knows
// (coerceParams / coerceToUUID / buildRowFromTmpl), so a UUID column addressed by a
// string works and a STRING column holding a canonical-UUID-form value stays text.
// Same rule for the native []any / typed-Value path (toValue) — one consistent
// behaviour across every arg surface.

package hazedb

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// RowsToJSON renders a result set as {"columns":[...],"rows":[[...],...]}.
//
// Hand-rolled: it appends each Value straight into one capacity-hinted buffer,
// with no [][]any intermediate, no interface boxing, and no reflection. On a hot
// read path (one PHP call per request) that is faster than encoding/json and
// collapses the per-row allocations into a single buffer allocation. The returned
// []byte is the caller's to keep or copy (the PHP adapter copies it into a
// zend_string immediately). Never errors — the signature keeps the error for API
// symmetry with the rest of wire.go.
func RowsToJSON(cols []string, rows []Row) ([]byte, error) {
	ncols := len(cols)
	// Rough capacity: envelope + columns + ~24 B per cell. Over-estimating
	// avoids regrow; under-estimating just costs one extra append-grow.
	est := 24 + 12*ncols + len(rows)*(4+24*ncols)
	return appendJSONRows(make([]byte, 0, est), cols, rows), nil
}

// RowsToJSONObjects renders a result set as a JSON array of objects keyed by
// column name — [{"col":val,...},...] — the shape a fetchall-style caller
// forwards straight to an HTTP/JSON response. Same hand-rolled, single-buffer
// approach as RowsToJSON; never errors (error kept for API symmetry).
func RowsToJSONObjects(cols []string, rows []Row) ([]byte, error) {
	ncols := len(cols)
	est := 2 + len(rows)*(8+ncols*24)
	b := make([]byte, 0, est)
	b = append(b, '[')
	for i := range rows {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendRowJSONObject(b, cols, rows[i])
	}
	return append(b, ']'), nil
}

// RowToJSONObject renders ONE row as a single JSON object {"col":val,...} — the
// shape a PK / single-row read returns (vs RowsToJSONObjects' array). Never
// errors (kept for API symmetry).
func RowToJSONObject(cols []string, row Row) ([]byte, error) {
	return appendRowJSONObject(make([]byte, 0, 2+len(cols)*24), cols, row), nil
}

// appendRowJSONObject appends one row as a {"col":val,...} object. Shared by
// RowsToJSONObjects and the streaming QueryJSON, so the object shape has one
// definition. The row may alias live arena storage (QueryJSON encodes under the
// shard lock); every cell is copied into b here, so nothing is retained.
func appendRowJSONObject(b []byte, cols []string, row Row) []byte {
	b = append(b, '{')
	for j, cell := range row {
		if j > 0 {
			b = append(b, ',')
		}
		if j < len(cols) {
			b = appendJSONString(b, cols[j])
		} else {
			b = appendJSONString(b, "")
		}
		b = append(b, ':')
		b = appendValueJSON(b, cell)
	}
	return append(b, '}')
}

// appendRowJSONObjectProject appends a projected row as a {"col":val,...}
// object: cols[j] keys row[ords[j]]. The PK→JSON read path uses it to encode
// only the SELECTed columns straight from the live row under the shard lock
// (every cell is copied into b, so nothing is retained). SELECT * passes ords
// nil and uses appendRowJSONObject instead.
func appendRowJSONObjectProject(b []byte, cols []string, row Row, ords []int) []byte {
	b = append(b, '{')
	for j, ord := range ords {
		if j > 0 {
			b = append(b, ',')
		}
		if j < len(cols) {
			b = appendJSONString(b, cols[j])
		} else {
			b = appendJSONString(b, "")
		}
		b = append(b, ':')
		b = appendValueJSON(b, row[ord])
	}
	return append(b, '}')
}

// appendRowJSONObjectPre appends one row as a {"col":val,...} object using
// pre-escaped key fragments (each prefix is `"col":`, built once at plan time),
// so the constant column names are not re-escaped per row. The row may alias
// live arena storage; every cell is copied into b here, so nothing is retained.
func appendRowJSONObjectPre(b []byte, prefix [][]byte, row Row) []byte {
	b = append(b, '{')
	for j, cell := range row {
		if j > 0 {
			b = append(b, ',')
		}
		if j < len(prefix) {
			b = append(b, prefix[j]...)
		} else {
			b = append(b, '"', '"', ':')
		}
		b = appendValueJSON(b, cell)
	}
	return append(b, '}')
}

const jsonHexDigits = "0123456789abcdef"

// appendJSONString appends s as a JSON string literal, escaping only the
// characters JSON requires. The common case (no escapes) is one bulk append.
func appendJSONString(b []byte, s string) []byte {
	b = append(b, '"')
	last := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' || c == '\\' || c < 0x20 {
			b = append(b, s[last:i]...)
			switch c {
			case '"':
				b = append(b, '\\', '"')
			case '\\':
				b = append(b, '\\', '\\')
			case '\n':
				b = append(b, '\\', 'n')
			case '\r':
				b = append(b, '\\', 'r')
			case '\t':
				b = append(b, '\\', 't')
			default:
				b = append(b, '\\', 'u', '0', '0', jsonHexDigits[c>>4], jsonHexDigits[c&0xf])
			}
			last = i + 1
		}
	}
	b = append(b, s[last:]...)
	return append(b, '"')
}

// appendUUIDJSON appends a UUID as a quoted canonical 36-char string, writing
// hex directly into b with no intermediate string allocation.
func appendUUIDJSON(b []byte, u UUID) []byte {
	b = append(b, '"')
	off := 0
	for i, n := range [5]int{4, 2, 2, 2, 6} {
		if i > 0 {
			b = append(b, '-')
		}
		for k := 0; k < n; k++ {
			c := u[off]
			off++
			b = append(b, jsonHexDigits[c>>4], jsonHexDigits[c&0xf])
		}
	}
	return append(b, '"')
}

// appendValueJSON appends one cell. Bytes render as a base64 string (the wire
// format for byte columns); every kind appends straight into b with no alloc.
func appendValueJSON(b []byte, v Value) []byte {
	switch v.Kind {
	case KindNull:
		return append(b, 'n', 'u', 'l', 'l')
	case KindInt:
		return strconv.AppendInt(b, v.Int(), 10)
	case KindBool:
		if v.Bool() {
			return append(b, 't', 'r', 'u', 'e')
		}
		return append(b, 'f', 'a', 'l', 's', 'e')
	case KindString:
		return appendJSONString(b, v.Str())
	case KindBytes:
		// base64 output is all [A-Za-z0-9+/=] — no JSON escaping needed — so append
		// it straight into b between quotes, skipping both the intermediate string
		// and appendJSONString's escape scan.
		b = append(b, '"')
		b = base64.StdEncoding.AppendEncode(b, v.Bytes())
		return append(b, '"')
	case KindUUID:
		return appendUUIDJSON(b, v.UUID())
	}
	return append(b, 'n', 'u', 'l', 'l')
}

// appendJSONRows writes the {"columns":...,"rows":...} envelope into b and
// returns the grown slice. Split out so a hot caller can reuse a scratch buffer
// (pass b[:0]) for zero steady-state allocations.
func appendJSONRows(b []byte, cols []string, rows []Row) []byte {
	b = append(b, `{"columns":[`...)
	for i, c := range cols {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendJSONString(b, c)
	}
	b = append(b, `],"rows":[`...)
	for i := range rows {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '[')
		for j, cell := range rows[i] {
			if j > 0 {
				b = append(b, ',')
			}
			b = appendValueJSON(b, cell)
		}
		b = append(b, ']')
	}
	return append(b, ']', '}')
}

// ExecResultJSON renders a write result as {"affected":n}. Hand-appended: the
// shape is fixed, so reflection (json.Marshal) would be pure overhead on every
// HTTP write response.
func ExecResultJSON(affected int) []byte {
	b := make([]byte, 0, 24)
	b = append(b, `{"affected":`...)
	b = strconv.AppendInt(b, int64(affected), 10)
	return append(b, '}')
}

// ErrorJSON renders an error as {"error":"msg"}.
func ErrorJSON(msg string) []byte {
	b, _ := json.Marshal(struct {
		Error string `json:"error"`
	}{msg})
	return b
}

// QueryArgs parses an adapter's args parameter in either form:
//
//   - ""                  → no args
//   - starts with '['     → a JSON array (ArgsFromJSON; multi-arg / typed / writes)
//   - anything else       → ONE positional arg passed directly as a STRING,
//     verbatim (surrounding whitespace is part of the value). A UUID column
//     addressed this way is parsed from the string downstream by column type —
//     see the file header.
//
// The direct form lets a caller pass a key it already has — e.g. the UUID from
// a clicked link — straight into `WHERE id = ?` with no json_encode wrapping,
// the common single-key read. For integer args or several args, use the JSON
// array form so types are unambiguous.
func QueryArgs(s string) ([]any, error) {
	t := strings.TrimSpace(s)
	if t == "" {
		return nil, nil
	}
	if t[0] == '[' {
		return ArgsFromJSON([]byte(t))
	}
	// One positional STRING arg, returned verbatim (the trim only classified
	// empty / '['). A UUID column receiving it is coerced downstream by column type.
	return []any{s}, nil
}

// ArgsFromJSON parses a JSON array of positional SQL args into the []any
// db.Query / db.Exec accept. Empty/nil input yields no args. See the file header
// for the type mapping (strings stay STRING here; UUID coercion is by column type).
func ArgsFromJSON(raw []byte) ([]any, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var arr []any
	if err := dec.Decode(&arr); err != nil {
		return nil, fmt.Errorf("args: %w", err)
	}
	// Decode consumes only the FIRST JSON value, so "[1] 2" would otherwise pass
	// silently with args=[1]. A well-formed stream reads EOF next; anything else
	// (a second value, or junk) is malformed.
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("args: unexpected data after the JSON array")
	}
	out := make([]any, len(arr))
	for i, a := range arr {
		switch x := a.(type) {
		case nil:
			out[i] = nil
		case bool:
			out[i] = x
		case string:
			out[i] = x
		case json.Number:
			n, err := x.Int64()
			if err != nil {
				return nil, fmt.Errorf("args[%d]: only integer numbers are supported, got %q", i, x.String())
			}
			out[i] = n
		default:
			return nil, fmt.Errorf("args[%d]: unsupported JSON type %T", i, a)
		}
	}
	return out, nil
}
