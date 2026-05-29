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
// bool→BOOL, null→NULL, string→STRING UNLESS it parses as a canonical UUID, in
// which case→UUID. The UUID rule lets the string-only PHP/HTTP surface address
// and insert UUID columns; the canonical 36-char hyphenated form is specific
// enough that a real text value rarely collides.

package hazedb

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// valueToAny converts a cell to a JSON-encodable Go value.
func valueToAny(v Value) any {
	switch v.Kind {
	case KindNull:
		return nil
	case KindInt:
		return v.Int()
	case KindBool:
		return v.Bool()
	case KindString:
		return v.Str()
	case KindBytes:
		return base64.StdEncoding.EncodeToString(v.Bytes())
	case KindUUID:
		return v.UUID().String()
	}
	return nil
}

type rowsEnvelope struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

// RowsToJSON renders a result set as {"columns":[...],"rows":[[...],...]}.
func RowsToJSON(cols []string, rows []Row) ([]byte, error) {
	env := rowsEnvelope{Columns: cols, Rows: make([][]any, len(rows))}
	for i, r := range rows {
		cells := make([]any, len(r))
		for j := range r {
			cells[j] = valueToAny(r[j])
		}
		env.Rows[i] = cells
	}
	return json.Marshal(env)
}

// ExecResultJSON renders a write result as {"affected":n}.
func ExecResultJSON(affected int) []byte {
	b, _ := json.Marshal(struct {
		Affected int `json:"affected"`
	}{affected})
	return b
}

// ErrorJSON renders an error as {"error":"msg"}.
func ErrorJSON(msg string) []byte {
	b, _ := json.Marshal(struct {
		Error string `json:"error"`
	}{msg})
	return b
}

// ArgsFromJSON parses a JSON array of positional SQL args into the []any
// db.Query / db.Exec accept. Empty/nil input yields no args. See the file
// header for the type mapping (notably the canonical-UUID-string rule).
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
	out := make([]any, len(arr))
	for i, a := range arr {
		switch x := a.(type) {
		case nil:
			out[i] = nil
		case bool:
			out[i] = x
		case string:
			if u, err := ParseUUID(x); err == nil {
				out[i] = u
			} else {
				out[i] = x
			}
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
