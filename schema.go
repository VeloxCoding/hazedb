package hazedb

import (
	"errors"
	"fmt"
)

// ColumnType is the declared type of a column. Generic v1 supports a
// small set; codegen (M3) expands to richer types.
type ColumnType uint8

const (
	TypeInt ColumnType = iota + 1
	TypeString
	TypeBytes
	TypeBool
)

func (t ColumnType) String() string {
	switch t {
	case TypeInt:
		return "INT"
	case TypeString:
		return "STRING"
	case TypeBytes:
		return "BYTES"
	case TypeBool:
		return "BOOL"
	}
	return "?"
}

// ColumnDef describes one column at table-create time.
type ColumnDef struct {
	Name     string
	Type     ColumnType
	PK       bool
	Nullable bool
}

// TableDef defines a table. Exactly one column must have PK=true.
// Multi-column PK is v1.1+.
type TableDef struct {
	Name    string
	Columns []ColumnDef
}

// Schema is the database schema. Tables are addressed by name.
type Schema struct {
	Tables []TableDef
}

// resolvedTable is the internal, validated form of a TableDef. Holds
// the column ordinal lookup map used by both parser and executor.
type resolvedTable struct {
	def      TableDef
	colByName map[string]int
	pkOrdinal int
}

func resolveSchema(s Schema) (map[string]*resolvedTable, error) {
	out := make(map[string]*resolvedTable, len(s.Tables))
	for _, t := range s.Tables {
		if t.Name == "" {
			return nil, errors.New("schema: table with empty name")
		}
		if _, dup := out[t.Name]; dup {
			return nil, fmt.Errorf("schema: duplicate table %q", t.Name)
		}
		rt := &resolvedTable{
			def:       t,
			colByName: make(map[string]int, len(t.Columns)),
			pkOrdinal: -1,
		}
		for i, c := range t.Columns {
			if c.Name == "" {
				return nil, fmt.Errorf("schema: table %q column %d has empty name", t.Name, i)
			}
			if _, dup := rt.colByName[c.Name]; dup {
				return nil, fmt.Errorf("schema: table %q duplicate column %q", t.Name, c.Name)
			}
			rt.colByName[c.Name] = i
			if c.PK {
				if rt.pkOrdinal >= 0 {
					return nil, fmt.Errorf("schema: table %q has multiple PK columns", t.Name)
				}
				rt.pkOrdinal = i
			}
		}
		if rt.pkOrdinal < 0 {
			return nil, fmt.Errorf("schema: table %q has no PK column", t.Name)
		}
		out[t.Name] = rt
	}
	return out, nil
}

// validateValue checks a Value against a ColumnDef. Used at INSERT
// and UPDATE time.
func validateValue(c ColumnDef, v Value) error {
	if v.IsNull() {
		if !c.Nullable {
			return fmt.Errorf("column %q is NOT NULL", c.Name)
		}
		return nil
	}
	switch c.Type {
	case TypeInt:
		if v.Kind != KindInt {
			return fmt.Errorf("column %q expects INT, got %v", c.Name, v.Kind)
		}
	case TypeString:
		if v.Kind != KindString {
			return fmt.Errorf("column %q expects STRING, got %v", c.Name, v.Kind)
		}
	case TypeBytes:
		if v.Kind != KindBytes {
			return fmt.Errorf("column %q expects BYTES, got %v", c.Name, v.Kind)
		}
	case TypeBool:
		if v.Kind != KindBool {
			return fmt.Errorf("column %q expects BOOL, got %v", c.Name, v.Kind)
		}
	}
	return nil
}
