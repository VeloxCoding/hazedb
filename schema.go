package hazedb

import (
	"errors"
	"fmt"
	"unicode/utf8"
)

// ColumnType is the declared type of a column. Generic v1 supports a
// small set; codegen (M3) expands to richer types.
type ColumnType uint8

const (
	TypeInt ColumnType = iota + 1
	TypeString
	TypeBytes
	TypeBool
	TypeUUID
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
	case TypeUUID:
		return "UUID"
	}
	return "?"
}

// ColumnDef describes one column at table-create time.
type ColumnDef struct {
	Name string
	Type ColumnType
	// PK marks the single primary-key column. It must be TypeUUID and is
	// implicitly immutable.
	PK bool
	// Immutable rejects UPDATE SET on this column at plan time. Used for an
	// ordering column (e.g. seq) so a tail index can cache its order safely.
	Immutable bool
	// PartitionKey marks the column the table is sharded/ordered by (e.g.
	// thread_id on a messages table). At most one per table; must be UUID in
	// v1; implicitly immutable (a partition move is DELETE + INSERT). Storage
	// routing + the ordered tail index build on it.
	PartitionKey bool
	Nullable     bool
}

// IndexDef declares a secondary index on one non-PK column. Maintained
// asynchronously and read through a hybrid path; see docs/secondary-indexes.md.
// Single-column only in v1 (composite is v1.1+).
type IndexDef struct {
	Name   string // optional; auto-derived as "idx_<col>" when empty
	Column string
	Unique bool
	// Ordered makes this a sorted index (serves equality + ranges + ORDER BY)
	// instead of the default hash index (equality only). A column has one or the
	// other, not both.
	Ordered bool
}

// TableDef defines a table. Exactly one column must have PK=true.
// Multi-column PK is v1.1+.
type TableDef struct {
	Name    string
	Columns []ColumnDef
	Indexes []IndexDef
}

// Schema is the database schema. Tables are addressed by name.
type Schema struct {
	Tables []TableDef
}

// resolvedIndex is the validated form of an IndexDef: the indexed column's
// ordinal plus its uniqueness, resolved once at schema time.
type resolvedIndex struct {
	name    string
	ordinal int
	unique  bool
	ordered bool
}

// resolvedTable is the internal, validated form of a TableDef. Holds
// the column ordinal lookup map used by both parser and executor.
type resolvedTable struct {
	def              TableDef
	colByName        map[string]int
	pkOrdinal        int
	partitionOrdinal int // -1 if the table is not partitioned
	indexes          []resolvedIndex
}

// indexOfColumn returns the resolved index on column ordinal ord, or nil.
func (rt *resolvedTable) indexOfColumn(ord int) *resolvedIndex {
	for i := range rt.indexes {
		if rt.indexes[i].ordinal == ord {
			return &rt.indexes[i]
		}
	}
	return nil
}

// partitioned reports whether the table declares a PartitionKey.
func (rt *resolvedTable) partitioned() bool { return rt.partitionOrdinal >= 0 }

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
			def:              t,
			colByName:        make(map[string]int, len(t.Columns)),
			pkOrdinal:        -1,
			partitionOrdinal: -1,
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
				if c.Type != TypeUUID {
					return nil, fmt.Errorf("schema: table %q PK column %q must be UUID, got %s", t.Name, c.Name, c.Type)
				}
				rt.pkOrdinal = i
			}
			if c.PartitionKey {
				if rt.partitionOrdinal >= 0 {
					return nil, fmt.Errorf("schema: table %q has multiple PartitionKey columns", t.Name)
				}
				if c.Type != TypeUUID {
					return nil, fmt.Errorf("schema: table %q PartitionKey column %q must be UUID, got %s", t.Name, c.Name, c.Type)
				}
				rt.partitionOrdinal = i
			}
		}
		if rt.pkOrdinal < 0 {
			return nil, fmt.Errorf("schema: table %q has no PK column", t.Name)
		}
		if rt.partitionOrdinal == rt.pkOrdinal {
			return nil, fmt.Errorf("schema: table %q PartitionKey must be a different column than the PK", t.Name)
		}
		if rt.partitioned() && len(t.Indexes) > 0 {
			return nil, fmt.Errorf("schema: table %q secondary indexes on partitioned tables are not supported yet", t.Name)
		}
		seenIdxCol := make(map[int]bool, len(t.Indexes))
		seenIdxName := make(map[string]bool, len(t.Indexes))
		for _, ix := range t.Indexes {
			ord, ok := rt.colByName[ix.Column]
			if !ok {
				return nil, fmt.Errorf("schema: table %q INDEX on unknown column %q", t.Name, ix.Column)
			}
			if ord == rt.pkOrdinal {
				return nil, fmt.Errorf("schema: table %q INDEX on PK column %q is redundant", t.Name, ix.Column)
			}
			if seenIdxCol[ord] {
				return nil, fmt.Errorf("schema: table %q has multiple indexes on column %q", t.Name, ix.Column)
			}
			seenIdxCol[ord] = true
			name := ix.Name
			if name == "" {
				name = "idx_" + ix.Column
			}
			if seenIdxName[name] {
				return nil, fmt.Errorf("schema: table %q duplicate index name %q", t.Name, name)
			}
			seenIdxName[name] = true
			rt.indexes = append(rt.indexes, resolvedIndex{name: name, ordinal: ord, unique: ix.Unique, ordered: ix.Ordered})
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
		// STRING is text: valid UTF-8 only. Arbitrary bytes belong in a BYTES
		// column. Checked on the write path; reads never re-validate.
		if !utf8.ValidString(v.Str()) {
			return fmt.Errorf("column %q expects valid UTF-8", c.Name)
		}
	case TypeBytes:
		if v.Kind != KindBytes {
			return fmt.Errorf("column %q expects BYTES, got %v", c.Name, v.Kind)
		}
	case TypeBool:
		if v.Kind != KindBool {
			return fmt.Errorf("column %q expects BOOL, got %v", c.Name, v.Kind)
		}
	case TypeUUID:
		if v.Kind != KindUUID {
			return fmt.Errorf("column %q expects UUID, got %v", c.Name, v.Kind)
		}
	}
	return nil
}
