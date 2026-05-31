package hazedb

import (
	"encoding/binary"
	"fmt"
)

// catalog is the immutable set of tables, published behind DB.cat
// (atomic.Pointer). Readers load it once per call and use that snapshot for
// the whole operation — lock-free. DDL (CREATE/DROP) builds a NEW catalog and
// swaps it in atomically, so schema changes never block or slow reads/writes,
// and an in-flight query keeps a consistent view. version bumps on every
// change so a cached plan can tell it is stale and re-bind.
type catalog struct {
	version uint64
	byName  map[string]*tableRT
	byID    []*tableRT // index = tableID; a nil slot is a dropped table
}

func (rt *tableRT) name() string { return rt.table.def.def.Name }

// newCatalog builds the bootstrap catalog from the Open() schema. Table IDs
// are the schema order (0..n-1); these are the durable IDs the WAL uses.
func newCatalog(schema Schema, sizeHint int) (*catalog, error) {
	resolved, err := resolveSchema(schema)
	if err != nil {
		return nil, err
	}
	c := &catalog{
		version: 1,
		byName:  make(map[string]*tableRT, len(schema.Tables)),
		byID:    make([]*tableRT, 0, len(schema.Tables)),
	}
	for i, td := range schema.Tables {
		rt := &tableRT{table: newTable(resolved[td.Name], sizeHint), tableID: uint16(i)}
		c.byName[td.Name] = rt
		c.byID = append(c.byID, rt)
	}
	return c, nil
}

// withTable returns a copy of c that includes rt at its tableID. Only the
// registry maps/slice are copied (pointers); existing table storage is shared
// and untouched. Cheap because DDL is rare.
func (c *catalog) withTable(rt *tableRT) *catalog {
	nc := &catalog{
		version: c.version + 1,
		byName:  make(map[string]*tableRT, len(c.byName)+1),
		byID:    append([]*tableRT(nil), c.byID...),
	}
	for k, v := range c.byName {
		nc.byName[k] = v
	}
	nc.byName[rt.name()] = rt
	for len(nc.byID) <= int(rt.tableID) {
		nc.byID = append(nc.byID, nil)
	}
	nc.byID[rt.tableID] = rt
	return nc
}

// withoutTable returns a copy of c with name removed (its byID slot nil'd, so
// later tables keep their IDs).
func (c *catalog) withoutTable(name string) *catalog {
	old := c.byName[name]
	nc := &catalog{
		version: c.version + 1,
		byName:  make(map[string]*tableRT, len(c.byName)),
		byID:    append([]*tableRT(nil), c.byID...),
	}
	for k, v := range c.byName {
		if k != name {
			nc.byName[k] = v
		}
	}
	if old != nil && int(old.tableID) < len(nc.byID) {
		nc.byID[old.tableID] = nil
	}
	return nc
}

// createTable is the first-class CREATE: validate the definition, allocate a
// durable table_id, journal the catalog record to the WAL BEFORE publishing,
// then atomically swap in the new catalog. ddlMu serialises concurrent DDL;
// reads/writes never take it.
func (db *DB) createTable(td TableDef) error {
	db.ddlMu.Lock()
	defer db.ddlMu.Unlock()
	cur := db.cat.Load()
	if _, exists := cur.byName[td.Name]; exists {
		return fmt.Errorf("%w: %q", ErrTableExists, td.Name)
	}
	resolved, err := resolveSchema(Schema{Tables: []TableDef{td}})
	if err != nil {
		return err
	}
	rt := &tableRT{table: newTable(resolved[td.Name], db.sizeHint), tableID: uint16(len(cur.byID))}
	if db.wal != nil {
		body := encodeCreateTable(db.scratch.get(), rt.tableID, td)
		werr := db.wal.writeRecord(recCreateTable, body)
		db.scratch.put(body)
		if werr != nil {
			return werr
		}
	}
	db.cat.Store(cur.withTable(rt))
	return nil
}

// dropTable removes a table: journal the drop, then swap in a catalog without
// it. The table's storage is reclaimed by GC once no in-flight call still
// holds the old catalog snapshot.
func (db *DB) dropTable(name string) error {
	db.ddlMu.Lock()
	defer db.ddlMu.Unlock()
	cur := db.cat.Load()
	if _, exists := cur.byName[name]; !exists {
		return fmt.Errorf("%w: %q", ErrUnknownTable, name)
	}
	if db.wal != nil {
		body := encodeDropTable(db.scratch.get(), name)
		werr := db.wal.writeRecord(recDropTable, body)
		db.scratch.put(body)
		if werr != nil {
			return werr
		}
	}
	db.cat.Store(cur.withoutTable(name))
	return nil
}

// --- catalog WAL record encoding ---
//
// CREATE: tableID:2 | name(len:2+bytes) | numCols:2 | per col: name(len:2+bytes) | type:1 | flags:1
//   flags bit 0 = PK, 1 = PartitionKey, 2 = Immutable, 3 = Nullable
//   then (optional, omitted by pre-index records): numIndexes:2 |
//        per index: name(len:2+bytes) | col(len:2+bytes) | flags:1 (bit 1 = Ordered; bit 0 reserved)
// DROP:   name(len:2+bytes)

func encodeCreateTable(buf []byte, tableID uint16, td TableDef) []byte {
	buf = appendU16LE(buf, tableID)
	buf = appendU16LE(buf, uint16(len(td.Name)))
	buf = append(buf, td.Name...)
	buf = appendU16LE(buf, uint16(len(td.Columns)))
	for _, c := range td.Columns {
		buf = appendU16LE(buf, uint16(len(c.Name)))
		buf = append(buf, c.Name...)
		buf = append(buf, byte(c.Type))
		var flags byte
		if c.PK {
			flags |= 1
		}
		if c.PartitionKey {
			flags |= 2
		}
		if c.Immutable {
			flags |= 4
		}
		if c.Nullable {
			flags |= 8
		}
		buf = append(buf, flags)
	}
	buf = appendU16LE(buf, uint16(len(td.Indexes)))
	for _, ix := range td.Indexes {
		buf = appendU16LE(buf, uint16(len(ix.Name)))
		buf = append(buf, ix.Name...)
		buf = appendU16LE(buf, uint16(len(ix.Column)))
		buf = append(buf, ix.Column...)
		var flags byte
		if ix.Ordered {
			flags |= 2
		}
		buf = append(buf, flags)
	}
	return buf
}

func decodeCreateTable(b []byte) (uint16, TableDef, error) {
	var td TableDef
	if len(b) < 4 {
		return 0, td, fmt.Errorf("%w: create-table header", ErrWALCorrupt)
	}
	tableID := binary.LittleEndian.Uint16(b[0:2])
	off := 2
	name, n, err := getLenStr(b[off:])
	if err != nil {
		return 0, td, err
	}
	off += n
	td.Name = name
	if off+2 > len(b) {
		return 0, td, fmt.Errorf("%w: create-table numcols", ErrWALCorrupt)
	}
	ncols := int(binary.LittleEndian.Uint16(b[off : off+2]))
	off += 2
	for i := 0; i < ncols; i++ {
		cn, n, err := getLenStr(b[off:])
		if err != nil {
			return 0, td, err
		}
		off += n
		if off+2 > len(b) {
			return 0, td, fmt.Errorf("%w: create-table col meta", ErrWALCorrupt)
		}
		var c ColumnDef
		c.Name = cn
		c.Type = ColumnType(b[off])
		flags := b[off+1]
		off += 2
		c.PK = flags&1 != 0
		c.PartitionKey = flags&2 != 0
		c.Immutable = flags&4 != 0
		c.Nullable = flags&8 != 0
		td.Columns = append(td.Columns, c)
	}
	// Index section is optional: pre-index WAL records end after the columns.
	if off+2 > len(b) {
		return tableID, td, nil
	}
	nidx := int(binary.LittleEndian.Uint16(b[off : off+2]))
	off += 2
	for i := 0; i < nidx; i++ {
		name, n, err := getLenStr(b[off:])
		if err != nil {
			return 0, td, err
		}
		off += n
		col, n, err := getLenStr(b[off:])
		if err != nil {
			return 0, td, err
		}
		off += n
		if off >= len(b) {
			return 0, td, fmt.Errorf("%w: create-table index flags", ErrWALCorrupt)
		}
		flags := b[off]
		off++
		td.Indexes = append(td.Indexes, IndexDef{Name: name, Column: col, Ordered: flags&2 != 0})
	}
	return tableID, td, nil
}

func encodeDropTable(buf []byte, name string) []byte {
	buf = appendU16LE(buf, uint16(len(name)))
	return append(buf, name...)
}

func decodeDropTable(b []byte) (string, error) {
	name, _, err := getLenStr(b)
	return name, err
}

// getLenStr reads a uint16-length-prefixed string, returning it and the bytes
// consumed.
func getLenStr(b []byte) (string, int, error) {
	if len(b) < 2 {
		return "", 0, fmt.Errorf("%w: string length", ErrWALCorrupt)
	}
	n := int(binary.LittleEndian.Uint16(b[0:2]))
	if 2+n > len(b) {
		return "", 0, fmt.Errorf("%w: string body", ErrWALCorrupt)
	}
	return string(b[2 : 2+n]), 2 + n, nil
}
