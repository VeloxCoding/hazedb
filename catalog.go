package hazedb

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
)

// catalog is the immutable set of tables, published behind DB.cat
// (atomic.Pointer). Readers load it once per call and use that snapshot for
// the whole operation — lock-free. DDL (CREATE/DROP) builds a NEW catalog and
// swaps it in atomically, so schema changes never block or slow reads/writes,
// and an in-flight query keeps a consistent view. version bumps on every
// change so a cached plan can tell it is stale and re-bind. DB.ddlMu serialises
// concurrent DDL (the only writers of DB.cat); reads/writes never take it.
type catalog struct {
	version uint64
	byName  map[string]*tableRT
	byID    []*tableRT // index = tableID; a nil slot is a dropped table
}

func (rt *tableRT) name() string { return rt.table.def.def.Name }

// newCatalog builds the bootstrap catalog from the Open() schema. Table IDs
// are the schema order (0..n-1); these are the durable IDs the WAL uses.
func newCatalog(schema Schema, sizeHint int, budget *byteBudget) (*catalog, error) {
	// Table IDs are the schema order (0..n-1) and stamp every WAL mutation as a
	// uint16, so n must fit: n-1 <= MaxUint16, i.e. at most MaxUint16+1 tables.
	if len(schema.Tables) > math.MaxUint16+1 {
		return nil, fmt.Errorf("%w: %d bootstrap tables", ErrTooManyTables, len(schema.Tables))
	}
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
		rt := &tableRT{table: newTable(resolved[td.Name], sizeHint, budget), tableID: uint16(i)}
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
// then atomically swap in the new catalog.
func (db *DB) createTable(td TableDef) error {
	db.ddlMu.Lock()
	defer db.ddlMu.Unlock()
	cur := db.cat.Load()
	if _, exists := cur.byName[td.Name]; exists {
		return fmt.Errorf("%w: %q", ErrTableExists, td.Name)
	}
	// The _hz_ namespace is reserved for the companion's own tables; a user table
	// there would collide with them in the mirror. Match case-insensitively, since
	// SQLite identifiers are.
	if strings.HasPrefix(strings.ToLower(td.Name), reservedTablePrefix) {
		return fmt.Errorf("%w: %q", ErrReservedName, td.Name)
	}
	// Table IDs are never reused after DROP (a dropped slot stays nil and the slice
	// keeps its length), so len(byID) is the count of IDs ever handed out — this caps
	// lifetime creates, not just live tables. A wrapped uint16 ID would overwrite an
	// existing slot and mis-route that table's WAL mutations.
	if len(cur.byID) > math.MaxUint16 {
		return fmt.Errorf("%w: %d table slots used", ErrTooManyTables, len(cur.byID))
	}
	// Names/counts must fit the uint16 catalog WAL codec; a silent truncation there
	// writes a wrong length prefix over full-length bytes and corrupts replay, so the
	// next Open fails. Reject before journaling.
	if err := validateCatalogWireDef(td); err != nil {
		return err
	}
	resolved, err := resolveSchema(Schema{Tables: []TableDef{td}})
	if err != nil {
		return err
	}
	rt := &tableRT{table: newTable(resolved[td.Name], db.sizeHint, db.budget), tableID: uint16(len(cur.byID))}
	if db.wal != nil {
		bp := db.scratch.get()
		*bp = encodeCreateTable(*bp, rt.tableID, td)
		werr := db.wal.writeRecord(recCreateTable, *bp)
		db.scratch.put(bp)
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
	// createTable rejects an oversized name, so an existing name already fits — this
	// keeps the uint16 codec precondition explicit and local to the encode call.
	if err := checkU16(len(name), "table name length"); err != nil {
		return err
	}
	if db.wal != nil {
		bp := db.scratch.get()
		*bp = encodeDropTable(*bp, name)
		werr := db.wal.writeRecord(recDropTable, *bp)
		db.scratch.put(bp)
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
//   then (optional — absent when the record ends after the columns): numIndexes:2 |
//        per index: name(len:2+bytes) | numCols:2 | per col: col(len:2+bytes) |
//        flags:1 (bit 1 = Ordered; bit 0 reserved)
// DROP:   name(len:2+bytes)

// checkU16 rejects a length or count that would not survive the uint16 the catalog
// WAL codec encodes it as: a silent truncation there writes a wrong length prefix
// over full-length bytes, so the record cannot be decoded and replay fails.
func checkU16(n int, what string) error {
	if n > math.MaxUint16 {
		return fmt.Errorf("%w: %s is %d (max %d)", ErrCatalogTooLarge, what, n, math.MaxUint16)
	}
	return nil
}

// validateCatalogWireDef rejects a table definition whose names or counts exceed the
// uint16 fields of encodeCreateTable. Called before journaling a CREATE.
func validateCatalogWireDef(td TableDef) error {
	if err := checkU16(len(td.Name), "table name length"); err != nil {
		return err
	}
	if err := checkU16(len(td.Columns), "column count"); err != nil {
		return err
	}
	for _, c := range td.Columns {
		if err := checkU16(len(c.Name), "column name length"); err != nil {
			return err
		}
	}
	if err := checkU16(len(td.Indexes), "index count"); err != nil {
		return err
	}
	for _, ix := range td.Indexes {
		if err := checkU16(len(ix.Name), "index name length"); err != nil {
			return err
		}
		if err := checkU16(len(ix.Columns), "index column count"); err != nil {
			return err
		}
		for _, col := range ix.Columns {
			if err := checkU16(len(col), "index column name length"); err != nil {
				return err
			}
		}
	}
	return nil
}

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
		buf = appendU16LE(buf, uint16(len(ix.Columns)))
		for _, col := range ix.Columns {
			buf = appendU16LE(buf, uint16(len(col)))
			buf = append(buf, col...)
		}
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
	// Index section is optional: a record that ends exactly after the columns has
	// zero indexes. Anything left that is not a well-formed index section is corrupt.
	if off == len(b) {
		return tableID, td, nil
	}
	if off+2 > len(b) {
		return 0, td, fmt.Errorf("%w: create-table numidx", ErrWALCorrupt)
	}
	nidx := int(binary.LittleEndian.Uint16(b[off : off+2]))
	off += 2
	for i := 0; i < nidx; i++ {
		name, n, err := getLenStr(b[off:])
		if err != nil {
			return 0, td, err
		}
		off += n
		if off+2 > len(b) {
			return 0, td, fmt.Errorf("%w: create-table index numcols", ErrWALCorrupt)
		}
		ncol := int(binary.LittleEndian.Uint16(b[off : off+2]))
		off += 2
		cols := make([]string, 0, ncol)
		for j := 0; j < ncol; j++ {
			col, n, err := getLenStr(b[off:])
			if err != nil {
				return 0, td, err
			}
			off += n
			cols = append(cols, col)
		}
		if off >= len(b) {
			return 0, td, fmt.Errorf("%w: create-table index flags", ErrWALCorrupt)
		}
		flags := b[off]
		off++
		td.Indexes = append(td.Indexes, IndexDef{Name: name, Columns: cols, Ordered: flags&2 != 0})
	}
	if off != len(b) {
		return 0, td, fmt.Errorf("%w: create-table has %d trailing bytes", ErrWALCorrupt, len(b)-off)
	}
	return tableID, td, nil
}

func encodeDropTable(buf []byte, name string) []byte {
	buf = appendU16LE(buf, uint16(len(name)))
	return append(buf, name...)
}

func decodeDropTable(b []byte) (string, error) {
	name, n, err := getLenStr(b)
	if err != nil {
		return "", err
	}
	if n != len(b) {
		return "", fmt.Errorf("%w: drop-table has %d trailing bytes", ErrWALCorrupt, len(b)-n)
	}
	return name, nil
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
