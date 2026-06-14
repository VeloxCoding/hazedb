package hazedb

import (
	"encoding/binary"
	"fmt"
)

// WAL recovery replay. Open replays the undrained WAL tail on top of the SQLite
// base; these apply one decoded record/mutation at a time. Single-threaded
// (runs inside Open before the DB is returned), so the catalog is mutated
// directly through its atomic pointer.

// onWALCorrupt records a corrupt segment found during recovery. Every record
// before the break was already applied; the unparseable suffix is skipped and
// recovery continues with the next segment rather than aborting Open.
func (db *DB) onWALCorrupt(seg uint64, err error) {
	db.logEvent("error", "wal-corruption", fmt.Sprintf("segment %d during recovery: %v — good prefix recovered, suffix skipped", seg, err))
}

// applyReplayRecord applies one decoded WAL record to the in-memory store during
// recovery. It is single-threaded (runs inside Open before the DB is returned),
// so it mutates the catalog directly via the atomic pointer; catalog records
// (CREATE/DROP) precede any mutation referencing the table, so a mutation always
// resolves against an already-rebuilt catalog. Driven by the undrained-tail
// replay (replayFrom): catalog records rebuild the catalog, mutations re-apply rows.
func (db *DB) applyReplayRecord(recType uint8, payload []byte) error {
	switch recType {
	case recCreateTable:
		tableID, td, err := decodeCreateTable(payload)
		if err != nil {
			return err
		}
		resolved, err := resolveSchema(Schema{Tables: []TableDef{td}})
		if err != nil {
			return err
		}
		rt := &tableRT{table: newTable(resolved[td.Name], db.sizeHint, db.budget), tableID: tableID}
		db.cat.Store(db.cat.Load().withTable(rt))
		return nil
	case recDropTable:
		name, err := decodeDropTable(payload)
		if err != nil {
			return err
		}
		db.cat.Store(db.cat.Load().withoutTable(name))
		return nil
	case recCheckpoint:
		return nil // no row state — skip
	case recMutation:
		return db.applyMutationRecord(payload)
	case recTxn:
		// A transaction is a count-prefixed group of sub-mutations, applied in order.
		// The whole group arrived as one CRC-valid envelope, so it is all-or-nothing
		// by construction; a torn group was discarded by the tail check before here.
		return forEachTxnMutation(payload, db.applyMutationRecord)
	}
	return fmt.Errorf("%w: unknown record type %d", ErrWALCorrupt, recType)
}

// applyMutationRecord decodes one op|tableID|op-body mutation record and
// applies it through the table's apply path. Shared by recMutation (one per
// envelope) and recTxn (many per envelope).
func (db *DB) applyMutationRecord(payload []byte) error {
	if len(payload) < 3 {
		return fmt.Errorf("%w: short mutation payload", ErrWALCorrupt)
	}
	op := payload[0]
	tableID := binary.LittleEndian.Uint16(payload[1:3])
	cat := db.cat.Load()
	if int(tableID) >= len(cat.byID) || cat.byID[tableID] == nil {
		return fmt.Errorf("%w: mutation for unknown table id %d", ErrWALCorrupt, tableID)
	}
	return db.applyMutation(cat.byID[tableID], op, payload[3:])
}

// applyMutation re-applies one decoded mutation to a table during replay. A
// CRC-valid record can still be tampered or wrong-typed, and replay writes
// straight into typed in-memory storage (a UUID-keyed pkMap, per-column cells),
// so every cell is validated against the schema and every PK kind-checked before
// it lands — replay fails closed (ErrWALCorrupt) rather than indexing garbage or
// panicking at boot. Validation matches the write path (validateValue +
// coerceToUUID), so it never rejects a record a normal write would have produced.
func (db *DB) applyMutation(rt *tableRT, op uint8, body []byte) error {
	cols := rt.def.def.Columns
	ncols := len(cols)
	switch op {
	case opInsert:
		row, err := decodeRow(body)
		if err != nil {
			return err
		}
		if err := validateInsertRow(cols, row); err != nil {
			return err
		}
		return rt.insert(row)
	case opUpdate:
		var ords []int
		var vals []Value
		pk, err := decodeUpdateMutation(body, func(ord int, v Value) error {
			// Range-check the ordinal before it is used as r[ord], then type-check the
			// SET cell against its column.
			if ord < 0 || ord >= ncols {
				return fmt.Errorf("%w: update ordinal %d out of range [0,%d)", ErrWALCorrupt, ord, ncols)
			}
			if err := validateValue(cols[ord], v); err != nil {
				return fmt.Errorf("%w: update %v", ErrWALCorrupt, err)
			}
			ords = append(ords, ord)
			vals = append(vals, v)
			return nil
		})
		if err != nil {
			return err
		}
		pkU, err := coerceToUUID(pk)
		if err != nil {
			return fmt.Errorf("%w: update %v", ErrWALCorrupt, err)
		}
		if !rt.update(pkU, func(r Row) Row {
			for i := range ords {
				r[ords[i]] = vals[i]
			}
			return r
		}) {
			return fmt.Errorf("%w: update for absent pk during replay", ErrWALCorrupt)
		}
		return nil
	case opDelete:
		pk, err := decodeDeleteBody(body)
		if err != nil {
			return err
		}
		pkU, err := coerceToUUID(pk)
		if err != nil {
			return fmt.Errorf("%w: delete %v", ErrWALCorrupt, err)
		}
		rt.deleteByPK(pkU)
		return nil
	}
	return fmt.Errorf("%w: unknown op %d", ErrWALCorrupt, op)
}
