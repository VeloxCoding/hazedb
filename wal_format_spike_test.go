package hazedb

// WAL-format spike — PRESERVED decision artifact for RFC rev. 7 (M4).
//
// Stays in package hazedb (not spike/, which is package spike) on purpose:
// the SQL-string replay arm is only a fair comparison because it runs the
// REAL parser + planner + execInsert/execUpdate/execDelete. Moving it to a
// separate package would force a re-implemented executor and void the
// comparison. As a _test.go it never ships in the binary.
//
// Compares three candidate WAL record encodings on the two dimensions that
// matter: on-disk write size per record, and decode+apply (replay) cost.
// Encode CPU/allocs come for free from the same benchmarks.
//
// The three formats:
//
//   Physical  — op + tableID + FULL row image (insert and update). This is
//               what wal.go does today. Replay applies straight to the
//               arena, no parse, no validation.
//   Typed     — logical mutation without SQL text. insert == full row;
//               UPDATE carries only pk + (ordinal,value) deltas. Replay
//               applies straight to the arena.
//   SQLStr    — RFC's chosen logical form: SQL string + typed params per
//               record. Replay goes through prepare (stmt-cache) + the real
//               execInsert/execUpdate pipeline (parse-once, then eval).
//
// All three share ONE value codec (putVal/getVal below) so size and CPU
// differences come from record STRUCTURE, not codec quirks. The decision
// rule: do not adopt SQLStr unless it is clearly simpler without hurting
// write size or replay materially vs Typed.

import (
	"encoding/binary"
	"testing"
)

// --- shared value codec (size-returning, so records compose) ---

func putU16(b []byte, v uint16) []byte {
	var u [2]byte
	binary.LittleEndian.PutUint16(u[:], v)
	return append(b, u[:]...)
}

func putU32(b []byte, v uint32) []byte {
	var u [4]byte
	binary.LittleEndian.PutUint32(u[:], v)
	return append(b, u[:]...)
}

func putVal(buf []byte, v Value) []byte {
	buf = append(buf, byte(v.Kind))
	switch v.Kind {
	case KindInt, KindBool:
		var u [8]byte
		binary.LittleEndian.PutUint64(u[:], uint64(v.I))
		buf = append(buf, u[:]...)
	case KindString:
		buf = putU32(buf, uint32(len(v.S)))
		buf = append(buf, v.S...)
	case KindBytes:
		buf = putU32(buf, uint32(len(v.B)))
		buf = append(buf, v.B...)
	case KindUUID:
		buf = append(buf, v.U[:]...)
	case KindNull:
		// kind byte only
	}
	return buf
}

func getVal(b []byte) (Value, int) {
	kind := ValueKind(b[0])
	off := 1
	switch kind {
	case KindInt, KindBool:
		v := int64(binary.LittleEndian.Uint64(b[off : off+8]))
		return Value{Kind: kind, I: v}, off + 8
	case KindString:
		ln := int(binary.LittleEndian.Uint32(b[off : off+4]))
		off += 4
		return Value{Kind: kind, S: string(b[off : off+ln])}, off + ln
	case KindBytes:
		ln := int(binary.LittleEndian.Uint32(b[off : off+4]))
		off += 4
		cp := make([]byte, ln)
		copy(cp, b[off:off+ln])
		return Value{Kind: kind, B: cp}, off + ln
	case KindUUID:
		var u UUID
		copy(u[:], b[off:off+16])
		return Value{Kind: kind, U: u}, off + 16
	case KindNull:
		return Value{Kind: KindNull}, off
	}
	return Value{}, off
}

func putRow(buf []byte, r Row) []byte {
	buf = putU16(buf, uint16(len(r)))
	for _, v := range r {
		buf = putVal(buf, v)
	}
	return buf
}

func getRow(b []byte) (Row, int) {
	n := int(binary.LittleEndian.Uint16(b[0:2]))
	off := 2
	r := make(Row, n)
	for i := 0; i < n; i++ {
		v, k := getVal(b[off:])
		r[i] = v
		off += k
	}
	return r, off
}

// --- record encoders (op+tableID header is 3 bytes for Phys/Typed; SQLStr
//     needs no op/table field because the SQL text carries both) ---

const spikeTableID uint16 = 0

func encPhysOrTypedIns(buf []byte, row Row) []byte {
	buf = append(buf, opInsert)
	buf = putU16(buf, spikeTableID)
	return putRow(buf, row)
}

func encPhysUpd(buf []byte, fullRow Row) []byte {
	buf = append(buf, opUpdate)
	buf = putU16(buf, spikeTableID)
	return putRow(buf, fullRow)
}

func encTypedUpd(buf []byte, pk Value, ords []int, vals Row) []byte {
	buf = append(buf, opUpdate)
	buf = putU16(buf, spikeTableID)
	buf = putVal(buf, pk)
	buf = append(buf, byte(len(ords)))
	for i, o := range ords {
		buf = putU16(buf, uint16(o))
		buf = putVal(buf, vals[i])
	}
	return buf
}

func encSQL(buf []byte, sql string, params Row) []byte {
	buf = putU32(buf, uint32(len(sql)))
	buf = append(buf, sql...)
	return putRow(buf, params)
}

// --- workload shapes ---

const spikeBody = "the quick brown fox jumps over the lazy dog, then does it once more for length"

func spikeID(i int) UUID { return tid(i) }

// messages: id(UUID PK), conv(int), seq(int), body(~78B str)
func msgSchema() Schema {
	return Schema{Tables: []TableDef{{
		Name: "messages",
		Columns: []ColumnDef{
			{Name: "id", Type: TypeUUID, PK: true},
			{Name: "conv", Type: TypeInt},
			{Name: "seq", Type: TypeInt},
			{Name: "body", Type: TypeString},
		},
	}}}
}

func msgRow(i int, body string) Row {
	return Row{UUIDVal(spikeID(i)), Int(int64(i)), Int(int64(i)), Str(body)}
}

const (
	insSQL = "INSERT INTO messages (id, conv, seq, body) VALUES (?, ?, ?, ?)"
	updSQL = "UPDATE messages SET body = ? WHERE id = ?"
	delSQL = "DELETE FROM messages WHERE id = ?"
)

// delete carries only the PK in every format. Physical and Typed are
// byte-identical here (op+tableID+pk); SQLStr adds the statement text.
func encPhysOrTypedDel(buf []byte, pk Value) []byte {
	buf = append(buf, opDelete)
	buf = putU16(buf, spikeTableID)
	return putVal(buf, pk)
}

// wide: id + 8 string cols (~20B each); UPDATE touches one col. Stresses the
// full-row (Physical) vs delta (Typed/SQLStr) gap.
func wideRow(i int) Row {
	r := make(Row, 9)
	r[0] = UUIDVal(spikeID(i))
	for j := 1; j < 9; j++ {
		r[j] = Str("colvalue_padding_xx")
	}
	return r
}

const wideUpdSQL = "UPDATE wide SET c1 = ? WHERE id = ?"

// =====================  ENCODE (size + CPU + allocs)  =====================

func benchEnc(b *testing.B, enc func(buf []byte) []byte) {
	buf := make([]byte, 0, 1024)
	recLen := len(enc(buf[:0]))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = enc(buf[:0])
	}
	_ = buf
	b.ReportMetric(float64(recLen), "B/rec")
}

func BenchmarkWALEnc_Insert_Physical(b *testing.B) {
	row := msgRow(1, spikeBody)
	benchEnc(b, func(buf []byte) []byte { return encPhysOrTypedIns(buf, row) })
}
func BenchmarkWALEnc_Insert_Typed(b *testing.B) {
	row := msgRow(1, spikeBody)
	benchEnc(b, func(buf []byte) []byte { return encPhysOrTypedIns(buf, row) })
}
func BenchmarkWALEnc_Insert_SQLStr(b *testing.B) {
	row := msgRow(1, spikeBody)
	benchEnc(b, func(buf []byte) []byte { return encSQL(buf, insSQL, row) })
}

func BenchmarkWALEnc_UpdNarrow_Physical(b *testing.B) {
	row := msgRow(1, spikeBody)
	benchEnc(b, func(buf []byte) []byte { return encPhysUpd(buf, row) })
}
func BenchmarkWALEnc_UpdNarrow_Typed(b *testing.B) {
	pk := UUIDVal(spikeID(1))
	vals := Row{Str(spikeBody)}
	benchEnc(b, func(buf []byte) []byte { return encTypedUpd(buf, pk, []int{3}, vals) })
}
func BenchmarkWALEnc_UpdNarrow_SQLStr(b *testing.B) {
	params := Row{Str(spikeBody), UUIDVal(spikeID(1))}
	benchEnc(b, func(buf []byte) []byte { return encSQL(buf, updSQL, params) })
}

func BenchmarkWALEnc_UpdWide_Physical(b *testing.B) {
	row := wideRow(1)
	benchEnc(b, func(buf []byte) []byte { return encPhysUpd(buf, row) })
}
func BenchmarkWALEnc_UpdWide_Typed(b *testing.B) {
	pk := UUIDVal(spikeID(1))
	vals := Row{Str("colvalue_padding_xx")}
	benchEnc(b, func(buf []byte) []byte { return encTypedUpd(buf, pk, []int{1}, vals) })
}
func BenchmarkWALEnc_UpdWide_SQLStr(b *testing.B) {
	params := Row{Str("colvalue_padding_xx"), UUIDVal(spikeID(1))}
	benchEnc(b, func(buf []byte) []byte { return encSQL(buf, wideUpdSQL, params) })
}

func BenchmarkWALEnc_Delete_Physical(b *testing.B) {
	pk := UUIDVal(spikeID(1))
	benchEnc(b, func(buf []byte) []byte { return encPhysOrTypedDel(buf, pk) })
}
func BenchmarkWALEnc_Delete_Typed(b *testing.B) {
	pk := UUIDVal(spikeID(1))
	benchEnc(b, func(buf []byte) []byte { return encPhysOrTypedDel(buf, pk) })
}
func BenchmarkWALEnc_Delete_SQLStr(b *testing.B) {
	params := Row{UUIDVal(spikeID(1))}
	benchEnc(b, func(buf []byte) []byte { return encSQL(buf, delSQL, params) })
}

// =====================  REPLAY (decode + apply)  =====================

const spikeCorpus = 20000

func newMsgDB(tb testing.TB) *DB {
	db, err := Open(Options{Schema: msgSchema(), SizeHint: spikeCorpus})
	if err != nil {
		tb.Fatal(err)
	}
	return db
}

func newMsgDBFilled(tb testing.TB) *DB {
	db := newMsgDB(tb)
	for i := 0; i < spikeCorpus; i++ {
		if err := db.t["messages"].insert(msgRow(i, spikeBody)); err != nil {
			tb.Fatal(err)
		}
	}
	return db
}

// benchReplay applies a fixed pre-encoded corpus. For insert workloads it
// rebuilds the DB each wrap (StopTimer-guarded) so re-applying the same ids
// doesn't hit duplicate-PK short-circuits and skew the result.
func benchReplay(b *testing.B, corpus [][]byte, makeDB func(testing.TB) *DB, rebuildOnWrap bool, apply func(*DB, []byte)) {
	m := len(corpus)
	db := makeDB(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if rebuildOnWrap && i > 0 && i%m == 0 {
			b.StopTimer()
			db = makeDB(b)
			b.StartTimer()
		}
		apply(db, corpus[i%m])
	}
}

func buildCorpus(enc func(i int) []byte) [][]byte {
	c := make([][]byte, spikeCorpus)
	for i := 0; i < spikeCorpus; i++ {
		c[i] = enc(i)
	}
	return c
}

// ---- insert replay ----

func applyDirectIns(db *DB, rec []byte) {
	row, _ := getRow(rec[3:]) // skip op+tableID
	_ = db.t["messages"].insert(row)
}

func applySQLIns(db *DB, rec []byte) {
	sl := int(binary.LittleEndian.Uint32(rec[0:4]))
	off := 4 + sl
	sql := string(rec[4 : 4+sl])
	params, _ := getRow(rec[off:])
	pl, err := db.prepare(sql)
	if err != nil {
		return
	}
	_, _ = db.execInsert(pl, params)
}

func BenchmarkWALReplay_Insert_Physical(b *testing.B) {
	c := buildCorpus(func(i int) []byte { return encPhysOrTypedIns(nil, msgRow(i, spikeBody)) })
	benchReplay(b, c, newMsgDB, true, applyDirectIns)
}
func BenchmarkWALReplay_Insert_Typed(b *testing.B) {
	c := buildCorpus(func(i int) []byte { return encPhysOrTypedIns(nil, msgRow(i, spikeBody)) })
	benchReplay(b, c, newMsgDB, true, applyDirectIns)
}
func BenchmarkWALReplay_Insert_SQLStr(b *testing.B) {
	c := buildCorpus(func(i int) []byte { return encSQL(nil, insSQL, msgRow(i, spikeBody)) })
	benchReplay(b, c, newMsgDB, true, applySQLIns)
}

// ---- update replay (rows pre-exist; updating same id repeatedly is fine) ----

func applyPhysUpd(db *DB, rec []byte) {
	row, _ := getRow(rec[3:])
	pk := row[0].U
	db.t["messages"].update(pk, func(_ Row) Row { return row })
}

func applyTypedUpd(db *DB, rec []byte) {
	off := 3
	pk, n := getVal(rec[off:])
	off += n
	ns := int(rec[off])
	off++
	ords := make([]int, ns)
	vals := make(Row, ns)
	for i := 0; i < ns; i++ {
		ords[i] = int(binary.LittleEndian.Uint16(rec[off : off+2]))
		off += 2
		v, k := getVal(rec[off:])
		off += k
		vals[i] = v
	}
	db.t["messages"].update(pk.U, func(r Row) Row {
		for i := range ords {
			r[ords[i]] = vals[i]
		}
		return r
	})
}

func applySQLUpd(db *DB, rec []byte) {
	sl := int(binary.LittleEndian.Uint32(rec[0:4]))
	off := 4 + sl
	sql := string(rec[4 : 4+sl])
	params, _ := getRow(rec[off:])
	pl, err := db.prepare(sql)
	if err != nil {
		return
	}
	_, _ = db.execUpdate(pl, params)
}

func BenchmarkWALReplay_UpdNarrow_Physical(b *testing.B) {
	c := buildCorpus(func(i int) []byte { return encPhysUpd(nil, msgRow(i, spikeBody)) })
	benchReplay(b, c, newMsgDBFilled, false, applyPhysUpd)
}
func BenchmarkWALReplay_UpdNarrow_Typed(b *testing.B) {
	c := buildCorpus(func(i int) []byte { return encTypedUpd(nil, UUIDVal(spikeID(i)), []int{3}, Row{Str(spikeBody)}) })
	benchReplay(b, c, newMsgDBFilled, false, applyTypedUpd)
}
func BenchmarkWALReplay_UpdNarrow_SQLStr(b *testing.B) {
	c := buildCorpus(func(i int) []byte { return encSQL(nil, updSQL, Row{Str(spikeBody), UUIDVal(spikeID(i))}) })
	benchReplay(b, c, newMsgDBFilled, false, applySQLUpd)
}

// ---- delete replay (rebuild-on-wrap: each id is deleted once per filled DB,
//      so we measure the real tombstone + PK-map removal, not the idempotent
//      already-absent short-circuit) ----

func applyDirectDel(db *DB, rec []byte) {
	pk, _ := getVal(rec[3:]) // skip op+tableID
	db.t["messages"].deleteByPK(pk.U)
}

func applySQLDel(db *DB, rec []byte) {
	sl := int(binary.LittleEndian.Uint32(rec[0:4]))
	off := 4 + sl
	sql := string(rec[4 : 4+sl])
	params, _ := getRow(rec[off:])
	pl, err := db.prepare(sql)
	if err != nil {
		return
	}
	_, _ = db.execDelete(pl, params)
}

func BenchmarkWALReplay_Delete_Physical(b *testing.B) {
	c := buildCorpus(func(i int) []byte { return encPhysOrTypedDel(nil, UUIDVal(spikeID(i))) })
	benchReplay(b, c, newMsgDBFilled, true, applyDirectDel)
}
func BenchmarkWALReplay_Delete_Typed(b *testing.B) {
	c := buildCorpus(func(i int) []byte { return encPhysOrTypedDel(nil, UUIDVal(spikeID(i))) })
	benchReplay(b, c, newMsgDBFilled, true, applyDirectDel)
}
func BenchmarkWALReplay_Delete_SQLStr(b *testing.B) {
	c := buildCorpus(func(i int) []byte { return encSQL(nil, delSQL, Row{UUIDVal(spikeID(i))}) })
	benchReplay(b, c, newMsgDBFilled, true, applySQLDel)
}

// =====================  size summary (run with -v)  =====================

func TestWALFormatSizes(t *testing.T) {
	type row struct {
		name             string
		phys, typd, sqls int
	}
	rows := []row{
		{
			"insert (messages)",
			len(encPhysOrTypedIns(nil, msgRow(1, spikeBody))),
			len(encPhysOrTypedIns(nil, msgRow(1, spikeBody))),
			len(encSQL(nil, insSQL, msgRow(1, spikeBody))),
		},
		{
			"update narrow (set body)",
			len(encPhysUpd(nil, msgRow(1, spikeBody))),
			len(encTypedUpd(nil, UUIDVal(spikeID(1)), []int{3}, Row{Str(spikeBody)})),
			len(encSQL(nil, updSQL, Row{Str(spikeBody), UUIDVal(spikeID(1))})),
		},
		{
			"update wide (set 1 of 9 cols)",
			len(encPhysUpd(nil, wideRow(1))),
			len(encTypedUpd(nil, UUIDVal(spikeID(1)), []int{1}, Row{Str("colvalue_padding_xx")})),
			len(encSQL(nil, wideUpdSQL, Row{Str("colvalue_padding_xx"), UUIDVal(spikeID(1))})),
		},
		{
			"delete (by pk)",
			len(encPhysOrTypedDel(nil, UUIDVal(spikeID(1)))),
			len(encPhysOrTypedDel(nil, UUIDVal(spikeID(1)))),
			len(encSQL(nil, delSQL, Row{UUIDVal(spikeID(1))})),
		},
	}
	t.Logf("%-32s %8s %8s %8s", "record", "Physical", "Typed", "SQLStr")
	for _, r := range rows {
		t.Logf("%-32s %6dB %6dB %6dB", r.name, r.phys, r.typd, r.sqls)
	}
}
