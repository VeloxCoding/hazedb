// Package GO-SQLDB is a deliberately minimal spike to validate the central
// thesis of the GO-SQLDB design: that an in-memory Go store with a canonical
// WAL can beat SQLite-in-memory on the hot OLTP path.
//
// The spike implements ONE hardcoded table:
//
//	users(id TEXT PK, email TEXT, name TEXT)
//
// The implementations are as simple as possible, but the types are the same
// types the full v1 design will use: RowID, RowRef, RowHead, Mutation,
// canonical WAL records. That way, adding partitions, ordered indexes, or
// checkpoints later does not require rewriting the existing code.
package spike

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// ---------- Stable identities (matches v1 design) ----------

type TableID uint32
type PartitionID uint16
type RowID uint64

// RowRef is the stable logical row identity used across partitions and
// (later) global indexes. In the spike there is only one table and one
// partition, but every API still produces RowRefs so that adding partitions
// later does not change the index contracts.
type RowRef struct {
	Table     TableID
	Partition PartitionID
	Row       RowID
}

const (
	tableUsers    TableID     = 1
	partitionZero PartitionID = 0
	currentLayout uint16      = 1
)

// ---------- Row model (matches v1 design) ----------

// RowHead is the indirection between RowID and the current row.
// V4 variant: stores the decoded User struct directly (no encoded bytes
// retained). Get becomes a pure struct copy with no decodeUser call.
// Encoded bytes are still produced on the Insert/Update path for the WAL
// write, but discarded after the WAL append returns.
type RowHead struct {
	Row     User
	Deleted bool
}

// User is the public Go shape. The spike has a hardcoded schema, so we use
// a struct directly instead of a generic column model. The codec is still
// schema-driven: every encoded row starts with a layout version and a null
// bitmap, exactly like the v1 EncodedRow.
type User struct {
	ID    string
	Email string
	Name  string
}

// ---------- Row codec (schema-driven binary, matches v1 design) ----------
//
// Layout:
//   uint16 layout_version
//   uint8  null_bitmap (1 byte, 3 columns -> 3 bits used)
//   for each non-null variable column: uint32 length + bytes
//
// All three columns are TEXT in this spike. A full v1 codec would have a
// fixed-width section and a variable-field directory; here we only have
// variable fields, so we keep it inline.

func encodeUser(u User) []byte {
	// Pre-size: 2 (version) + 1 (nullbits) + 3 * (4 + len)
	size := 2 + 1 + 4 + len(u.ID) + 4 + len(u.Email) + 4 + len(u.Name)
	buf := make([]byte, 0, size)

	var hdr [3]byte
	binary.LittleEndian.PutUint16(hdr[0:2], currentLayout)
	hdr[2] = 0 // null bitmap: all columns NOT NULL in this spike
	buf = append(buf, hdr[:]...)

	buf = appendLenPrefixed(buf, []byte(u.ID))
	buf = appendLenPrefixed(buf, []byte(u.Email))
	buf = appendLenPrefixed(buf, []byte(u.Name))
	return buf
}

func decodeUser(b []byte) (User, error) {
	if len(b) < 3 {
		return User{}, errors.New("row too short")
	}
	ver := binary.LittleEndian.Uint16(b[0:2])
	if ver != currentLayout {
		return User{}, fmt.Errorf("unsupported layout version %d", ver)
	}
	// nullbits := b[2] // not used in this spike
	p := 3
	id, p, err := readLenPrefixed(b, p)
	if err != nil {
		return User{}, err
	}
	email, p, err := readLenPrefixed(b, p)
	if err != nil {
		return User{}, err
	}
	name, _, err := readLenPrefixed(b, p)
	if err != nil {
		return User{}, err
	}
	return User{ID: string(id), Email: string(email), Name: string(name)}, nil
}

func appendLenPrefixed(buf, v []byte) []byte {
	var lb [4]byte
	binary.LittleEndian.PutUint32(lb[:], uint32(len(v)))
	buf = append(buf, lb[:]...)
	buf = append(buf, v...)
	return buf
}

func readLenPrefixed(b []byte, p int) ([]byte, int, error) {
	if p+4 > len(b) {
		return nil, 0, errors.New("truncated length")
	}
	n := int(binary.LittleEndian.Uint32(b[p : p+4]))
	p += 4
	if p+n > len(b) {
		return nil, 0, errors.New("truncated value")
	}
	return b[p : p+n], p + n, nil
}

// ---------- WAL (canonical record format, matches v1 design) ----------
//
// Record layout (little-endian):
//   uint32 total_length    (header + payload, excluding this field)
//   uint8  record_type     (1=insert, 2=update, 3=delete)
//   uint64 lsn
//   uint32 table_id
//   uint16 partition_id
//   uint64 row_id
//   uint32 payload_length
//   bytes  payload         (encoded row, or empty for delete)
//   uint32 crc32           (over everything from record_type to end of payload)
//
// The spike does NOT implement partial-record detection or segment files.
// On corruption it returns an error and refuses to start. That is the right
// behavior for a spike; v1 needs to recover the valid prefix.

const (
	walInsert uint8 = 1
	walUpdate uint8 = 2
	walDelete uint8 = 3
)

const walHeaderSize = 1 + 8 + 4 + 2 + 8 + 4 // = 27 bytes between total_length and payload

type walRecord struct {
	Type      uint8
	LSN       uint64
	Table     TableID
	Partition PartitionID
	Row       RowID
	Payload   []byte
}

type wal struct {
	mu  sync.Mutex
	f   *os.File
	bw  *bufio.Writer
	lsn uint64
}

func openWAL(path string) (*wal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &wal{f: f, bw: bufio.NewWriterSize(f, 64<<10)}, nil
}

func (w *wal) append(r *walRecord, sync bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.lsn++
	r.LSN = w.lsn

	totalLen := uint32(walHeaderSize + len(r.Payload) + 4)
	buf := make([]byte, 0, 4+totalLen)

	var u32 [4]byte
	var u64 [8]byte
	var u16 [2]byte

	binary.LittleEndian.PutUint32(u32[:], totalLen)
	buf = append(buf, u32[:]...)

	hashStart := len(buf)
	buf = append(buf, r.Type)
	binary.LittleEndian.PutUint64(u64[:], r.LSN)
	buf = append(buf, u64[:]...)
	binary.LittleEndian.PutUint32(u32[:], uint32(r.Table))
	buf = append(buf, u32[:]...)
	binary.LittleEndian.PutUint16(u16[:], uint16(r.Partition))
	buf = append(buf, u16[:]...)
	binary.LittleEndian.PutUint64(u64[:], uint64(r.Row))
	buf = append(buf, u64[:]...)
	binary.LittleEndian.PutUint32(u32[:], uint32(len(r.Payload)))
	buf = append(buf, u32[:]...)
	buf = append(buf, r.Payload...)

	crc := crc32.ChecksumIEEE(buf[hashStart:])
	binary.LittleEndian.PutUint32(u32[:], crc)
	buf = append(buf, u32[:]...)

	if _, err := w.bw.Write(buf); err != nil {
		return err
	}
	if sync {
		if err := w.bw.Flush(); err != nil {
			return err
		}
		return w.f.Sync()
	}
	return nil
}

func (w *wal) replay(apply func(*walRecord) error) error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var u32 [4]byte
	for {
		if _, err := io.ReadFull(w.f, u32[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		totalLen := binary.LittleEndian.Uint32(u32[:])
		if totalLen < uint32(walHeaderSize)+4 {
			return fmt.Errorf("wal: total length %d too small", totalLen)
		}
		body := make([]byte, totalLen)
		if _, err := io.ReadFull(w.f, body); err != nil {
			return fmt.Errorf("wal: truncated record: %w", err)
		}
		// verify crc
		payload := body[:len(body)-4]
		gotCRC := binary.LittleEndian.Uint32(body[len(body)-4:])
		wantCRC := crc32.ChecksumIEEE(payload)
		if gotCRC != wantCRC {
			return fmt.Errorf("wal: crc mismatch at lsn area")
		}

		p := 0
		r := &walRecord{}
		r.Type = payload[p]
		p++
		r.LSN = binary.LittleEndian.Uint64(payload[p : p+8])
		p += 8
		r.Table = TableID(binary.LittleEndian.Uint32(payload[p : p+4]))
		p += 4
		r.Partition = PartitionID(binary.LittleEndian.Uint16(payload[p : p+2]))
		p += 2
		r.Row = RowID(binary.LittleEndian.Uint64(payload[p : p+8]))
		p += 8
		plen := binary.LittleEndian.Uint32(payload[p : p+4])
		p += 4
		if p+int(plen) != len(payload) {
			return fmt.Errorf("wal: payload length mismatch")
		}
		r.Payload = payload[p:]
		if r.LSN > w.lsn {
			w.lsn = r.LSN
		}
		if err := apply(r); err != nil {
			return err
		}
	}
}

func (w *wal) close() error {
	if err := w.bw.Flush(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// ---------- DB ----------
//
// One global RWMutex protects the row directory and the primary index.
// In v1 this becomes one RWMutex per partition. The single-lock spike
// gives us a clean baseline.

type DB struct {
	mu sync.RWMutex

	// Row directory: RowID -> RowHead. In v1 this is a sparse/paged
	// structure per partition; here it is one map.
	rows map[RowID]*RowHead

	// Primary index on users.id: id -> RowID. In v1 this is the
	// partition-local primary index. The map value is RowID, not *RowHead
	// or a pointer, because RowID is the stable logical identity.
	pkIndex map[string]RowID

	nextRowID RowID
	wal       *wal
	syncWAL   bool
	noWAL     bool // memory-only mode: skip all WAL writes
}

// Open creates or recovers a DB. If walPath exists, it is fully replayed.
func Open(walPath string, syncWAL bool) (*DB, error) {
	return OpenWithSize(walPath, syncWAL, 0)
}

// OpenMemory creates a memory-only DB with no WAL. Used to measure the
// ceiling of the in-process map+index path without any disk I/O. NOT
// durable — process exit loses all data. Useful for cache/scratch
// workloads where the source of truth lives elsewhere.
func OpenMemory(sizeHint int) *DB {
	return &DB{
		rows:    make(map[RowID]*RowHead, sizeHint),
		pkIndex: make(map[string]RowID, sizeHint),
		noWAL:   true,
	}
}

// OpenWithSize is Open with an estimated row-count hint. The hint pre-sizes
// the in-memory maps so the hot Insert path doesn't pay for repeated
// bucket-grow allocations. Pass 0 for "no hint" (matches Open).
func OpenWithSize(walPath string, syncWAL bool, sizeHint int) (*DB, error) {
	w, err := openWAL(walPath)
	if err != nil {
		return nil, err
	}
	db := &DB{
		rows:    make(map[RowID]*RowHead, sizeHint),
		pkIndex: make(map[string]RowID, sizeHint),
		wal:     w,
		syncWAL: syncWAL,
	}
	if err := w.replay(db.applyWAL); err != nil {
		w.close()
		return nil, err
	}
	return db, nil
}

func (db *DB) Close() error {
	if db.noWAL {
		return nil
	}
	return db.wal.close()
}

// applyWAL is used during recovery. It assumes no concurrent access.
func (db *DB) applyWAL(r *walRecord) error {
	switch r.Type {
	case walInsert, walUpdate:
		u, err := decodeUser(r.Payload)
		if err != nil {
			return err
		}
		head, ok := db.rows[r.Row]
		if !ok {
			head = &RowHead{}
			db.rows[r.Row] = head
		}
		head.Row = u
		head.Deleted = false
		db.pkIndex[u.ID] = r.Row
	case walDelete:
		if head, ok := db.rows[r.Row]; ok {
			if !head.Deleted {
				delete(db.pkIndex, head.Row.ID)
			}
			head.Deleted = true
			head.Row = User{}
		}
	default:
		return fmt.Errorf("wal: unknown record type %d", r.Type)
	}
	if r.Row >= db.nextRowID {
		db.nextRowID = r.Row + 1
	}
	return nil
}

// ---------- Public API ----------

func (db *DB) Insert(u User) (RowRef, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.pkIndex[u.ID]; exists {
		return RowRef{}, fmt.Errorf("duplicate primary key: %s", u.ID)
	}

	rid := db.nextRowID
	db.nextRowID++
	payload := encodeUser(u)

	if !db.noWAL {
		if err := db.wal.append(&walRecord{
			Type:      walInsert,
			Table:     tableUsers,
			Partition: partitionZero,
			Row:       rid,
			Payload:   payload,
		}, db.syncWAL); err != nil {
			db.nextRowID--
			return RowRef{}, err
		}
	}

	db.rows[rid] = &RowHead{Row: u}
	db.pkIndex[u.ID] = rid
	return RowRef{Table: tableUsers, Partition: partitionZero, Row: rid}, nil
}

func (db *DB) Get(id string) (User, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	rid, ok := db.pkIndex[id]
	if !ok {
		return User{}, false, nil
	}
	head := db.rows[rid]
	if head == nil || head.Deleted {
		return User{}, false, nil
	}
	return head.Row, true, nil
}

func (db *DB) Update(u User) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	rid, ok := db.pkIndex[u.ID]
	if !ok {
		return fmt.Errorf("not found: %s", u.ID)
	}
	payload := encodeUser(u)

	if !db.noWAL {
		if err := db.wal.append(&walRecord{
			Type:      walUpdate,
			Table:     tableUsers,
			Partition: partitionZero,
			Row:       rid,
			Payload:   payload,
		}, db.syncWAL); err != nil {
			return err
		}
	}
	db.rows[rid].Row = u
	return nil
}

func (db *DB) Delete(id string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	rid, ok := db.pkIndex[id]
	if !ok {
		return fmt.Errorf("not found: %s", id)
	}

	if !db.noWAL {
		if err := db.wal.append(&walRecord{
			Type:      walDelete,
			Table:     tableUsers,
			Partition: partitionZero,
			Row:       rid,
			Payload:   nil,
		}, db.syncWAL); err != nil {
			return err
		}
	}
	db.rows[rid].Deleted = true
	db.rows[rid].Row = User{}
	delete(db.pkIndex, id)
	return nil
}

// UnsafeGet skips db.mu entirely. Only valid if the caller GUARANTEES
// no concurrent writes (frozen dataset, read-only phase). Used to
// measure the lock-free ceiling of the Get path. NOT for production use.
func (db *DB) UnsafeGet(id string) (User, bool) {
	rid, ok := db.pkIndex[id]
	if !ok {
		return User{}, false
	}
	head := db.rows[rid]
	if head == nil || head.Deleted {
		return User{}, false
	}
	return head.Row, true
}

// InsertBatch inserts a slice of users under a single db.mu hold and
// (when WAL is on) a single wal.mu hold around the bufio writes. Reduces
// per-call lock/unlock and function-call overhead vs N separate Inserts.
// Returns the first error encountered; any prior successful inserts in
// the batch are NOT rolled back.
func (db *DB) InsertBatch(users []User) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	for i := range users {
		u := users[i]
		if _, exists := db.pkIndex[u.ID]; exists {
			return fmt.Errorf("duplicate primary key: %s", u.ID)
		}
		rid := db.nextRowID
		db.nextRowID++
		if !db.noWAL {
			payload := encodeUser(u)
			if err := db.wal.append(&walRecord{
				Type:      walInsert,
				Table:     tableUsers,
				Partition: partitionZero,
				Row:       rid,
				Payload:   payload,
			}, db.syncWAL); err != nil {
				db.nextRowID--
				return err
			}
		}
		db.rows[rid] = &RowHead{Row: u}
		db.pkIndex[u.ID] = rid
	}
	return nil
}

// Stats returns rough counts for sanity checks in benchmarks.
func (db *DB) Stats() (rows, liveIndex int) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.rows), len(db.pkIndex)
}
