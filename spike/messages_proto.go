package spike

// Messages prototype — the thesis test.
//
// Schema: messages(id UUIDv7 PK, thread_id [16]byte, seq int64, body string)
// Index:  (thread_id, seq) for the canonical "last N messages in thread"
//         tail-scan query that the FASTSQL RFC calls out as the real
//         differentiator vs SQLite/Redis/BoltDB.
//
// Storage:
//   - 128 sharded RWMutex maps for PK lookup
//   - Per-thread sorted-by-seq slice for the ordered tail-scan path
//
// The per-thread slice assumes seq is monotonically increasing per
// thread (true for chat-/log-style messages — the realistic case).
// Out-of-order inserts would require sorted-insert (~O(log N) bsearch
// + O(N) shift), which is fine for the spike but suboptimal.
//
// This is the workload that the RFC's Phase 2 was designed for.

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

type Message struct {
	ID       UUIDv7
	ThreadID [16]byte
	Seq      int64
	Body     string
}

const msgShards = 128

// threadIndex — parallel arrays: seqs[i] is the seq value of the
// message at messages[rowIDs[i]] in its shard. Inserts are O(1) when
// seq is monotone per thread, O(log N + N) shift otherwise.
type threadIndex struct {
	seqs   []int64
	rowIDs []uint32
}

type msgShard struct {
	mu      sync.RWMutex
	rows    []Message
	pk      map[UUIDv7]uint32
	threads map[[16]byte]*threadIndex
}

type MessagesDB struct {
	shards [msgShards]msgShard

	// WAL — single global lock around the bufio.Writer. Insert order
	// (and thus LSN) is whatever wins walMu, which differs from the
	// shard-mu order. For per-table durability that's fine; for
	// cross-table consistency v1 needs a different LSN strategy.
	walMu  sync.Mutex
	walF   *os.File
	walBW  *bufio.Writer
	walLSN uint64
	noWAL  bool
}

// OpenMessagesDB — memory-only (no WAL).
func OpenMessagesDB(sizeHint int) *MessagesDB {
	db := &MessagesDB{noWAL: true}
	initShards(&db.shards, sizeHint)
	return db
}

// OpenMessagesDBWAL — WAL-backed for the mixed workload test.
func OpenMessagesDBWAL(walPath string, sizeHint int) (*MessagesDB, error) {
	f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	db := &MessagesDB{
		walF:  f,
		walBW: bufio.NewWriterSize(f, 64<<10),
	}
	initShards(&db.shards, sizeHint)
	if err := db.replay(); err != nil {
		f.Close()
		return nil, err
	}
	return db, nil
}

func initShards(shards *[msgShards]msgShard, sizeHint int) {
	per := sizeHint / msgShards
	if per < 16 {
		per = 16
	}
	for i := range shards {
		shards[i].rows = make([]Message, 0, per)
		shards[i].pk = make(map[UUIDv7]uint32, per)
		shards[i].threads = make(map[[16]byte]*threadIndex)
	}
}

func (db *MessagesDB) Close() error {
	if db.noWAL {
		return nil
	}
	if err := db.walBW.Flush(); err != nil {
		db.walF.Close()
		return err
	}
	return db.walF.Close()
}

// shardOfThread — route by thread_id so all messages of a thread land
// in the same shard. This is critical: it makes tail-scan queries
// single-shard. Hashing by message ID instead would scatter them.
func (db *MessagesDB) shardOfThread(threadID [16]byte) *msgShard {
	var h uint32 = 2166136261
	for i := 0; i < 16; i++ {
		h ^= uint32(threadID[i])
		h *= 16777619
	}
	return &db.shards[h&(msgShards-1)]
}

func (db *MessagesDB) Insert(m Message) error {
	if !db.noWAL {
		payload := encodeMessage(m)
		db.walMu.Lock()
		err := db.writeWALRecord(payload)
		db.walMu.Unlock()
		if err != nil {
			return err
		}
	}
	return db.applyInsert(m)
}

func (db *MessagesDB) applyInsert(m Message) error {
	s := db.shardOfThread(m.ThreadID)
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.pk[m.ID]; exists {
		return errors.New("duplicate primary key")
	}

	rowID := uint32(len(s.rows))
	s.rows = append(s.rows, m)
	s.pk[m.ID] = rowID

	t, ok := s.threads[m.ThreadID]
	if !ok {
		t = &threadIndex{}
		s.threads[m.ThreadID] = t
	}
	// Monotone-seq fast path: just append.
	if len(t.seqs) == 0 || t.seqs[len(t.seqs)-1] < m.Seq {
		t.seqs = append(t.seqs, m.Seq)
		t.rowIDs = append(t.rowIDs, rowID)
		return nil
	}
	// Out-of-order: sorted insert (rare path for chat workloads).
	pos := lowerBoundInt64(t.seqs, m.Seq)
	t.seqs = append(t.seqs, 0)
	copy(t.seqs[pos+1:], t.seqs[pos:])
	t.seqs[pos] = m.Seq
	t.rowIDs = append(t.rowIDs, 0)
	copy(t.rowIDs[pos+1:], t.rowIDs[pos:])
	t.rowIDs[pos] = rowID
	return nil
}

// encodeMessage — compact binary framing for the WAL.
//
//	uuid:16 | threadID:16 | seq:8 | bodyLen:4 | body:N
func encodeMessage(m Message) []byte {
	size := 16 + 16 + 8 + 4 + len(m.Body)
	buf := make([]byte, 0, size)
	buf = append(buf, m.ID[:]...)
	buf = append(buf, m.ThreadID[:]...)
	var u64 [8]byte
	binary.LittleEndian.PutUint64(u64[:], uint64(m.Seq))
	buf = append(buf, u64[:]...)
	var u32 [4]byte
	binary.LittleEndian.PutUint32(u32[:], uint32(len(m.Body)))
	buf = append(buf, u32[:]...)
	buf = append(buf, m.Body...)
	return buf
}

func decodeMessage(b []byte) (Message, error) {
	if len(b) < 16+16+8+4 {
		return Message{}, errors.New("message: truncated")
	}
	var m Message
	copy(m.ID[:], b[0:16])
	copy(m.ThreadID[:], b[16:32])
	m.Seq = int64(binary.LittleEndian.Uint64(b[32:40]))
	bodyLen := binary.LittleEndian.Uint32(b[40:44])
	if 44+int(bodyLen) > len(b) {
		return Message{}, errors.New("message: body truncated")
	}
	m.Body = string(b[44 : 44+bodyLen])
	return m, nil
}

// writeWALRecord — same framing as v1_proto.go's writeWAL:
//
//	totalLen:4 | type:1 | payloadLen:4 | payload:N | crc:4
func (db *MessagesDB) writeWALRecord(payload []byte) error {
	db.walLSN++
	totalLen := uint32(1 + 4 + len(payload) + 4)
	var u32 [4]byte
	binary.LittleEndian.PutUint32(u32[:], totalLen)
	db.walBW.Write(u32[:])
	body := make([]byte, 0, 1+4+len(payload))
	body = append(body, 1) // type: insert
	binary.LittleEndian.PutUint32(u32[:], uint32(len(payload)))
	body = append(body, u32[:]...)
	body = append(body, payload...)
	db.walBW.Write(body)
	crc := crc32.ChecksumIEEE(body)
	binary.LittleEndian.PutUint32(u32[:], crc)
	_, err := db.walBW.Write(u32[:])
	return err
}

func (db *MessagesDB) replay() error {
	if _, err := db.walF.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var u32 [4]byte
	for {
		if _, err := io.ReadFull(db.walF, u32[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		totalLen := binary.LittleEndian.Uint32(u32[:])
		body := make([]byte, totalLen)
		if _, err := io.ReadFull(db.walF, body); err != nil {
			return err
		}
		payload := body[:len(body)-4]
		gotCRC := binary.LittleEndian.Uint32(body[len(body)-4:])
		if crc32.ChecksumIEEE(payload) != gotCRC {
			return errors.New("wal: crc mismatch")
		}
		plen := binary.LittleEndian.Uint32(payload[1:5])
		m, err := decodeMessage(payload[5 : 5+plen])
		if err != nil {
			return err
		}
		db.applyInsert(m)
	}
}

// FlushWAL forces a bufio flush. Used by mixed-workload tests to bound
// durability windows during the bench.
func (db *MessagesDB) FlushWAL() error {
	if db.noWAL {
		return nil
	}
	db.walMu.Lock()
	defer db.walMu.Unlock()
	return db.walBW.Flush()
}

func lowerBoundInt64(s []int64, v int64) int {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := (lo + hi) >> 1
		if s[mid] < v {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// GetByID — point lookup. Uses pk index, no ordered traversal.
func (db *MessagesDB) GetByID(id UUIDv7, threadID [16]byte) (Message, bool) {
	s := db.shardOfThread(threadID)
	s.mu.RLock()
	rowID, ok := s.pk[id]
	if !ok {
		s.mu.RUnlock()
		return Message{}, false
	}
	m := s.rows[rowID]
	s.mu.RUnlock()
	return m, ok
}

// LastN — the thesis query. Returns the last N messages of a thread in
// descending seq order. Single-shard read since we sharded by thread_id.
func (db *MessagesDB) LastN(threadID [16]byte, n int, dst []Message) []Message {
	s := db.shardOfThread(threadID)
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.threads[threadID]
	if !ok {
		return dst
	}
	end := len(t.rowIDs)
	start := end - n
	if start < 0 {
		start = 0
	}
	// Walk backwards through rowIDs, copy rows.
	for i := end - 1; i >= start; i-- {
		dst = append(dst, s.rows[t.rowIDs[i]])
	}
	return dst
}

// LastNSince — variant: last N messages with seq > minSeq. Used to
// validate the index-driven range scan (vs full scan + filter).
func (db *MessagesDB) LastNSince(threadID [16]byte, minSeq int64, limit int, dst []Message) []Message {
	s := db.shardOfThread(threadID)
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.threads[threadID]
	if !ok {
		return dst
	}
	// Find first index with seq > minSeq via binary search.
	lo := lowerBoundInt64(t.seqs, minSeq+1)
	end := len(t.rowIDs)
	count := 0
	for i := end - 1; i >= lo && count < limit; i-- {
		dst = append(dst, s.rows[t.rowIDs[i]])
		count++
	}
	return dst
}
