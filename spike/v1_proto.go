package spike

// v1_proto.go — what v1 might look like for the single hardcoded
// `users` table. Combines:
//   - typed Go-struct storage (no binary encode/decode on the read path)
//   - 16 sharded locks (parallel reads + parallel writes scale with cores)
//   - WAL with bufio for durability (cost: ~150 ns per insert, V3 data)
//   - hand-coded accessors per table (sqlc-style "always compiled")
//
// This is a deliberately monomorphic prototype: only `users`, no
// schema flexibility. Used to measure the production ceiling so the
// v1 architecture decision (freeze-pipeline vs always-compiled) can
// be made from data.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math/bits"
	"os"
	"runtime"
	"sync"
)

// v1Shards — sized to ~4× core count, rounded up to a power of two.
// Below 64: floor at 64. Above 1024: cap at 1024 (more is wasted
// per-shard fixed overhead with no further contention to remove).
// Measured sweet spot (see v5_shardsweep.go): ~6.5 ns parallel read
// at numShards × 4 on a 32-core host.
var v1Shards = func() int {
	n := runtime.NumCPU() * 4
	if n < 64 {
		n = 64
	}
	if n > 1024 {
		n = 1024
	}
	// Round up to power of two.
	return 1 << bits.Len(uint(n-1))
}()

type v1Shard struct {
	mu    sync.RWMutex
	users map[string]User
}

type V1DB struct {
	shards []v1Shard

	walMu sync.Mutex // serialises WAL framing across shards
	walF  *os.File
	walBW *bufio.Writer
	noWAL bool
}

// OpenV1 — durable WAL-backed.
func OpenV1(walPath string, sizeHint int) (*V1DB, error) {
	f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	db := &V1DB{
		walF:   f,
		walBW:  bufio.NewWriterSize(f, 64<<10),
		shards: make([]v1Shard, v1Shards),
	}
	per := sizeHint / v1Shards
	for i := range db.shards {
		db.shards[i].users = make(map[string]User, per)
	}
	if err := db.replay(); err != nil {
		f.Close()
		return nil, err
	}
	return db, nil
}

// OpenV1Memory — no WAL, ceiling measurement.
func OpenV1Memory(sizeHint int) *V1DB {
	db := &V1DB{
		noWAL:  true,
		shards: make([]v1Shard, v1Shards),
	}
	per := sizeHint / v1Shards
	for i := range db.shards {
		db.shards[i].users = make(map[string]User, per)
	}
	return db
}

func (db *V1DB) Close() error {
	if db.noWAL {
		return nil
	}
	if err := db.walBW.Flush(); err != nil {
		db.walF.Close()
		return err
	}
	return db.walF.Close()
}

func (db *V1DB) shardOf(id string) *v1Shard {
	var h uint32 = 2166136261
	for i := 0; i < len(id); i++ {
		h ^= uint32(id[i])
		h *= 16777619
	}
	return &db.shards[h&uint32(len(db.shards)-1)]
}

// Insert — typed in-memory, then WAL framing under walMu.
//
// Order: WAL FIRST so that on crash mid-Insert the in-memory state
// reflects only what's durable. Spike order was the opposite; v1
// gets it right.
func (db *V1DB) Insert(u User) error {
	if !db.noWAL {
		payload := encodeUser(u)
		db.walMu.Lock()
		err := db.writeWAL(walInsert, u.ID, payload)
		db.walMu.Unlock()
		if err != nil {
			return err
		}
	}
	s := db.shardOf(u.ID)
	s.mu.Lock()
	if _, exists := s.users[u.ID]; exists {
		s.mu.Unlock()
		return fmt.Errorf("duplicate primary key: %s", u.ID)
	}
	s.users[u.ID] = u
	s.mu.Unlock()
	return nil
}

func (db *V1DB) GetByID(id string) (User, bool) {
	s := db.shardOf(id)
	s.mu.RLock()
	u, ok := s.users[id]
	s.mu.RUnlock()
	return u, ok
}

func (db *V1DB) writeWAL(typ uint8, id string, payload []byte) error {
	// V1 WAL framing (simplified spike-style record).
	totalLen := uint32(1 + 4 + len(payload) + 4)
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], totalLen)
	db.walBW.Write(hdr[:])

	hashStart := db.walBW.Buffered() // we'll compute CRC over buf-tail after write
	_ = hashStart                    // not used in this simplified framing — CRC inlined below

	var typByte = [1]byte{typ}
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))

	body := make([]byte, 0, 1+4+len(payload))
	body = append(body, typByte[:]...)
	body = append(body, lenBuf[:]...)
	body = append(body, payload...)
	db.walBW.Write(body)

	crc := crc32.ChecksumIEEE(body)
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	_, err := db.walBW.Write(crcBuf[:])
	return err
}

func (db *V1DB) replay() error {
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
			return fmt.Errorf("wal: truncated: %w", err)
		}
		payload := body[:len(body)-4]
		gotCRC := binary.LittleEndian.Uint32(body[len(body)-4:])
		wantCRC := crc32.ChecksumIEEE(payload)
		if gotCRC != wantCRC {
			return fmt.Errorf("wal: crc mismatch")
		}
		// typ := payload[0]
		plen := binary.LittleEndian.Uint32(payload[1:5])
		userBytes := payload[5 : 5+plen]
		u, err := decodeUser(userBytes)
		if err != nil {
			return err
		}
		s := db.shardOf(u.ID)
		s.users[u.ID] = u
	}
}

// InsertBatch — N inserts under N shard-locks (one per shard's batch).
// More throughput than per-record Insert because WAL framing is
// amortised under a single walMu hold.
func (db *V1DB) InsertBatch(users []User) error {
	if !db.noWAL {
		db.walMu.Lock()
		for i := range users {
			payload := encodeUser(users[i])
			if err := db.writeWAL(walInsert, users[i].ID, payload); err != nil {
				db.walMu.Unlock()
				return err
			}
		}
		db.walMu.Unlock()
	}
	for i := range users {
		u := users[i]
		s := db.shardOf(u.ID)
		s.mu.Lock()
		if _, exists := s.users[u.ID]; exists {
			s.mu.Unlock()
			return fmt.Errorf("duplicate primary key: %s", u.ID)
		}
		s.users[u.ID] = u
		s.mu.Unlock()
	}
	return nil
}
