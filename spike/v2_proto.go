package spike

// V2 prototype — Tier 1 of the perfectionist ladder.
//
//   V2DB: UUIDv7 [16]byte PKs + cached shard hash + sharded RWMutex
//   V3DB: V2 + atomic.Pointer snapshot per shard + drainer batching
//
// Both are memory-only (WAL plugs in later; same ~150 ns cost as V1).
// Both compared against V1 (Go strings PK + sharded RWMutex).

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ---------- UUIDv7 ----------
//
// 6-byte ms timestamp | 4-bit version (7) + 12-bit rand_a |
// 2-bit variant + 14-bit rand_b | 64-bit rand_c
//
// Last 4 bytes are uniform random — perfect for shard hashing without
// a separate hash function.

type UUIDv7 [16]byte

func NewUUIDv7() UUIDv7 {
	var u UUIDv7
	now := time.Now().UnixMilli()
	u[0] = byte(now >> 40)
	u[1] = byte(now >> 32)
	u[2] = byte(now >> 24)
	u[3] = byte(now >> 16)
	u[4] = byte(now >> 8)
	u[5] = byte(now)
	rand.Read(u[6:])
	u[6] = (u[6] & 0x0F) | 0x70 // version 7
	u[8] = (u[8] & 0x3F) | 0x80 // variant 10
	return u
}

// ---------- UserV2: typed row, UUIDv7 PK ----------

type UserV2 struct {
	ID     UUIDv7
	Email  string
	Name   string
	Active bool
}

// shardOfUUID — last 4 bytes of UUIDv7 are uniformly random, use them
// directly as the shard hash. No FNV-1a needed.
func shardOfUUID(id UUIDv7, n uint32) uint32 {
	h := binary.LittleEndian.Uint32(id[12:16])
	return h & (n - 1)
}

// ====================================================================
//  V2DB — UUIDv7 + cached shard hash + sharded RWMutex
// ====================================================================

const v2Shards = 16

type v2Shard struct {
	mu    sync.RWMutex
	users map[UUIDv7]UserV2
}

type V2DB struct {
	shards [v2Shards]v2Shard
}

func OpenV2(sizeHint int) *V2DB {
	db := &V2DB{}
	per := sizeHint / v2Shards
	for i := range db.shards {
		db.shards[i].users = make(map[UUIDv7]UserV2, per)
	}
	return db
}

func (db *V2DB) Close() error { return nil }

func (db *V2DB) shardOf(id UUIDv7) *v2Shard {
	return &db.shards[shardOfUUID(id, v2Shards)]
}

func (db *V2DB) Insert(u UserV2) error {
	s := db.shardOf(u.ID)
	s.mu.Lock()
	if _, exists := s.users[u.ID]; exists {
		s.mu.Unlock()
		return errors.New("duplicate primary key")
	}
	s.users[u.ID] = u
	s.mu.Unlock()
	return nil
}

func (db *V2DB) GetByID(id UUIDv7) (UserV2, bool) {
	s := db.shardOf(id)
	s.mu.RLock()
	u, ok := s.users[id]
	s.mu.RUnlock()
	return u, ok
}

// ====================================================================
//  V3DB — V2 + atomic.Pointer snapshot per shard + drainer batching
// ====================================================================
//
// Reads: atomic.Load → map lookup, NO lock. Aim: 2–5 ns parallel.
// Writes: queue → drainer rebuilds snapshot, atomic.Store. Sync mode
// waits for the next commit; async returns immediately.

const v3Shards = 16

type v3ShardData struct {
	users map[UUIDv7]UserV2
}

type v3PendingWrite struct {
	op    uint8
	user  UserV2
	reply chan error // non-nil for sync
}

const (
	v3OpInsert uint8 = 1
	v3OpUpdate uint8 = 2
	v3OpDelete uint8 = 3
)

type v3Shard struct {
	mu       sync.Mutex // protects pending
	snapshot atomic.Pointer[v3ShardData]
	pending  []v3PendingWrite

	signal chan struct{} // drainer wakeup
}

type V3DB struct {
	shards [v3Shards]v3Shard
	done   chan struct{}
	wg     sync.WaitGroup
}

func OpenV3(sizeHint int) *V3DB {
	db := &V3DB{done: make(chan struct{})}
	per := sizeHint / v3Shards
	for i := range db.shards {
		db.shards[i].snapshot.Store(&v3ShardData{
			users: make(map[UUIDv7]UserV2, per),
		})
		db.shards[i].signal = make(chan struct{}, 1)
	}
	for i := range db.shards {
		db.wg.Add(1)
		go db.drainerLoop(i)
	}
	return db
}

func (db *V3DB) Close() error {
	close(db.done)
	db.wg.Wait()
	return nil
}

func (db *V3DB) shardOf(id UUIDv7) *v3Shard {
	return &db.shards[shardOfUUID(id, v3Shards)]
}

// GetByID — fully lock-free. Atomic load + map lookup.
func (db *V3DB) GetByID(id UUIDv7) (UserV2, bool) {
	s := db.shardOf(id)
	sd := s.snapshot.Load()
	u, ok := sd.users[id]
	return u, ok
}

// Insert — async. Returns when the write is enqueued, not when committed.
// For read-your-writes consistency, use InsertSync.
func (db *V3DB) Insert(u UserV2) error {
	s := db.shardOf(u.ID)
	s.mu.Lock()
	s.pending = append(s.pending, v3PendingWrite{op: v3OpInsert, user: u})
	s.mu.Unlock()
	select {
	case s.signal <- struct{}{}:
	default:
	}
	return nil
}

// InsertSync — blocks until the write has been applied to a snapshot.
func (db *V3DB) InsertSync(u UserV2) error {
	reply := make(chan error, 1)
	s := db.shardOf(u.ID)
	s.mu.Lock()
	s.pending = append(s.pending, v3PendingWrite{op: v3OpInsert, user: u, reply: reply})
	s.mu.Unlock()
	select {
	case s.signal <- struct{}{}:
	default:
	}
	return <-reply
}

func (db *V3DB) drainerLoop(shardIdx int) {
	defer db.wg.Done()
	s := &db.shards[shardIdx]
	// 100µs periodic tick — also wakes for explicit signals.
	ticker := time.NewTicker(100 * time.Microsecond)
	defer ticker.Stop()
	for {
		select {
		case <-db.done:
			s.commit()
			return
		case <-s.signal:
			s.commit()
		case <-ticker.C:
			s.commit()
		}
	}
}

// BulkInsert — bypasses the drainer for warm-loads. Acquires shard
// mutexes, builds one new snapshot per shard with all rows inserted,
// atomic-stores. Avoids the O(N^2) cost of N sequential InsertSync.
func (db *V3DB) BulkInsert(users []UserV2) error {
	// Group users by shard.
	buckets := make([][]UserV2, v3Shards)
	for _, u := range users {
		idx := shardOfUUID(u.ID, v3Shards)
		buckets[idx] = append(buckets[idx], u)
	}
	for i := range db.shards {
		batch := buckets[i]
		if len(batch) == 0 {
			continue
		}
		s := &db.shards[i]
		s.mu.Lock()
		cur := s.snapshot.Load()
		next := &v3ShardData{
			users: make(map[UUIDv7]UserV2, len(cur.users)+len(batch)),
		}
		for k, v := range cur.users {
			next.users[k] = v
		}
		for _, u := range batch {
			if _, exists := next.users[u.ID]; exists {
				s.mu.Unlock()
				return errors.New("duplicate primary key")
			}
			next.users[u.ID] = u
		}
		s.snapshot.Store(next)
		s.mu.Unlock()
	}
	return nil
}

func (s *v3Shard) commit() {
	s.mu.Lock()
	if len(s.pending) == 0 {
		s.mu.Unlock()
		return
	}
	batch := s.pending
	s.pending = nil
	s.mu.Unlock()

	cur := s.snapshot.Load()
	next := &v3ShardData{
		users: make(map[UUIDv7]UserV2, len(cur.users)+len(batch)),
	}
	for k, v := range cur.users {
		next.users[k] = v
	}
	for i := range batch {
		p := &batch[i]
		var err error
		switch p.op {
		case v3OpInsert:
			if _, exists := next.users[p.user.ID]; exists {
				err = errors.New("duplicate primary key")
			} else {
				next.users[p.user.ID] = p.user
			}
		case v3OpUpdate:
			next.users[p.user.ID] = p.user
		case v3OpDelete:
			delete(next.users, p.user.ID)
		}
		if p.reply != nil {
			p.reply <- err
		}
	}
	s.snapshot.Store(next)
}
