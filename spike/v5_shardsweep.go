package spike

// V5 — V1's architecture (sharded RWMutex over typed map), but with
// configurable shard count. Built to answer one question:
//
//   "Is the 2.4 ns parallel read of V3 a snapshot-architecture win, or
//   just a lock-vermijdings win that ANY lockless-enough read path
//   captures?"
//
// V1 used 16 shards. On 32 cores with 32 goroutines doing random Gets,
// each shard sees ~2 contending readers — non-zero RWMutex contention.
// If we crank shards to 256 (contention per shard ~0.125 readers, i.e.
// effectively zero), does V5 catch up to V3's lockless ceiling?
//
// If yes: snapshot mode is over-engineered — one storage engine wins.
// If no: investigate epoch-based reclamation (RCU) as the right path.

import (
	"errors"
	"sync"
)

type v5Shard struct {
	mu    sync.RWMutex
	users map[string]UserV2
	// Pad to defeat false sharing — RWMutex + map header total ~80 bytes,
	// rounding up to 128 bytes (2 cache lines) per shard so adjacent
	// shards don't bounce each other's cache lines.
	_ [48]byte
}

// V5DB is generic over shard count via a runtime-sized slice. Compile-
// time constant would be ideal but Go doesn't let us parametrize struct
// fields by const easily. Runtime cost: one extra index op per shardOf,
// negligible.
type V5DB struct {
	shards []v5Shard
	mask   uint32
}

// OpenV5 — shards must be a power of two.
func OpenV5(numShards, sizeHint int) *V5DB {
	if numShards&(numShards-1) != 0 {
		panic("V5: shards must be power of two")
	}
	db := &V5DB{
		shards: make([]v5Shard, numShards),
		mask:   uint32(numShards) - 1,
	}
	per := sizeHint / numShards
	if per < 16 {
		per = 16
	}
	for i := range db.shards {
		db.shards[i].users = make(map[string]UserV2, per)
	}
	return db
}

func (db *V5DB) Close() error { return nil }

func (db *V5DB) shardOf(id string) *v5Shard {
	var h uint32 = 2166136261
	for i := 0; i < len(id); i++ {
		h ^= uint32(id[i])
		h *= 16777619
	}
	return &db.shards[h&db.mask]
}

func (db *V5DB) Insert(u UserV2, id string) error {
	s := db.shardOf(id)
	s.mu.Lock()
	if _, exists := s.users[id]; exists {
		s.mu.Unlock()
		return errors.New("duplicate primary key")
	}
	s.users[id] = u
	s.mu.Unlock()
	return nil
}

func (db *V5DB) GetByID(id string) (UserV2, bool) {
	s := db.shardOf(id)
	s.mu.RLock()
	u, ok := s.users[id]
	s.mu.RUnlock()
	return u, ok
}
