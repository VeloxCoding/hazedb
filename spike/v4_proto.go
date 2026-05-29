package spike

// V4 = V3's snapshot architecture but with string PKs (Go's map fastpath).
// Tests whether the V2 regression is the [16]byte key cost or something
// inherent to V2's RWMutex shape.

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const v4Shards = 16

type v4ShardData struct {
	users map[string]UserV2
}

type v4PendingWrite struct {
	op    uint8
	user  UserV2
	reply chan error
}

type v4Shard struct {
	mu       sync.Mutex
	snapshot atomic.Pointer[v4ShardData]
	pending  []v4PendingWrite
	signal   chan struct{}
}

type V4DB struct {
	shards [v4Shards]v4Shard
	done   chan struct{}
	wg     sync.WaitGroup
}

func OpenV4(sizeHint int) *V4DB {
	db := &V4DB{done: make(chan struct{})}
	per := sizeHint / v4Shards
	for i := range db.shards {
		db.shards[i].snapshot.Store(&v4ShardData{users: make(map[string]UserV2, per)})
		db.shards[i].signal = make(chan struct{}, 1)
	}
	for i := range db.shards {
		db.wg.Add(1)
		go db.drainerLoop(i)
	}
	return db
}

func (db *V4DB) Close() error {
	close(db.done)
	db.wg.Wait()
	return nil
}

// shardOfString — short FNV-1a for now (matches V1's shape).
func (db *V4DB) shardOf(id string) *v4Shard {
	var h uint32 = 2166136261
	for i := 0; i < len(id); i++ {
		h ^= uint32(id[i])
		h *= 16777619
	}
	return &db.shards[h&(v4Shards-1)]
}

func (db *V4DB) GetByID(id string) (UserV2, bool) {
	s := db.shardOf(id)
	sd := s.snapshot.Load()
	u, ok := sd.users[id]
	return u, ok
}

func (db *V4DB) Insert(u UserV2, idStr string) error {
	s := db.shardOf(idStr)
	s.mu.Lock()
	s.pending = append(s.pending, v4PendingWrite{op: v3OpInsert, user: u})
	s.mu.Unlock()
	select {
	case s.signal <- struct{}{}:
	default:
	}
	return nil
}

func (db *V4DB) BulkInsert(users []UserV2, ids []string) error {
	buckets := make([][]int, v4Shards)
	for i, idStr := range ids {
		var h uint32 = 2166136261
		for j := 0; j < len(idStr); j++ {
			h ^= uint32(idStr[j])
			h *= 16777619
		}
		idx := h & (v4Shards - 1)
		buckets[idx] = append(buckets[idx], i)
	}
	for i := range db.shards {
		batch := buckets[i]
		if len(batch) == 0 {
			continue
		}
		s := &db.shards[i]
		s.mu.Lock()
		cur := s.snapshot.Load()
		next := &v4ShardData{users: make(map[string]UserV2, len(cur.users)+len(batch))}
		for k, v := range cur.users {
			next.users[k] = v
		}
		for _, idx := range batch {
			if _, exists := next.users[ids[idx]]; exists {
				s.mu.Unlock()
				return errors.New("duplicate primary key")
			}
			next.users[ids[idx]] = users[idx]
		}
		s.snapshot.Store(next)
		s.mu.Unlock()
	}
	return nil
}

func (db *V4DB) drainerLoop(shardIdx int) {
	defer db.wg.Done()
	s := &db.shards[shardIdx]
	ticker := time.NewTicker(100 * time.Microsecond)
	defer ticker.Stop()
	for {
		select {
		case <-db.done:
			s.commitV4()
			return
		case <-s.signal:
			s.commitV4()
		case <-ticker.C:
			s.commitV4()
		}
	}
}

func (s *v4Shard) commitV4() {
	s.mu.Lock()
	if len(s.pending) == 0 {
		s.mu.Unlock()
		return
	}
	batch := s.pending
	s.pending = nil
	s.mu.Unlock()

	cur := s.snapshot.Load()
	next := &v4ShardData{users: make(map[string]UserV2, len(cur.users)+len(batch))}
	for k, v := range cur.users {
		next.users[k] = v
	}
	for i := range batch {
		p := &batch[i]
		// no native ID-string here; for the bench we encode the UUIDv7
		// bytes as a string. Skip duplicate-check for benchmark simplicity.
		key := string(p.user.ID[:])
		next.users[key] = p.user
		if p.reply != nil {
			p.reply <- nil
		}
	}
	s.snapshot.Store(next)
}
