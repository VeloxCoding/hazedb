package spike

// L3 variants — typed-storage prototypes for measuring the in-memory
// representation ceiling. The user's "freeze" proposal claims compiled
// L2 (specialised execution over the same binary row format) is the
// right v1 target, with L3 (typed Go struct storage) deferred. These
// variants prototype L3 by hand to see whether the deferral hides a
// large speedup or a marginal one.
//
// All variants are memory-only (no WAL), single-table (users), no
// generic execution. They mirror what hand-coded "compiled" output
// for a single frozen table would look like.

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// ---------- L3a: map[string]*User direct, no RowID for reads ----------

type DBL3a struct {
	mu    sync.RWMutex
	users map[string]*User
}

func OpenL3a(sizeHint int) *DBL3a {
	return &DBL3a{users: make(map[string]*User, sizeHint)}
}

func (db *DBL3a) Insert(u User) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, exists := db.users[u.ID]; exists {
		return fmt.Errorf("duplicate primary key: %s", u.ID)
	}
	uu := u
	db.users[u.ID] = &uu
	return nil
}

func (db *DBL3a) Get(id string) (User, bool) {
	db.mu.RLock()
	u, ok := db.users[id]
	db.mu.RUnlock()
	if !ok {
		return User{}, false
	}
	return *u, true
}

func (db *DBL3a) Close() error { return nil }

// ---------- L3b: map[string]User value-type direct ----------

type DBL3b struct {
	mu    sync.RWMutex
	users map[string]User
}

func OpenL3b(sizeHint int) *DBL3b {
	return &DBL3b{users: make(map[string]User, sizeHint)}
}

func (db *DBL3b) Insert(u User) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, exists := db.users[u.ID]; exists {
		return fmt.Errorf("duplicate primary key: %s", u.ID)
	}
	db.users[u.ID] = u
	return nil
}

func (db *DBL3b) Get(id string) (User, bool) {
	db.mu.RLock()
	u, ok := db.users[id]
	db.mu.RUnlock()
	return u, ok
}

func (db *DBL3b) Close() error { return nil }

// ---------- L3c: map[string]int + []User slice (index indirection) ----

type DBL3c struct {
	mu   sync.RWMutex
	idx  map[string]int
	rows []User
}

func OpenL3c(sizeHint int) *DBL3c {
	return &DBL3c{
		idx:  make(map[string]int, sizeHint),
		rows: make([]User, 0, sizeHint),
	}
}

func (db *DBL3c) Insert(u User) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, exists := db.idx[u.ID]; exists {
		return fmt.Errorf("duplicate primary key: %s", u.ID)
	}
	db.idx[u.ID] = len(db.rows)
	db.rows = append(db.rows, u)
	return nil
}

func (db *DBL3c) Get(id string) (User, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	i, ok := db.idx[id]
	if !ok {
		return User{}, false
	}
	return db.rows[i], true
}

func (db *DBL3c) Close() error { return nil }

// ---------- L3d: hand-coded "compiled" UsersDB ------------------------
//
// What sqlc-style codegen for a single frozen table+statements would
// produce. No generic execution, no RowID indirection for reads,
// dedicated methods per statement. Includes a (no-op) "PreparedStmt"
// for the canonical Get-by-id pattern.

type UsersDB struct {
	mu    sync.RWMutex
	users map[string]User
}

func OpenUsersDB(sizeHint int) *UsersDB {
	return &UsersDB{users: make(map[string]User, sizeHint)}
}

// GetByID — the compiled equivalent of "PREPARE get_user_by_id AS
// SELECT id,email,name FROM users WHERE id = ?". No parser, no plan,
// just the typed accessor.
func (db *UsersDB) GetByID(id string) (User, bool) {
	db.mu.RLock()
	u, ok := db.users[id]
	db.mu.RUnlock()
	return u, ok
}

func (db *UsersDB) Insert(u User) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, exists := db.users[u.ID]; exists {
		return fmt.Errorf("duplicate primary key: %s", u.ID)
	}
	db.users[u.ID] = u
	return nil
}

func (db *UsersDB) Close() error { return nil }

// ---------- L3e: atomic.Pointer snapshot, lock-free reads -------------
//
// Writers create a new map (copy + add), atomic-swap. Readers atomic-
// Load and access without any lock. Write cost is O(N); only useful
// when reads dominate.

type DBL3e struct {
	mu       sync.Mutex // serialises writers
	snapshot atomic.Pointer[map[string]User]
}

func OpenL3e(sizeHint int) *DBL3e {
	m := make(map[string]User, sizeHint)
	db := &DBL3e{}
	db.snapshot.Store(&m)
	return db
}

func (db *DBL3e) Insert(u User) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	cur := *db.snapshot.Load()
	if _, exists := cur[u.ID]; exists {
		return fmt.Errorf("duplicate primary key: %s", u.ID)
	}
	next := make(map[string]User, len(cur)+1)
	for k, v := range cur {
		next[k] = v
	}
	next[u.ID] = u
	db.snapshot.Store(&next)
	return nil
}

// InsertBulk avoids the O(N) copy per insert by building the snapshot
// once. Use for setup/bench warmup where N inserts would otherwise be
// O(N^2). The snapshot is replaced atomically at the end.
func (db *DBL3e) InsertBulk(users []User) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	cur := *db.snapshot.Load()
	next := make(map[string]User, len(cur)+len(users))
	for k, v := range cur {
		next[k] = v
	}
	for _, u := range users {
		if _, exists := next[u.ID]; exists {
			return fmt.Errorf("duplicate primary key: %s", u.ID)
		}
		next[u.ID] = u
	}
	db.snapshot.Store(&next)
	return nil
}

func (db *DBL3e) Get(id string) (User, bool) {
	m := *db.snapshot.Load()
	u, ok := m[id]
	return u, ok
}

func (db *DBL3e) Close() error { return nil }

// ---------- L3f: sharded typed maps (parallel writes + reads) ---------
//
// 16 shards, hash key → shard. Reads on different shards don't contend
// on RWMutex; writes on different shards don't contend on Mutex.

const l3fShards = 16

type l3fShard struct {
	mu    sync.RWMutex
	users map[string]User
}

type DBL3f struct {
	shards [l3fShards]l3fShard
}

func OpenL3f(sizeHint int) *DBL3f {
	db := &DBL3f{}
	per := sizeHint / l3fShards
	for i := range db.shards {
		db.shards[i].users = make(map[string]User, per)
	}
	return db
}

// fnv1aLite — short FNV-1a for shard routing. Avoids importing hash/fnv
// (~50 ns) inside the hot path.
func (db *DBL3f) shardOf(id string) *l3fShard {
	var h uint32 = 2166136261
	for i := 0; i < len(id); i++ {
		h ^= uint32(id[i])
		h *= 16777619
	}
	return &db.shards[h&(l3fShards-1)]
}

func (db *DBL3f) Insert(u User) error {
	s := db.shardOf(u.ID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.users[u.ID]; exists {
		return fmt.Errorf("duplicate primary key: %s", u.ID)
	}
	s.users[u.ID] = u
	return nil
}

func (db *DBL3f) Get(id string) (User, bool) {
	s := db.shardOf(id)
	s.mu.RLock()
	u, ok := s.users[id]
	s.mu.RUnlock()
	return u, ok
}

func (db *DBL3f) Close() error { return nil }
