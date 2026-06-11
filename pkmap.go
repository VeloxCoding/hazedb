package hazedb

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"math/bits"
)

// pkMap is the per-shard PK→rowID index: a linear-probed open-addressed table
// replacing map[UUID]uint64 on the hottest write block (the duplicate-PK check
// + insert). It wins over the runtime map by (a) hashing with one multiplicative
// fold instead of the full AES path, (b) probing key+ref in one 24-byte slot
// (no separate control-byte group walk), and (c) letting the insert path probe
// ONCE for both the duplicate check and the target slot (reserve/commit), where
// the map costs a mapaccess2 plus a mapassign.
//
// Invariants:
//   - len(slots) is a power of two (or 0); shift == 64 - log2(len(slots)).
//   - ref == 0 marks an empty slot; an entry stores rowID+1, so the zero value
//     is a valid empty map and rowID 0 stays representable.
//   - load is kept ≤ 3/4: reserve doubles the table before it would exceed it.
//   - deletion backward-shifts (Knuth 6.4R) instead of tombstoning — DELETEs
//     are ongoing, and tombstones would accrete and stretch every later probe.
//   - the hash XORs a per-process random seed into the fold before the fixed
//     multiplier, so an adversarial client cannot craft PKs that collide into
//     one probe chain without knowing the seed (the runtime map gets the same
//     property from its seeded AES). Even a fully-colliding chain is bounded:
//     one shard, and growth keeps it ≤ 3/4 of that shard's table — degraded
//     to a linear scan of that shard's keys, not the table.
//
// Not safe for concurrent use; every call site already holds the shard lock.
type pkMap struct {
	slots []pkSlot
	shift uint8 // 64 - log2(len(slots)); home slot = hash >> shift
	used  int   // live entries
}

type pkSlot struct {
	key UUID
	ref uint64 // rowID+1; 0 = empty
}

// pkHashSeed is XOR-ed into every fold (see pkHome). Seeded once per process;
// if entropy is unavailable the zero seed degrades to the unseeded fold, never
// to a broken one.
var pkHashSeed = func() uint64 {
	var b [8]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		return 0
	}
	return binary.LittleEndian.Uint64(b[:])
}()

// pkHome folds both 64-bit halves of the UUID into a home slot — same shape as
// shardIdxOf, but a DIFFERENT odd constant (SplitMix64's first mixer) so the
// slot bits do not correlate with shard routing, and the top bits (the best
// mixed) selected via shift rather than a fixed window. The per-process seed
// is XOR-ed in BEFORE the multiply: the multiplier keeps its avalanche
// quality, while the home slot of any given key stays unpredictable across
// processes (adversarial-PK guard; see the type comment).
func pkHome(k UUID, shift uint8) uint64 {
	a := binary.LittleEndian.Uint64(k[0:8])
	b := binary.LittleEndian.Uint64(k[8:16])
	return ((a ^ bits.RotateLeft64(b, 32) ^ pkHashSeed) * 0xBF58476D1CE4E5B9) >> shift
}

// init pre-sizes an empty map so hint entries fit without growing (the
// make(map, hint) counterpart for newTable's per-shard size hint).
func (m *pkMap) init(hint int) {
	n := 16
	for 4*hint > 3*n {
		n <<= 1
	}
	m.slots = make([]pkSlot, n)
	m.shift = uint8(64 - bits.Len(uint(n-1)))
}

// get returns the rowID stored under k. Probes from the home slot until the
// key or an empty slot; the empty check runs first, so a stored zero UUID can
// never be confused with an empty slot's zero key.
func (m *pkMap) get(k UUID) (uint64, bool) {
	if len(m.slots) == 0 {
		return 0, false
	}
	mask := uint64(len(m.slots) - 1)
	i := pkHome(k, m.shift)
	for {
		s := &m.slots[i]
		if s.ref == 0 {
			return 0, false
		}
		if s.key == k {
			return s.ref - 1, true
		}
		i = (i + 1) & mask
	}
}

// reserve probes once for k, growing first if the insert would push load past
// 3/4. found=true means k already exists (slot is its index — the duplicate-PK
// reject path). found=false means slot is the empty slot where k belongs; the
// caller stores it with commit. The reservation is only valid while the shard
// lock is held and no other pkMap mutation intervenes — exactly the journaled
// insert's shape (reserve → WAL append → commit), where the WAL append never
// touches the map. A growth triggered by an insert that turns out to be a
// duplicate is harmless (just early).
func (m *pkMap) reserve(k UUID) (slot uint64, found bool) {
	if 4*(m.used+1) > 3*len(m.slots) {
		m.grow()
	}
	mask := uint64(len(m.slots) - 1)
	i := pkHome(k, m.shift)
	for {
		s := &m.slots[i]
		if s.ref == 0 {
			return i, false
		}
		if s.key == k {
			return i, true
		}
		i = (i + 1) & mask
	}
}

// commit fills the empty slot a reserve miss returned. See reserve for the
// validity window.
func (m *pkMap) commit(slot uint64, k UUID, rowID uint64) {
	m.slots[slot] = pkSlot{key: k, ref: rowID + 1}
	m.used++
}

// put inserts or overwrites k (the replay/tx-commit assign, map[k]=v parity).
func (m *pkMap) put(k UUID, rowID uint64) {
	slot, found := m.reserve(k)
	m.slots[slot] = pkSlot{key: k, ref: rowID + 1}
	if !found {
		m.used++
	}
}

// del removes k and returns the rowID it held. The vacated slot is repaired by
// backward-shift compaction (Knuth 6.4R): walk forward from the gap; any entry
// whose home slot does NOT lie cyclically in (gap, entry] would become
// unreachable past the gap, so it moves into the gap and the walk continues
// from its old position; the first empty slot ends the chain. No tombstones,
// so probe chains never outlive their entries.
func (m *pkMap) del(k UUID) (uint64, bool) {
	if len(m.slots) == 0 {
		return 0, false
	}
	mask := uint64(len(m.slots) - 1)
	i := pkHome(k, m.shift)
	for {
		s := &m.slots[i]
		if s.ref == 0 {
			return 0, false
		}
		if s.key == k {
			break
		}
		i = (i + 1) & mask
	}
	rowID := m.slots[i].ref - 1
	j := i
	for {
		j = (j + 1) & mask
		s := &m.slots[j]
		if s.ref == 0 {
			break
		}
		h := pkHome(s.key, m.shift)
		if (j > i && (h <= i || h > j)) || (j < i && (h <= i && h > j)) {
			m.slots[i] = *s
			i = j
		}
	}
	m.slots[i] = pkSlot{}
	m.used--
	return rowID, true
}

// grow doubles the table (16 slots from empty) and reinserts every entry under
// the new shift. Probe order within a chain may change; chains only shorten.
func (m *pkMap) grow() {
	n := len(m.slots) * 2
	if n < 16 {
		n = 16
	}
	old := m.slots
	m.slots = make([]pkSlot, n)
	m.shift = uint8(64 - bits.Len(uint(n-1)))
	mask := uint64(n - 1)
	for idx := range old {
		s := &old[idx]
		if s.ref == 0 {
			continue
		}
		i := pkHome(s.key, m.shift)
		for m.slots[i].ref != 0 {
			i = (i + 1) & mask
		}
		m.slots[i] = *s
	}
}
