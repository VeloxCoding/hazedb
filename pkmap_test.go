package hazedb

import (
	"encoding/binary"
	"math/rand"
	"testing"
)

// --- deterministic key generators ---

// seqV7Key builds the i-th key of a worst-case sequential UUIDv7 stream: a
// real v7 layout (48-bit big-endian ms timestamp, version nibble, 12-bit
// counter) but with a FIXED random tail — so all entropy sits in the
// timestamp+counter bytes, the hardest realistic input for an unseeded hash.
func seqV7Key(i int) UUID {
	var k UUID
	ts := uint64(0x018f_1234_0000) + uint64(i>>12) // ms tick every counter wrap
	ctr := uint64(i) & 0xfff
	k[0] = byte(ts >> 40)
	k[1] = byte(ts >> 32)
	k[2] = byte(ts >> 24)
	k[3] = byte(ts >> 16)
	k[4] = byte(ts >> 8)
	k[5] = byte(ts)
	k[6] = 0x70 | byte(ctr>>8)
	k[7] = byte(ctr)
	k[8] = 0x80
	copy(k[9:], []byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03})
	return k
}

// randV7Key builds a v7-shaped key whose timestamp and tail both come from the
// seeded rng — the random-PK flavour, deterministic per seed.
func randV7Key(rng *rand.Rand) UUID {
	var k UUID
	binary.BigEndian.PutUint64(k[0:8], rng.Uint64())
	binary.BigEndian.PutUint64(k[8:16], rng.Uint64())
	k[6] = 0x70 | (k[6] & 0x0f)
	k[8] = 0x80 | (k[8] & 0x3f)
	return k
}

// --- model test: pkMap vs a reference map under churn ---

// runPKMapChurn drives ~200k mixed ops (reserve/commit inserts, dels, put
// upserts, gets) against a zero-value pkMap — so it passes through every
// growth doubling from 16 slots up — mirroring every op into a Go map and
// failing on the first divergence. genKey controls the key shape.
func runPKMapChurn(t *testing.T, seed int64, genKey func(rng *rand.Rand, i int) UUID) {
	t.Helper()
	var m pkMap
	ref := make(map[UUID]uint64)
	rng := rand.New(rand.NewSource(seed))
	var pool []UUID // every key ever inserted; dels/gets draw from it (may already be gone)

	pick := func(i int) UUID {
		if len(pool) > 0 && rng.Intn(4) > 0 {
			return pool[rng.Intn(len(pool))]
		}
		return genKey(rng, i)
	}

	const ops = 200_000
	for i := 0; i < ops; i++ {
		switch op := rng.Intn(10); {
		case op < 5: // insert-if-absent — the live insertJournaled shape
			k := genKey(rng, i)
			slot, found := m.reserve(k)
			if _, inRef := ref[k]; found != inRef {
				t.Fatalf("op %d: reserve(%v) found=%v, reference says %v", i, k, found, inRef)
			}
			if !found {
				rid := uint64(rng.Intn(1 << 20))
				m.commit(slot, k, rid)
				ref[k] = rid
				pool = append(pool, k)
			}
		case op < 7: // delete (existing or already-gone)
			k := pick(i)
			rid, ok := m.del(k)
			refRID, inRef := ref[k]
			if ok != inRef || (ok && rid != refRID) {
				t.Fatalf("op %d: del(%v) = (%d,%v), reference (%d,%v)", i, k, rid, ok, refRID, inRef)
			}
			delete(ref, k)
		case op < 8: // upsert — the tx-commit put shape
			k := pick(i)
			rid := uint64(rng.Intn(1 << 20))
			if _, inRef := ref[k]; !inRef {
				pool = append(pool, k)
			}
			m.put(k, rid)
			ref[k] = rid
		default: // point read
			k := pick(i)
			rid, ok := m.get(k)
			refRID, inRef := ref[k]
			if ok != inRef || (ok && rid != refRID) {
				t.Fatalf("op %d: get(%v) = (%d,%v), reference (%d,%v)", i, k, rid, ok, refRID, inRef)
			}
		}
		if i%20_000 == 0 && m.used != len(ref) {
			t.Fatalf("op %d: used=%d, reference holds %d", i, m.used, len(ref))
		}
	}

	// Full-state verification: counts match, every reference key reachable with
	// the right rowID, every occupied slot backed by the reference (no strays,
	// no entry stranded by a bad backward-shift).
	if m.used != len(ref) {
		t.Fatalf("final: used=%d, reference holds %d", m.used, len(ref))
	}
	for k, rid := range ref {
		got, ok := m.get(k)
		if !ok || got != rid {
			t.Fatalf("final: get(%v) = (%d,%v), want (%d,true)", k, got, ok, rid)
		}
	}
	occupied := 0
	for _, s := range m.slots {
		if s.ref == 0 {
			continue
		}
		occupied++
		if rid, inRef := ref[s.key]; !inRef || rid != s.ref-1 {
			t.Fatalf("final: slot holds (%v,%d) not in reference", s.key, s.ref-1)
		}
	}
	if occupied != len(ref) {
		t.Fatalf("final: %d occupied slots, reference holds %d", occupied, len(ref))
	}
}

func TestPKMapChurnRandomV7(t *testing.T) {
	runPKMapChurn(t, 1, func(rng *rand.Rand, _ int) UUID { return randV7Key(rng) })
}

func TestPKMapChurnSequentialV7(t *testing.T) {
	// i alone would repeat keys across ops (deliberate: re-insert after delete);
	// mix in a monotone counter so inserts mostly advance the sequence.
	n := 0
	runPKMapChurn(t, 2, func(_ *rand.Rand, _ int) UUID { n++; return seqV7Key(n) })
}

// TestPKMapRowIDZero pins ref = rowID+1: rowID 0 must round-trip through
// get/del distinguishably from "absent", including under the zero UUID key.
func TestPKMapRowIDZero(t *testing.T) {
	var m pkMap
	for _, k := range []UUID{seqV7Key(1), zeroUUID} {
		if _, ok := m.get(k); ok {
			t.Fatalf("get(%v) on empty map reported a hit", k)
		}
		m.put(k, 0)
		if rid, ok := m.get(k); !ok || rid != 0 {
			t.Fatalf("get(%v) = (%d,%v), want (0,true)", k, rid, ok)
		}
		if rid, ok := m.del(k); !ok || rid != 0 {
			t.Fatalf("del(%v) = (%d,%v), want (0,true)", k, rid, ok)
		}
		if _, ok := m.get(k); ok {
			t.Fatalf("get(%v) after del reported a hit", k)
		}
	}
}

// TestPKMapDrainToEmpty grows the table through many doublings, deletes every
// key in shuffled order (exercising backward-shift on long mixed chains), and
// requires a perfectly empty table — then proves the drained table is reusable.
func TestPKMapDrainToEmpty(t *testing.T) {
	var m pkMap
	rng := rand.New(rand.NewSource(3))
	const n = 50_000
	keys := make([]UUID, n)
	for i := range keys {
		if i%2 == 0 {
			keys[i] = seqV7Key(i)
		} else {
			keys[i] = randV7Key(rng)
		}
		m.put(keys[i], uint64(i))
	}
	rng.Shuffle(n, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	for _, k := range keys {
		if _, ok := m.del(k); !ok {
			t.Fatalf("del(%v) missed a key that was inserted", k)
		}
	}
	if m.used != 0 {
		t.Fatalf("drained map reports used=%d", m.used)
	}
	for i, s := range m.slots {
		if s.ref != 0 {
			t.Fatalf("drained map slot %d still occupied (%v)", i, s.key)
		}
	}
	m.put(keys[0], 7)
	if rid, ok := m.get(keys[0]); !ok || rid != 7 {
		t.Fatalf("reuse after drain: get = (%d,%v), want (7,true)", rid, ok)
	}
}

// TestPKMapWraparound pins the probe/backward-shift behaviour across the
// slots[len-1] → slots[0] boundary: keys homed at the top of a 16-slot table
// form a chain that wraps, then deletes from the middle must keep every
// survivor reachable.
func TestPKMapWraparound(t *testing.T) {
	var m pkMap
	m.init(8) // 16 slots, shift 60
	rng := rand.New(rand.NewSource(4))
	size := uint64(len(m.slots))
	var keys []UUID
	for len(keys) < 5 {
		k := randV7Key(rng)
		if h := pkHome(k, m.shift); h >= size-2 { // home in {14,15}: chain wraps past 0
			keys = append(keys, k)
		}
	}
	want := make(map[UUID]uint64, len(keys))
	for i, k := range keys {
		m.put(k, uint64(i))
		want[k] = uint64(i)
	}
	// Delete middle, head, tail of the wrapped chain, then the rest; after each
	// del every survivor must stay reachable with its rowID (a backward-shift
	// bug across the wrap strands or mismaps survivors).
	for _, di := range []int{2, 0, 4, 1, 3} {
		k := keys[di]
		if rid, ok := m.del(k); !ok || rid != want[k] {
			t.Fatalf("del(keys[%d]) = (%d,%v), want (%d,true)", di, rid, ok, want[k])
		}
		delete(want, k)
		for sk, srid := range want {
			if rid, ok := m.get(sk); !ok || rid != srid {
				t.Fatalf("after del(keys[%d]): survivor %v = (%d,%v), want (%d,true)", di, sk, rid, ok, srid)
			}
		}
	}
	if m.used != 0 {
		t.Fatalf("used=%d after deleting all wrapped keys", m.used)
	}
}

// TestPKMapHomeSlotDistribution guards the unseeded multiplicative fold: both
// random-v7 and worst-case sequential-v7 keys must spread evenly over a
// 4096-slot table — no bucket past 3× the mean, few empty buckets. A failure
// here means the hash constant or fold regressed, which would silently turn
// probe chains into linear scans.
func TestPKMapHomeSlotDistribution(t *testing.T) {
	const slotBits = 12
	const size = 1 << slotBits
	const shift = 64 - slotBits
	const n = 100_000
	mean := float64(n) / size

	check := func(name string, gen func(i int) UUID) {
		counts := make([]int, size)
		for i := 0; i < n; i++ {
			counts[pkHome(gen(i), shift)]++
		}
		maxC, empty := 0, 0
		for _, c := range counts {
			if c > maxC {
				maxC = c
			}
			if c == 0 {
				empty++
			}
		}
		if float64(maxC) > 3*mean {
			t.Errorf("%s: max bucket %d > 3×mean (%.1f) — hash clumping", name, maxC, mean)
		}
		if empty > size/20 {
			t.Errorf("%s: %d/%d buckets empty (>5%%) — hash not covering the table", name, empty, size)
		}
	}

	rng := rand.New(rand.NewSource(5))
	check("random-v7", func(_ int) UUID { return randV7Key(rng) })
	check("sequential-v7", seqV7Key)
}
