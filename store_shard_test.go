package hazedb

import "testing"

// shardIdxOf must spread keys roughly evenly across shards for BOTH key shapes
// hazedb sees: random UUIDv7 (entropy in the low bytes) and sequential keys
// built by tid (entropy in the high timestamp bytes, low bytes zero). A hash
// that ignored either end would pile keys onto a few shards and serialise their
// RWMutexes under load — so this is a correctness guard on the hash, not a perf
// nicety. The band is wide enough that uniform random never false-fails (~11σ)
// but any gross skew (a near-constant index) trips it.
func TestShardIdxOfDistribution(t *testing.T) {
	const (
		nShards = 64
		n       = 200000
		lo, hi  = 0.80, 1.20
	)
	tbl := &table{mask: nShards - 1}
	expected := float64(n) / nShards

	check := func(name string, keyAt func(i int) UUID) {
		t.Helper()
		var counts [nShards]int
		for i := 0; i < n; i++ {
			idx := tbl.shardIdxOf(keyAt(i))
			if idx >= nShards {
				t.Fatalf("%s: shard index %d out of range [0,%d)", name, idx, nShards)
			}
			counts[idx]++
		}
		min, max := counts[0], counts[0]
		for _, c := range counts {
			if c < min {
				min = c
			}
			if c > max {
				max = c
			}
		}
		if float64(min) < expected*lo || float64(max) > expected*hi {
			t.Fatalf("%s: uneven over %d shards: min=%d max=%d expected≈%.0f (band %.0f..%.0f)",
				name, nShards, min, max, expected, expected*lo, expected*hi)
		}
	}

	check("random_uuidv7", func(i int) UUID { return NewUUIDv7() })
	check("sequential_tid", func(i int) UUID { return tid(i) })
	check("strided_tid", func(i int) UUID { return tid(i * 7919) })
}

// Isolated cost of the shard hash on the point-read hot path.
func BenchmarkShardIdxOf(b *testing.B) {
	tbl := &table{mask: 63}
	keys := make([]UUID, 1024)
	for i := range keys {
		keys[i] = NewUUIDv7()
	}
	b.ResetTimer()
	var sink uint32
	for i := 0; i < b.N; i++ {
		sink ^= tbl.shardIdxOf(keys[i&1023])
	}
	_ = sink
}
