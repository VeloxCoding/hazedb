package spike_test

import (
	"sync/atomic"
	"testing"

	"github.com/VeloxCoding/hazedb/spike"
)

// Shard-count sweep. Same code, different shard counts. Tests whether
// V1's parallel-Get bottleneck is RWMutex contention (curable by more
// shards) or something deeper (lockless required).

func benchV5GetParallel(b *testing.B, numShards int) {
	const n = 100_000
	db := spike.OpenV5(numShards, n)
	defer db.Close()
	_, users := preGenUUIDs(n)
	ids := make([]string, n)
	for i := range users {
		ids[i] = string(users[i].ID[:])
		db.Insert(users[i], ids[i])
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, ok := db.GetByID(ids[i%n])
			if !ok {
				b.Fatal("missing")
			}
			i++
		}
	})
}

func Benchmark_V5_GetParallel_Shards16(b *testing.B)   { benchV5GetParallel(b, 16) }
func Benchmark_V5_GetParallel_Shards32(b *testing.B)   { benchV5GetParallel(b, 32) }
func Benchmark_V5_GetParallel_Shards64(b *testing.B)   { benchV5GetParallel(b, 64) }
func Benchmark_V5_GetParallel_Shards128(b *testing.B)  { benchV5GetParallel(b, 128) }
func Benchmark_V5_GetParallel_Shards256(b *testing.B)  { benchV5GetParallel(b, 256) }
func Benchmark_V5_GetParallel_Shards512(b *testing.B)  { benchV5GetParallel(b, 512) }
func Benchmark_V5_GetParallel_Shards1024(b *testing.B) { benchV5GetParallel(b, 1024) }

// Single-thread sanity — shard count should be irrelevant when one
// goroutine is on one shard at a time. Confirms no per-shard overhead.
func Benchmark_V5_GetSingle_Shards16(b *testing.B)   { benchV5GetSingle(b, 16) }
func Benchmark_V5_GetSingle_Shards256(b *testing.B)  { benchV5GetSingle(b, 256) }
func Benchmark_V5_GetSingle_Shards1024(b *testing.B) { benchV5GetSingle(b, 1024) }

func benchV5GetSingle(b *testing.B, numShards int) {
	const n = 100_000
	db := spike.OpenV5(numShards, n)
	defer db.Close()
	_, users := preGenUUIDs(n)
	ids := make([]string, n)
	for i := range users {
		ids[i] = string(users[i].ID[:])
		db.Insert(users[i], ids[i])
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok := db.GetByID(ids[i%n])
		if !ok {
			b.Fatal("missing")
		}
	}
}

// Mixed Insert+Get parallel — realistic workload, see how shard count
// affects write throughput too.

func benchV5MixedParallel(b *testing.B, numShards int) {
	const n = 100_000
	db := spike.OpenV5(numShards, n)
	defer db.Close()
	_, users := preGenUUIDs(n)
	ids := make([]string, n)
	for i := range users {
		ids[i] = string(users[i].ID[:])
		db.Insert(users[i], ids[i])
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// 90% reads, 10% writes — typical OLTP mix
			if i%10 == 0 {
				// dummy update (uses existing key)
				db.Insert(users[i%n], ids[i%n]) // will return dup error, fine
			} else {
				db.GetByID(ids[i%n])
			}
			i++
		}
	})
}

func Benchmark_V5_Mixed_Shards16(b *testing.B)   { benchV5MixedParallel(b, 16) }
func Benchmark_V5_Mixed_Shards256(b *testing.B)  { benchV5MixedParallel(b, 256) }
func Benchmark_V5_Mixed_Shards1024(b *testing.B) { benchV5MixedParallel(b, 1024) }

// Pure write parallel — N concurrent inserters, each with monotonic
// UUIDv7s. Tests whether shard contention rises again on writes despite
// the random tail of UUIDv7, and whether 128 vs 1024 shards trades off
// differently for writes than for reads.

func benchV5WriteParallel(b *testing.B, numShards int) {
	db := spike.OpenV5(numShards, b.N)
	defer db.Close()
	_, users := preGenUUIDs(b.N)
	ids := make([]string, b.N)
	for i := range users {
		ids[i] = string(users[i].ID[:])
	}
	b.ReportAllocs()
	b.ResetTimer()
	var ctr int64 = -1
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&ctr, 1)
			if int(i) >= len(users) {
				break
			}
			db.Insert(users[i], ids[i])
		}
	})
}

func Benchmark_V5_WriteParallel_Shards16(b *testing.B)   { benchV5WriteParallel(b, 16) }
func Benchmark_V5_WriteParallel_Shards64(b *testing.B)   { benchV5WriteParallel(b, 64) }
func Benchmark_V5_WriteParallel_Shards128(b *testing.B)  { benchV5WriteParallel(b, 128) }
func Benchmark_V5_WriteParallel_Shards256(b *testing.B)  { benchV5WriteParallel(b, 256) }
func Benchmark_V5_WriteParallel_Shards1024(b *testing.B) { benchV5WriteParallel(b, 1024) }
