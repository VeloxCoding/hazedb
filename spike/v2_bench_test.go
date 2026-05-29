package spike_test

import (
	"crypto/rand"
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VeloxCoding/hazedb/spike"
)

// preGenUUIDs returns N UUIDv7s + their User records, monotonically
// time-stamped so they're unique even at high call rates.
func preGenUUIDs(n int) ([]spike.UUIDv7, []spike.UserV2) {
	ids := make([]spike.UUIDv7, n)
	users := make([]spike.UserV2, n)
	now := time.Now().UnixMilli()
	for i := 0; i < n; i++ {
		var u spike.UUIDv7
		ts := now + int64(i/1_000_000) // bump ms every 1M to keep monotone
		u[0] = byte(ts >> 40)
		u[1] = byte(ts >> 32)
		u[2] = byte(ts >> 24)
		u[3] = byte(ts >> 16)
		u[4] = byte(ts >> 8)
		u[5] = byte(ts)
		// Use deterministic random tail so benches are reproducible.
		// Fill last 10 bytes with index-derived bytes XOR a fixed seed.
		binary.LittleEndian.PutUint64(u[6:14], uint64(i)*2654435761)
		binary.LittleEndian.PutUint16(u[14:16], uint16(i*31))
		u[6] = (u[6] & 0x0F) | 0x70
		u[8] = (u[8] & 0x3F) | 0x80
		ids[i] = u
		users[i] = spike.UserV2{
			ID:     u,
			Email:  "u" + intToStr(i) + "@x",
			Name:   "u" + intToStr(i),
			Active: true,
		}
	}
	_ = rand.Reader
	return ids, users
}

func intToStr(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// ====================================================================
//  V2DB — UUIDv7 + cached hash + sharded RWMutex
// ====================================================================

func Benchmark_V2_Insert(b *testing.B) {
	db := spike.OpenV2(b.N)
	defer db.Close()
	_, users := preGenUUIDs(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Insert(users[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_V2_GetByID(b *testing.B) {
	const n = 100_000
	db := spike.OpenV2(n)
	defer db.Close()
	ids, users := preGenUUIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(users[i])
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

func Benchmark_V2_GetByIDParallel(b *testing.B) {
	const n = 100_000
	db := spike.OpenV2(n)
	defer db.Close()
	ids, users := preGenUUIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(users[i])
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

func Benchmark_V2_InsertParallel(b *testing.B) {
	db := spike.OpenV2(b.N * 2)
	defer db.Close()
	_, users := preGenUUIDs(b.N + 10_000)
	b.ReportAllocs()
	b.ResetTimer()
	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1) - 1
			if int(i) >= len(users) {
				i = i % int64(len(users))
			}
			if err := db.Insert(users[i]); err != nil && err.Error() != "duplicate primary key" {
				b.Fatal(err)
			}
		}
	})
}

// ====================================================================
//  V3DB — UUIDv7 + atomic snapshot + drainer
// ====================================================================

func Benchmark_V3_InsertAsync(b *testing.B) {
	db := spike.OpenV3(b.N)
	defer db.Close()
	_, users := preGenUUIDs(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Insert(users[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_V3_InsertSync(b *testing.B) {
	db := spike.OpenV3(b.N)
	defer db.Close()
	_, users := preGenUUIDs(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.InsertSync(users[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_V3_GetByID(b *testing.B) {
	const n = 100_000
	db := spike.OpenV3(n)
	defer db.Close()
	ids, users := preGenUUIDs(n)
	db.BulkInsert(users)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok := db.GetByID(ids[i%n])
		if !ok {
			b.Fatal("missing")
		}
	}
}

func Benchmark_V3_GetByIDParallel(b *testing.B) {
	const n = 100_000
	db := spike.OpenV3(n)
	defer db.Close()
	ids, users := preGenUUIDs(n)
	db.BulkInsert(users)
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

// ====================================================================
//  V4DB — V3 architecture but with string PKs (Go's map fastpath)
// ====================================================================

func Benchmark_V4_GetByID(b *testing.B) {
	const n = 100_000
	db := spike.OpenV4(n)
	defer db.Close()
	_, users := preGenUUIDs(n)
	ids := make([]string, n)
	for i := range users {
		ids[i] = string(users[i].ID[:])
	}
	db.BulkInsert(users, ids)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok := db.GetByID(ids[i%n])
		if !ok {
			b.Fatal("missing")
		}
	}
}

func Benchmark_V4_GetByIDParallel(b *testing.B) {
	const n = 100_000
	db := spike.OpenV4(n)
	defer db.Close()
	_, users := preGenUUIDs(n)
	ids := make([]string, n)
	for i := range users {
		ids[i] = string(users[i].ID[:])
	}
	db.BulkInsert(users, ids)
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

func Benchmark_V3_InsertParallel(b *testing.B) {
	db := spike.OpenV3(b.N * 2)
	defer db.Close()
	_, users := preGenUUIDs(b.N + 10_000)
	b.ReportAllocs()
	b.ResetTimer()
	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt64(&counter, 1) - 1
			if int(i) >= len(users) {
				i = i % int64(len(users))
			}
			db.Insert(users[i])
		}
	})
}
