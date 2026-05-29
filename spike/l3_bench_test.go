package spike_test

import (
	"sync/atomic"
	"testing"

	"github.com/VeloxCoding/hazedb/spike"
)

// ---------- L3a: map[string]*User direct ----------

func Benchmark_L3a_Insert(b *testing.B) {
	db := spike.OpenL3a(b.N)
	defer db.Close()
	ids, emails, names := preGenIDs(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]}); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_L3a_Get(b *testing.B) {
	const n = 100_000
	db := spike.OpenL3a(n)
	defer db.Close()
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok := db.Get(ids[i%n])
		if !ok {
			b.Fatal("missing")
		}
	}
}

func Benchmark_L3a_GetParallel(b *testing.B) {
	const n = 100_000
	db := spike.OpenL3a(n)
	defer db.Close()
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]})
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, ok := db.Get(ids[i%n])
			if !ok {
				b.Fatal("missing")
			}
			i++
		}
	})
}

// ---------- L3b: map[string]User value-type ----------

func Benchmark_L3b_Insert(b *testing.B) {
	db := spike.OpenL3b(b.N)
	defer db.Close()
	ids, emails, names := preGenIDs(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]}); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_L3b_Get(b *testing.B) {
	const n = 100_000
	db := spike.OpenL3b(n)
	defer db.Close()
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok := db.Get(ids[i%n])
		if !ok {
			b.Fatal("missing")
		}
	}
}

func Benchmark_L3b_GetParallel(b *testing.B) {
	const n = 100_000
	db := spike.OpenL3b(n)
	defer db.Close()
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]})
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, ok := db.Get(ids[i%n])
			if !ok {
				b.Fatal("missing")
			}
			i++
		}
	})
}

// ---------- L3c: map[string]int + []User slice ----------

func Benchmark_L3c_Insert(b *testing.B) {
	db := spike.OpenL3c(b.N)
	defer db.Close()
	ids, emails, names := preGenIDs(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]}); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_L3c_Get(b *testing.B) {
	const n = 100_000
	db := spike.OpenL3c(n)
	defer db.Close()
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok := db.Get(ids[i%n])
		if !ok {
			b.Fatal("missing")
		}
	}
}

func Benchmark_L3c_GetParallel(b *testing.B) {
	const n = 100_000
	db := spike.OpenL3c(n)
	defer db.Close()
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]})
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, ok := db.Get(ids[i%n])
			if !ok {
				b.Fatal("missing")
			}
			i++
		}
	})
}

// ---------- L3d: hand-coded UsersDB (sqlc-style "compiled") ----------

func Benchmark_L3d_Insert(b *testing.B) {
	db := spike.OpenUsersDB(b.N)
	defer db.Close()
	ids, emails, names := preGenIDs(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]}); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_L3d_GetByID(b *testing.B) {
	const n = 100_000
	db := spike.OpenUsersDB(n)
	defer db.Close()
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]})
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

func Benchmark_L3d_GetByIDParallel(b *testing.B) {
	const n = 100_000
	db := spike.OpenUsersDB(n)
	defer db.Close()
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]})
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

// ---------- L3e: atomic-snapshot lock-free reads ----------

func setupL3e(n int) (*spike.DBL3e, []string) {
	db := spike.OpenL3e(n)
	ids, emails, names := preGenIDs(n)
	users := make([]spike.User, n)
	for i := 0; i < n; i++ {
		users[i] = spike.User{ID: ids[i], Email: emails[i], Name: names[i]}
	}
	db.InsertBulk(users)
	return db, ids
}

func Benchmark_L3e_Get(b *testing.B) {
	const n = 100_000
	db, ids := setupL3e(n)
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok := db.Get(ids[i%n])
		if !ok {
			b.Fatal("missing")
		}
	}
}

func Benchmark_L3e_GetParallel(b *testing.B) {
	const n = 100_000
	db, ids := setupL3e(n)
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, ok := db.Get(ids[i%n])
			if !ok {
				b.Fatal("missing")
			}
			i++
		}
	})
}

// ---------- L3f: 16-shard typed maps ----------

func Benchmark_L3f_Insert(b *testing.B) {
	db := spike.OpenL3f(b.N)
	defer db.Close()
	ids, emails, names := preGenIDs(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]}); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_L3f_Get(b *testing.B) {
	const n = 100_000
	db := spike.OpenL3f(n)
	defer db.Close()
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok := db.Get(ids[i%n])
		if !ok {
			b.Fatal("missing")
		}
	}
}

func Benchmark_L3f_GetParallel(b *testing.B) {
	const n = 100_000
	db := spike.OpenL3f(n)
	defer db.Close()
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]})
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, ok := db.Get(ids[i%n])
			if !ok {
				b.Fatal("missing")
			}
			i++
		}
	})
}

func Benchmark_L3f_InsertParallel(b *testing.B) {
	db := spike.OpenL3f(b.N * 2)
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&counter, 1)
			id := uintToString(n)
			if err := db.Insert(spike.User{ID: id, Email: id + "@x", Name: id}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ---------- V1 prototype: typed + sharded + WAL ----------

func Benchmark_V1_Insert(b *testing.B) {
	path := tempPath(b)
	db, err := spike.OpenV1(path, b.N)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	ids, emails, names := preGenIDs(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]}); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_V1_InsertMemory(b *testing.B) {
	db := spike.OpenV1Memory(b.N)
	defer db.Close()
	ids, emails, names := preGenIDs(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]}); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_V1_InsertParallel(b *testing.B) {
	path := tempPath(b)
	db, err := spike.OpenV1(path, b.N*2)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&counter, 1)
			id := uintToString(n)
			if err := db.Insert(spike.User{ID: id, Email: id + "@x", Name: id}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func Benchmark_V1_InsertBatch(b *testing.B) {
	const batchSize = 1000
	path := tempPath(b)
	db, err := spike.OpenV1(path, b.N)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	ids, emails, names := preGenIDs(b.N)
	batches := (b.N + batchSize - 1) / batchSize
	b.ReportAllocs()
	b.ResetTimer()
	for k := 0; k < batches; k++ {
		start := k * batchSize
		end := start + batchSize
		if end > b.N {
			end = b.N
		}
		users := make([]spike.User, end-start)
		for i := range users {
			j := start + i
			users[i] = spike.User{ID: ids[j], Email: emails[j], Name: names[j]}
		}
		if err := db.InsertBatch(users); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_V1_GetByID(b *testing.B) {
	const n = 100_000
	path := tempPath(b)
	db, err := spike.OpenV1(path, n)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]})
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

func Benchmark_V1_GetByIDParallel(b *testing.B) {
	const n = 100_000
	path := tempPath(b)
	db, err := spike.OpenV1(path, n)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	ids, emails, names := preGenIDs(n)
	for i := 0; i < n; i++ {
		db.Insert(spike.User{ID: ids[i], Email: emails[i], Name: names[i]})
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

func tempPath(b *testing.B) string {
	b.Helper()
	return b.TempDir() + "/v1.wal"
}

func uintToString(n int64) string {
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
