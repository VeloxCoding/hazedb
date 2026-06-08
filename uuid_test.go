package hazedb

import (
	"bytes"
	"sync"
	"testing"
)

func TestUUIDv7VersionVariant(t *testing.T) {
	u := NewUUIDv7()
	if u.Version() != 7 {
		t.Errorf("version = %d, want 7", u.Version())
	}
	if !u.IsV7() {
		t.Errorf("IsV7() false for %s (byte8=%#x)", u, u[8])
	}
	if u.IsZero() {
		t.Error("fresh UUID is zero")
	}
}

// Sequentially generated IDs must be strictly increasing as byte strings,
// including within a single millisecond (the monotonic counter).
func TestUUIDv7Monotonic(t *testing.T) {
	const n = 100_000
	prev := NewUUIDv7()
	for i := 1; i < n; i++ {
		u := NewUUIDv7()
		if bytes.Compare(u[:], prev[:]) <= 0 {
			t.Fatalf("not monotonic at %d: %s <= %s", i, u, prev)
		}
		prev = u
	}
}

func TestUUIDv7Unique(t *testing.T) {
	const n = 200_000
	seen := make(map[UUID]struct{}, n)
	for i := 0; i < n; i++ {
		u := NewUUIDv7()
		if _, dup := seen[u]; dup {
			t.Fatalf("duplicate at %d: %s", i, u)
		}
		seen[u] = struct{}{}
	}
}

func TestUUIDParseRoundTrip(t *testing.T) {
	for i := 0; i < 1000; i++ {
		u := NewUUIDv7()
		s := u.String()
		if len(s) != 36 {
			t.Fatalf("bad length %d for %q", len(s), s)
		}
		back, err := ParseUUID(s)
		if err != nil {
			t.Fatalf("parse %q: %v", s, err)
		}
		if back != u {
			t.Fatalf("round-trip mismatch: %s != %s", back, u)
		}
	}
}

func TestUUIDParseRejectsGarbage(t *testing.T) {
	bad := []string{
		"", "not-a-uuid",
		"0190f7e0-1234-7abc-8def-0123456789ab-extra",
		"0190f7e012347abc8def0123456789ab",     // no hyphens
		"0190f7e0-1234-7abc-8def-0123456789aZ", // non-hex
		"0190f7e0_1234_7abc_8def_0123456789ab", // wrong separators
	}
	for _, s := range bad {
		if _, err := ParseUUID(s); err == nil {
			t.Errorf("expected error for %q", s)
		}
	}
}

// Concurrent generation must stay unique and valid (no torn counter/rand
// under the lock). Run with -race.
func TestUUIDv7ConcurrentUnique(t *testing.T) {
	const goroutines, per = 16, 10_000
	var mu sync.Mutex
	seen := make(map[UUID]struct{}, goroutines*per)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]UUID, per)
			for i := range local {
				local[i] = NewUUIDv7()
			}
			mu.Lock()
			for _, u := range local {
				if !u.IsV7() {
					t.Errorf("invalid v7: %s", u)
				}
				if _, dup := seen[u]; dup {
					t.Errorf("duplicate across goroutines: %s", u)
				}
				seen[u] = struct{}{}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(seen) != goroutines*per {
		t.Fatalf("got %d unique, want %d", len(seen), goroutines*per)
	}
}

func BenchmarkNewUUIDv7(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = NewUUIDv7()
	}
}

func BenchmarkNewUUIDv7Parallel(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = NewUUIDv7()
		}
	})
}

// TestUUIDAppendStringMatchesString pins AppendString to the same bytes as String.
func TestUUIDAppendStringMatchesString(t *testing.T) {
	for i := 0; i < 1000; i++ {
		u := NewUUIDv7()
		if got := string(u.AppendString(nil)); got != u.String() {
			t.Fatalf("AppendString=%q String=%q", got, u.String())
		}
	}
	// non-empty prefix is preserved
	if got := string(NewUUIDv7().AppendString([]byte("x="))); got[:2] != "x=" || len(got) != 38 {
		t.Fatalf("prefix not preserved: %q", got)
	}
}

func BenchmarkUUIDString(b *testing.B) {
	u := NewUUIDv7()
	b.ReportAllocs()
	var s string
	for i := 0; i < b.N; i++ {
		s = u.String()
	}
	_ = s
}

func BenchmarkUUIDAppendString(b *testing.B) {
	u := NewUUIDv7()
	buf := make([]byte, 0, 36)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = u.AppendString(buf[:0])
	}
	_ = buf
}
