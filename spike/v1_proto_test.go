package spike

import (
	"path/filepath"
	"testing"
)

func TestV1_InsertGet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.wal")
	db, err := OpenV1(path, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Insert(User{ID: "alice", Email: "a@x", Name: "Alice"}); err != nil {
		t.Fatal(err)
	}
	u, ok := db.GetByID("alice")
	if !ok {
		t.Fatal("alice not found")
	}
	if u.Email != "a@x" || u.Name != "Alice" {
		t.Fatalf("wrong user: %+v", u)
	}
}

func TestV1_DuplicateRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.wal")
	db, _ := OpenV1(path, 1024)
	defer db.Close()

	if err := db.Insert(User{ID: "x", Email: "a", Name: "A"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Insert(User{ID: "x", Email: "b", Name: "B"}); err == nil {
		t.Fatal("expected dup error")
	}
}

func TestV1_Recovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.wal")

	db1, err := OpenV1(path, 1024)
	if err != nil {
		t.Fatal(err)
	}
	db1.Insert(User{ID: "alice", Email: "a@x", Name: "Alice"})
	db1.Insert(User{ID: "bob", Email: "b@x", Name: "Bob"})
	db1.Close()

	db2, err := OpenV1(path, 1024)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db2.Close()

	alice, ok := db2.GetByID("alice")
	if !ok || alice.Email != "a@x" {
		t.Fatalf("alice not recovered: ok=%v u=%+v", ok, alice)
	}
	bob, ok := db2.GetByID("bob")
	if !ok || bob.Name != "Bob" {
		t.Fatalf("bob not recovered: ok=%v u=%+v", ok, bob)
	}
}

func TestV1_ShardDistribution(t *testing.T) {
	db := OpenV1Memory(10_000)
	defer db.Close()

	for i := 0; i < 10_000; i++ {
		id := uintToString64(int64(i))
		db.Insert(User{ID: id, Email: id, Name: id})
	}

	counts := make([]int, len(db.shards))
	for i := range db.shards {
		counts[i] = len(db.shards[i].users)
	}

	total := 0
	for _, c := range counts {
		total += c
	}
	if total != 10_000 {
		t.Fatalf("lost rows: total=%d", total)
	}
	avg := total / len(db.shards)
	for i, c := range counts {
		if c > avg*4 {
			t.Fatalf("shard %d overloaded: %d vs avg %d", i, c, avg)
		}
	}
}

func uintToString64(n int64) string {
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
