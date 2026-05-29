package hazedb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// --- Step 1: TXN WAL envelope codec + replay ---

// A recTxn envelope holding several sub-mutations replays all of them, in
// order, as one unit. Written here at the WAL level (no Tx API yet) to pin the
// envelope format and replay dispatch in isolation.
func TestTxnEnvelopeReplays(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "txn.wal")
	db, err := Open(Options{Schema: Schema{}, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	db.Exec("CREATE TABLE t (id uuid primary key, n int)")
	rt := db.cat.Load().byName["t"]

	a, b := tid(1), tid(2)
	rowA := Row{UUIDVal(a), Int(10)}
	rowB := Row{UUIDVal(b), Int(20)}
	muts := [][]byte{
		encodeInsertMutation(nil, rt.tableID, rowA),
		encodeInsertMutation(nil, rt.tableID, rowB),
	}
	if err := db.wal.writeRecord(recTxn, encodeTxn(nil, muts)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(Options{Schema: Schema{}, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for _, tc := range []struct {
		id UUID
		n  int64
	}{{a, 10}, {b, 20}} {
		_, rows, err := db2.Query("SELECT n FROM t WHERE id = ?", tc.id)
		if err != nil || len(rows) != 1 || rows[0][0].I != tc.n {
			t.Fatalf("txn member not replayed: id=%v rows=%v err=%v", tc.id, rows, err)
		}
	}
}

// A torn TXN envelope at the tail (truncated mid-record) is discarded whole —
// the transaction is atomic across a crash, never partially applied.
func TestTxnEnvelopeTornTailDiscarded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "torn.wal")
	db, err := Open(Options{Schema: Schema{}, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	db.Exec("CREATE TABLE t (id uuid primary key, n int)")
	rt := db.cat.Load().byName["t"]
	a := tid(1)
	db.Exec("INSERT INTO t (id, n) VALUES (?, ?)", a, 1) // a committed single mutation

	// Append a TXN envelope, then chop its last byte to simulate a crash
	// mid-write.
	muts := [][]byte{encodeInsertMutation(nil, rt.tableID, Row{UUIDVal(tid(2)), Int(2)})}
	if err := db.wal.writeRecord(recTxn, encodeTxn(nil, muts)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, fi.Size()-1); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(Options{Schema: Schema{}, WALPath: path})
	if err != nil {
		t.Fatalf("torn TXN tail should be tolerated, got %v", err)
	}
	defer db2.Close()
	if _, rows, _ := db2.Query("SELECT n FROM t WHERE id = ?", a); len(rows) != 1 {
		t.Fatal("committed pre-txn row lost")
	}
	if _, rows, _ := db2.Query("SELECT n FROM t WHERE id = ?", tid(2)); len(rows) != 0 {
		t.Fatal("torn txn was partially applied")
	}
}

// --- Step 2: Tx + db.Transaction commit engine ---

func acct(t *testing.T, db *DB, id UUID) int64 {
	t.Helper()
	_, rows, err := db.Query("SELECT balance FROM accounts WHERE id = ?", id)
	if err != nil || len(rows) != 1 {
		t.Fatalf("read balance %v: rows=%v err=%v", id, rows, err)
	}
	return rows[0][0].I
}

// The canonical transfer: two arithmetic UPDATEs on different rows commit
// together. Arithmetic SET is evaluated under the commit lock.
func TestTransactionTransferCommits(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE accounts (id uuid primary key, balance int)")
	from, to := tid(1), tid(2)
	db.Exec("INSERT INTO accounts (id, balance) VALUES (?, ?)", from, 100)
	db.Exec("INSERT INTO accounts (id, balance) VALUES (?, ?)", to, 0)

	err := db.Transaction(func(tx *Tx) error {
		if _, err := tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = ?", 30, from); err != nil {
			return err
		}
		_, err := tx.Exec("UPDATE accounts SET balance = balance + ? WHERE id = ?", 30, to)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := acct(t, db, from); got != 70 {
		t.Fatalf("from balance = %d, want 70", got)
	}
	if got := acct(t, db, to); got != 30 {
		t.Fatalf("to balance = %d, want 30", got)
	}
}

// Returning an error from the closure rolls everything back — neither row moves.
func TestTransactionRollback(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE accounts (id uuid primary key, balance int)")
	from, to := tid(1), tid(2)
	db.Exec("INSERT INTO accounts (id, balance) VALUES (?, ?)", from, 100)
	db.Exec("INSERT INTO accounts (id, balance) VALUES (?, ?)", to, 0)

	sentinel := fmt.Errorf("abort")
	err := db.Transaction(func(tx *Tx) error {
		tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = ?", 30, from)
		tx.Exec("UPDATE accounts SET balance = balance + ? WHERE id = ?", 30, to)
		return sentinel
	})
	if err != sentinel {
		t.Fatalf("got %v, want sentinel", err)
	}
	if acct(t, db, from) != 100 || acct(t, db, to) != 0 {
		t.Fatal("rollback did not discard staged writes")
	}
}

// A committed transaction survives a restart as one unit (TXN envelope replay).
func TestTransactionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tx.wal")
	db, err := Open(Options{Schema: Schema{}, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	from, to := tid(1), tid(2)
	db.Exec("CREATE TABLE accounts (id uuid primary key, balance int)")
	db.Exec("INSERT INTO accounts (id, balance) VALUES (?, ?)", from, 100)
	db.Exec("INSERT INTO accounts (id, balance) VALUES (?, ?)", to, 0)
	if err := db.Transaction(func(tx *Tx) error {
		tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = ?", 40, from)
		tx.Exec("UPDATE accounts SET balance = balance + ? WHERE id = ?", 40, to)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(Options{Schema: Schema{}, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if acct(t, db2, from) != 60 || acct(t, db2, to) != 40 {
		t.Fatalf("transaction did not survive restart: from=%d to=%d", acct(t, db2, from), acct(t, db2, to))
	}
}

// Read-your-writes: a statement sees the effect of an earlier statement in the
// same transaction (INSERT then UPDATE the same PK).
func TestTransactionReadYourWrites(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE t (id uuid primary key, n int)")
	id := tid(1)
	err := db.Transaction(func(tx *Tx) error {
		if _, err := tx.Exec("INSERT INTO t (id, n) VALUES (?, ?)", id, 5); err != nil {
			return err
		}
		// arithmetic against the row inserted earlier in this same tx
		_, err := tx.Exec("UPDATE t SET n = n + ? WHERE id = ?", 10, id)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	_, rows, _ := db.Query("SELECT n FROM t WHERE id = ?", id)
	if len(rows) != 1 || rows[0][0].I != 15 {
		t.Fatalf("read-your-writes failed: %v", rows)
	}
}

// A partitioned table works inside a transaction: INSERT then UPDATE within one
// tx, indexed partition scan sees the committed result.
func TestTransactionPartitioned(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE msgs (id uuid primary key, thread uuid partition key, seq int immutable, n int)")
	th := tid(100)
	err := db.Transaction(func(tx *Tx) error {
		tx.Exec("INSERT INTO msgs (id, thread, seq, n) VALUES (?, ?, ?, ?)", tid(1), th, 0, 1)
		tx.Exec("INSERT INTO msgs (id, thread, seq, n) VALUES (?, ?, ?, ?)", tid(2), th, 1, 2)
		_, err := tx.Exec("UPDATE msgs SET n = n + ? WHERE id = ?", 100, tid(2))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	_, rows, err := db.Query("SELECT n FROM msgs WHERE thread = ? ORDER BY seq", th)
	if err != nil || len(rows) != 2 || rows[0][0].I != 1 || rows[1][0].I != 102 {
		t.Fatalf("partitioned tx result wrong: rows=%v err=%v", rows, err)
	}
}

// --- Step 3: v1 guardrails ---

func TestTransactionRejects(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE t (id uuid primary key, n int)")
	db.Exec("CREATE TABLE u (id uuid primary key, n int)")

	cases := []struct {
		name string
		body func(tx *Tx) (int, error)
	}{
		{"select", func(tx *Tx) (int, error) { return tx.Exec("SELECT n FROM t WHERE id = ?", tid(1)) }},
		{"ddl", func(tx *Tx) (int, error) { return tx.Exec("CREATE TABLE z (id uuid primary key)") }},
		{"non-pk-update", func(tx *Tx) (int, error) { return tx.Exec("UPDATE t SET n = ? WHERE n = ?", 1, 2) }},
		{"non-pk-delete", func(tx *Tx) (int, error) { return tx.Exec("DELETE FROM t WHERE n = ?", 2) }},
		{"two-tables", func(tx *Tx) (int, error) {
			if _, err := tx.Exec("UPDATE t SET n = ? WHERE id = ?", 1, tid(1)); err != nil {
				return 0, err
			}
			return tx.Exec("UPDATE u SET n = ? WHERE id = ?", 1, tid(1))
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := db.Transaction(func(tx *Tx) error {
				_, e := c.body(tx)
				return e
			})
			if !errors.Is(err, ErrTxUnsupported) {
				t.Fatalf("expected ErrTxUnsupported, got %v", err)
			}
		})
	}
}

// A failed tx.Exec poisons the transaction: even if the closure ignores the
// error and returns nil, the whole transaction rolls back and Transaction
// returns the sticky error. An earlier successful statement is discarded.
func TestTransactionPoisonOnError(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE t (id uuid primary key, n int)")
	id := tid(1)
	err := db.Transaction(func(tx *Tx) error {
		tx.Exec("INSERT INTO t (id, n) VALUES (?, ?)", id, 1) // staged OK
		tx.Exec("UPDATE t SET n = ? WHERE n = ?", 5, 1)       // poisons (non-PK-pinned)
		return nil                                            // ignored error, try to commit anyway
	})
	if !errors.Is(err, ErrTxUnsupported) {
		t.Fatalf("expected sticky ErrTxUnsupported, got %v", err)
	}
	if _, rows, _ := db.Query("SELECT n FROM t WHERE id = ?", id); len(rows) != 0 {
		t.Fatal("poisoned transaction applied its earlier statement")
	}
}

// A PK that is unique at stage time but already present at commit fails the
// whole transaction (uniqueness is validated under the commit lock), leaving
// the existing row untouched.
func TestTransactionDuplicatePKAtCommit(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE t (id uuid primary key, n int)")
	a := tid(1)
	db.Exec("INSERT INTO t (id, n) VALUES (?, ?)", a, 1)
	err := db.Transaction(func(tx *Tx) error {
		_, e := tx.Exec("INSERT INTO t (id, n) VALUES (?, ?)", a, 99)
		return e
	})
	if !errors.Is(err, ErrDuplicatePK) {
		t.Fatalf("expected ErrDuplicatePK at commit, got %v", err)
	}
	if _, rows, _ := db.Query("SELECT n FROM t WHERE id = ?", a); len(rows) != 1 || rows[0][0].I != 1 {
		t.Fatalf("existing row disturbed by failed tx: %v", rows)
	}
}

// Concurrent transfers must conserve the total balance: atomic commit + the
// arithmetic SET evaluated under the commit lock means no lost updates. Run
// with -race. The sum invariant is the real test — a torn or non-serialisable
// commit would leak or duplicate balance.
func TestTransactionConcurrentConserves(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE accounts (id uuid primary key, balance int)")
	const N = 50
	ids := make([]UUID, N)
	for i := range ids {
		ids[i] = tid(i)
		db.Exec("INSERT INTO accounts (id, balance) VALUES (?, ?)", ids[i], 1000)
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 400; i++ {
				from := ids[(g*7+i)%N]
				to := ids[(g*13+i+1)%N]
				if from == to {
					continue
				}
				db.Transaction(func(tx *Tx) error {
					if _, err := tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = ?", 1, from); err != nil {
						return err
					}
					_, err := tx.Exec("UPDATE accounts SET balance = balance + ? WHERE id = ?", 1, to)
					return err
				})
			}
		}(g)
	}
	wg.Wait()
	var total int64
	for _, id := range ids {
		total += acct(t, db, id)
	}
	if total != N*1000 {
		t.Fatalf("balance not conserved: total=%d, want %d", total, N*1000)
	}
}

// Measures the commit path (lock-all-shards + one TXN envelope) for a 2-row
// transfer. This is the number that justifies (or revisits) the lock-all-shards
// v1 choice; transactions are opt-in, so this is not the point-op hot path.
func BenchmarkTransactionTransfer(b *testing.B) {
	db, _ := Open(Options{Schema: Schema{}})
	defer db.Close()
	db.Exec("CREATE TABLE accounts (id uuid primary key, balance int)")
	from, to := tid(1), tid(2)
	db.Exec("INSERT INTO accounts (id, balance) VALUES (?, ?)", from, 1<<62)
	db.Exec("INSERT INTO accounts (id, balance) VALUES (?, ?)", to, 0)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.Transaction(func(tx *Tx) error {
			tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = ?", 1, from)
			tx.Exec("UPDATE accounts SET balance = balance + ? WHERE id = ?", 1, to)
			return nil
		})
	}
}
