package hazedb

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// Micro-bench guarding a REJECTED optimisation: preparing the drain's INSERT
// once per segment transaction and reusing the *sql.Stmt, instead of tx.Exec per
// row (drain.go applyMutation). The hypothesis was that tx.Exec recompiles the
// SQL each call (modernc has no statement cache) and prepared-once would be
// 2-5x faster. MEASURED: ~equal (per-row ~9.6 ms vs prepared ~9.5 ms / 1000
// rows, ~1% = noise; ~5% less alloc). database/sql routes tx.Exec through the
// driver's one-shot ExecerContext, and the real cost is the b-tree insert +
// commit, not the parse — so the drain keeps the simpler per-row Exec. Kept as
// the evidence; re-run before re-proposing.
//
// Same 1000-row batch per op, unique ids; the only difference is per-row Exec vs
// one Prepare + Stmt.Exec.
func benchDrainInsert(b *testing.B, prepared bool) {
	sdb, err := sql.Open("sqlite", filepath.Join(b.TempDir(), "m.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer sdb.Close()
	if _, err := sdb.Exec("CREATE TABLE t (id TEXT PRIMARY KEY, name TEXT, n INTEGER)"); err != nil {
		b.Fatal(err)
	}
	const rows = 1000
	const ins = "INSERT OR REPLACE INTO t (id, name, n) VALUES (?, ?, ?)"
	b.ResetTimer()
	for k := 0; k < b.N; k++ {
		tx, _ := sdb.Begin()
		if prepared {
			stmt, _ := tx.Prepare(ins)
			for i := 0; i < rows; i++ {
				stmt.Exec(fmt.Sprintf("%d-%d", k, i), "x", i)
			}
			stmt.Close()
		} else {
			for i := 0; i < rows; i++ {
				tx.Exec(ins, fmt.Sprintf("%d-%d", k, i), "x", i)
			}
		}
		tx.Commit()
	}
}

func BenchmarkDrainExecPerRow(b *testing.B) { benchDrainInsert(b, false) }
func BenchmarkDrainPrepared(b *testing.B)   { benchDrainInsert(b, true) }
