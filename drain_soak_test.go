package hazedb

// Drain soak — a multi-minute, RAM-bounded workload against the LIVE background
// drain (rotate + drain tickers running, exactly as in production), then a
// quiesce and an exact reference == engine == SQLite comparison.
//
// RAM stays flat because the live set is held near a target: inserts and deletes
// are balanced (plus a steady stream of updates), so the table churns instead of
// only growing. Throughput is capped below the drain rate so sealed segments are
// consumed and deleted continuously — bounding disk too. Reuses the wl driver and
// comparison helpers from drain_fidelity_test.go.
//
// Skipped unless HAZEDB_SOAK_SECONDS is set, so `go test ./...` stays fast.

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// stepSteady runs one op chosen to keep the live set hovering near target:
// ~45% updates, and inserts/deletes balanced around the band so the row count
// (and thus RAM) stays bounded over an arbitrarily long run.
func (w *wl) stepSteady(t *testing.T, target int) {
	t.Helper()
	switch r := w.rng.Intn(100); {
	case len(w.live) > 0 && r < 45:
		w.update(t)
	case len(w.live) < target*95/100:
		w.insert(t)
	case len(w.live) > target*105/100:
		w.delete(t)
	default: // inside the band: churn 50/50 so both paths keep firing
		if w.rng.Intn(2) == 0 {
			w.insert(t)
		} else {
			w.delete(t)
		}
	}
}

func openItemsSoakDB(t *testing.T, dir, sqPath string) *DB {
	t.Helper()
	db, err := Open(Options{
		Schema:            itemsSchema(),
		WALLevel:          WALPeriodic,
		WALPath:           dir,
		SQLitePath:        sqPath,
		WALRotateInterval: time.Second, // background rotate; drain defaults to the same
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

func sqliteCount(t *testing.T, db *DB) int {
	t.Helper()
	var n int
	if err := db.sq.sdb.QueryRow(`SELECT COUNT(*) FROM "items"`).Scan(&n); err != nil {
		return -1
	}
	return n
}

func TestSoak_DrainFidelity(t *testing.T) {
	secs := os.Getenv("HAZEDB_SOAK_SECONDS")
	if secs == "" {
		t.Skip("set HAZEDB_SOAK_SECONDS=<n> to run the soak")
	}
	n, err := strconv.Atoi(secs)
	if err != nil || n <= 0 {
		t.Fatalf("bad HAZEDB_SOAK_SECONDS %q: %v", secs, err)
	}

	const (
		target     = 30_000  // steady-state live rows (flat RAM)
		batch      = 2_000   // ops between throttle checks
		ratePerSec = 120_000 // cap < drain rate (~200k/s) so the 1s drain keeps up
	)
	batchPause := time.Duration(int64(time.Second) * batch / ratePerSec)

	dir := t.TempDir()
	sqPath := filepath.Join(t.TempDir(), "soak.db")
	db := openItemsSoakDB(t, dir, sqPath)
	defer db.Close()
	w := newWL(db, 99)

	start := time.Now()
	deadline := start.Add(time.Duration(n) * time.Second)
	nextLog := start.Add(15 * time.Second)
	ops := 0
	for {
		for k := 0; k < batch; k++ {
			w.stepSteady(t, target)
		}
		ops += batch
		now := time.Now()
		if now.After(nextLog) {
			t.Logf("t=%4.0fs ops=%-9d live=%-6d sqlite=%-6d (ins=%d upd=%d del=%d)",
				now.Sub(start).Seconds(), ops, len(w.ref), sqliteCount(t, db), w.nIns, w.nUpd, w.nDel)
			nextLog = now.Add(15 * time.Second)
		}
		if now.After(deadline) {
			break
		}
		time.Sleep(batchPause)
	}

	// Quiesce: stopDrainLoop seals the active segment, drains the tail, and joins
	// the goroutine on the way out — so the mirror is fully current and nothing
	// drains concurrently with the comparison. It nil's drainStop, so the
	// deferred Close() is a safe no-op for the loop.
	db.stopDrainLoop()

	t.Logf("FINAL t=%ds: ops=%d live=%d sqlite=%d  (ins=%d upd=%d del=%d)",
		n, ops, len(w.ref), sqliteCount(t, db), w.nIns, w.nUpd, w.nDel)

	ref := refToMap(w.ref)
	eng := engineRows(t, db, itemsSelect)
	sq := sqliteRows(t, db.sq.sdb, itemsSelectSQL)
	compareMaps(t, "soak ref-vs-engine", ref, eng)
	compareMaps(t, "soak ref-vs-sqlite", ref, sq)
	compareMaps(t, "soak engine-vs-sqlite", eng, sq)
}
