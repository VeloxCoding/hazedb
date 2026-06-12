package hazedb

import "time"

// Background arena compaction. A delete never reclaims a row's arena slot while
// the process runs — rowIDs must stay stable for the pkMap / pkDirectory / tails
// that point at them — so dead slots pile up on insert+delete workloads and
// scans (scanAll / scanPartition) drift toward O(ever-inserted) until a restart.
//
// A low-rate sweeper reclaims them OFF the write path: each tick it compacts any
// shard that has gone more-than-half dead (compactShardLocked relocates the live
// rows and renumbers). Running it in the background — not inline on the delete —
// keeps the O(live) relocation cost out of any user write's latency. The sweep
// pre-checks each shard under a shared lock and only escalates to the write lock
// for genuinely dense ones, so clean shards (the common case) cost a brief RLock.

const (
	// defaultCompactInterval is how often the sweeper looks for dense shards. An
	// idle sweep is ~9 ns/shard (BenchmarkSweepCompactIdle), so the interval costs
	// nothing when there is nothing to do; it mainly bounds reclaim latency (how
	// long dead slots linger after a shard crosses the dead>live threshold). 200ms
	// keeps that transient small without waking pointlessly often; the dead>live
	// gate prevents re-compacting a clean shard, so a short interval never thrashes.
	defaultCompactInterval = 200 * time.Millisecond
	// compactMinSlots skips small shards — their few dead slots cost less than the
	// rebuild, and the dead>live ratio is noisy at tiny sizes.
	compactMinSlots = 64
)

// shardDense reports whether shard s should be compacted: more dead slots than
// live rows, above the minimum size. Caller holds at least s.mu.RLock.
func shardDense(s *tableShard) bool {
	return len(s.rows)-s.live > s.live && len(s.rows) >= compactMinSlots
}

// compactShardIfDense compacts shard shardIdx when it is dense. A cheap
// shared-lock pre-check skips clean shards without blocking writers; a dense one
// escalates to the write lock(s) (pkDirectory then shard, the global order) and
// re-checks density before compacting, since it may have changed in between.
func (t *table) compactShardIfDense(shardIdx int) {
	s := &t.shards[shardIdx]
	s.mu.RLock()
	dense := shardDense(s)
	s.mu.RUnlock()
	if !dense {
		return
	}
	if t.pkDir != nil {
		t.pkDir.mu.Lock()
		s.mu.Lock()
		if shardDense(s) {
			t.compactShardLocked(s, uint32(shardIdx))
		}
		s.mu.Unlock()
		t.pkDir.mu.Unlock()
		return
	}
	s.mu.Lock()
	if shardDense(s) {
		t.compactShardLocked(s, uint32(shardIdx))
	}
	s.mu.Unlock()
}

// sweepCompact compacts every dense shard of every current table once. It reads
// the catalog snapshot, so a table dropped concurrently is simply not visited (or
// visited harmlessly — its detached storage is GC-bound either way).
func (db *DB) sweepCompact() {
	for _, rt := range db.cat.Load().byID {
		if rt == nil {
			continue
		}
		for i := range rt.shards {
			rt.compactShardIfDense(i)
		}
	}
}

// startCompactLoop launches the sweeper. Started by Open after replay (so it
// never races a replay reader) when compactInterval > 0.
func (db *DB) startCompactLoop(interval time.Duration) {
	db.compactStop = make(chan struct{})
	db.compactDone = make(chan struct{})
	go func() {
		defer close(db.compactDone)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-db.compactStop:
				return // reclamation is optional maintenance — no final sweep on Close
			case <-t.C:
				db.sweepCompact()
			}
		}
	}()
}

func (db *DB) stopCompactLoop() {
	if db.compactStop == nil {
		return
	}
	close(db.compactStop)
	<-db.compactDone
	db.compactStop = nil
}
