package hazedb

import "sync/atomic"

// byteBudget is the store-wide RAM admission control behind MaxBytes. total is
// the live byte sum across every table — the same per-row rowCost the per-shard
// tallies use — kept current by every mutation, so admission is one atomic load,
// not a walk. max is the hard ceiling: reserve rejects a write that would push
// total past it.
//
// max == 0 means UNLIMITED, and every method short-circuits on that first — a
// store opened without MaxBytes pays nothing (one predictable branch), so the
// accounting cost lands only on deployments that opt into a cap.
//
// reserve adds first and backs out if it overshot, so the ceiling is never
// exceeded — but two inserts racing the last free bytes can both back out and
// both reject, leaving a little headroom unused (conservative, never the
// reverse). release/adjust are plain atomic adds (a delete or size-changing
// UPDATE never needs to reject). One byteBudget is shared by every table in a DB;
// concurrent inserts contend on its one counter — the price of a global ceiling,
// paid only when enabled. A single Add beats a CAS-retry loop under that
// contention, which is why the rare boundary over-reject is the accepted trade.
type byteBudget struct {
	total atomic.Int64
	max   int64
}

// reserve admits cost bytes, adding them to the total, or returns false having
// added nothing when the write would exceed max. Unlimited (max == 0) always
// admits without touching the counter.
func (b *byteBudget) reserve(cost int64) bool {
	if b.max == 0 {
		return true
	}
	if b.total.Add(cost) > b.max {
		b.total.Add(-cost) // back out the reservation we just made
		return false
	}
	return true
}

// release returns cost bytes to the budget (a delete, or the shrink side of an
// UPDATE). No-op when unlimited.
func (b *byteBudget) release(cost int64) {
	if b.max != 0 {
		b.total.Add(-cost)
	}
}

// adjust applies a signed delta from an in-place UPDATE that changed a row's
// size. A grow can push total past max (only inserts are gated); the next insert
// then sees the over-budget total and is rejected until space frees. No-op when
// unlimited or when the size did not change.
func (b *byteBudget) adjust(delta int64) {
	if b.max != 0 && delta != 0 {
		b.total.Add(delta)
	}
}
