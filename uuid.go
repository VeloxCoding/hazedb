package hazedb

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// UUID is a 128-bit identifier (RFC 9562). It is a fixed array so it is
// comparable and usable directly as a key with no allocation — the
// primary key index is the open-addressed pkMap (pkmap.go) keyed by UUID.
type UUID [16]byte

// zeroUUID is the all-zero value; treated as "absent".
var zeroUUID UUID

// IsZero reports whether u is the zero UUID.
func (u UUID) IsZero() bool { return u == zeroUUID }

// Version returns the 4-bit version field (7 for a UUIDv7).
func (u UUID) Version() byte { return u[6] >> 4 }

// IsV7 reports whether u has the v7 version nibble and the RFC 4122/9562
// variant bits (10xx). Used to validate client-supplied IDs.
func (u UUID) IsV7() bool { return u.Version() == 7 && u[8]&0xC0 == 0x80 }

// String renders the canonical 8-4-4-4-12 lowercase hex form.
func (u UUID) String() string {
	var b [36]byte
	u.writeHyphenated(&b)
	return string(b[:])
}

// AppendString appends u's canonical 8-4-4-4-12 lowercase hex form to b and
// returns the extended slice — the allocation-free counterpart of String for
// hot encoders that pack into a reused buffer (the PHP row builder, JSON
// output), where String's per-call result string is pure waste.
func (u UUID) AppendString(b []byte) []byte {
	var tmp [36]byte
	u.writeHyphenated(&tmp)
	return append(b, tmp[:]...)
}

// writeHyphenated fills dst with u's canonical hex form; one layout definition
// shared by String and AppendString.
func (u UUID) writeHyphenated(dst *[36]byte) {
	hex.Encode(dst[0:8], u[0:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], u[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], u[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], u[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:36], u[10:16])
}

// hexNibble maps an ASCII byte to its hex value, or 0xFF if not a hex digit.
// One table lookup per nibble replaces hex.Decode's per-byte branching.
var hexNibble = func() [256]byte {
	var t [256]byte
	for i := range t {
		t[i] = 0xFF
	}
	for c := byte('0'); c <= '9'; c++ {
		t[c] = c - '0'
	}
	for c := byte('a'); c <= 'f'; c++ {
		t[c] = c - 'a' + 10
	}
	for c := byte('A'); c <= 'F'; c++ {
		t[c] = c - 'A' + 10
	}
	return t
}()

// uuidByteOff maps each of the 16 output bytes to the hi-nibble offset of its
// pair in the 36-char canonical string (lo-nibble is the next char); the
// hyphen positions 8/13/18/23 are skipped.
var uuidByteOff = [16]int{0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34}

// ParseUUID parses the canonical 36-character hyphenated hex form. It accepts
// any well-formed UUID (version is not enforced here — callers that require
// v7 check IsV7). Used at the API boundary to turn a string PK into the
// internal [16]byte; storage never sees the string form.
//
// Hand-decoded via a nibble lookup table in one pass — no per-segment
// hex.Decode call and no string→[]byte conversion (a hot read-path cost since
// every string PK arg lands here).
func ParseUUID(s string) (UUID, error) {
	u, ok := ParseUUIDOk(s)
	if !ok {
		return UUID{}, fmt.Errorf("%w: invalid UUID %q", ErrParse, s)
	}
	return u, nil
}

// ParseUUIDOk is ParseUUID without the error: it returns ok=false instead of
// formatting an error. The text adapters guess UUID-ness by trying to parse
// every string arg (the WHERE email=? case parses-and-fails on the hot path), so
// the discarded error string in ParseUUID was an allocation per non-UUID arg.
// Callers that only branch on success use this; callers that surface the error
// (DDL/coercion) use ParseUUID.
func ParseUUIDOk(s string) (UUID, bool) {
	var u UUID
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return u, false
	}
	for i := 0; i < 16; i++ {
		o := uuidByteOff[i]
		hi, lo := hexNibble[s[o]], hexNibble[s[o+1]]
		if hi == 0xFF || lo == 0xFF {
			return UUID{}, false
		}
		u[i] = hi<<4 | lo
	}
	return u, true
}

// --- monotonic UUIDv7 generator (RFC 9562 §5.7 + monotonic counter) ---

// uuidStamp is the generator clock packed into one atomic word: bits 16..63
// hold the 48-bit unix-ms timestamp, bits 0..15 the per-ms counter (12 bits
// used, so the low word never exceeds 0x0FFF). Every successful transition
// strictly increases the packed value, so each (ms, counter) pair is
// process-unique and totally time-ordered on its own — in-process uniqueness
// and monotonicity do not depend on rand_b. (Sharding the counter instead
// would rest in-process uniqueness on birthday bounds over rand_b's 62 bits
// and lose within-ms ordering across shards; the single CAS word keeps both
// guarantees unconditional and is cheaper than the mutex it replaced.)
// rand_b stays crypto-random for cross-process collision resistance and so
// IDs are not guessable from prior IDs.
var uuidStamp atomic.Uint64

// NewUUIDv7 returns a fresh UUIDv7 with within-millisecond monotonicity: IDs
// from this process sort by creation time, including inside one millisecond,
// via a 12-bit counter in rand_a (RFC 9562 "fixed-length dedicated counter").
// The counter caps at 4096 IDs per ms (≈4.1M auto-PKs/sec); beyond that the
// timestamp is nudged forward to keep the ordering total, so under sustained
// bulk-load above that rate the stamp drifts ahead of the wall clock (it
// self-corrects once the rate drops; uniqueness and ordering always hold).
//
// Auto-PK inserts (id omitted) are the only callers. The stamp advances by a
// single lock-free CAS, so parallel inserts never queue behind a generator
// mutex; rand_b is copied from one of uuidRandShards independently locked
// buffers. Client-supplied UUIDs skip this path entirely — no monotonicity
// guarantee, but also no shared state touched.
func NewUUIDv7() UUID {
	ms, c := nextUUIDStamp(time.Now().UnixMilli())

	// Shard pick by counter: same-ms concurrent claims hold consecutive
	// counters, so the mask spreads exactly the calls that are close enough
	// in time to contend. (At low rates every call lands on shard 0 — and
	// contention is nil there by definition.)
	var rb [8]byte
	uuidRand[c&(uuidRandShards-1)].read8(&rb)

	var u UUID
	// 48-bit big-endian unix-ms timestamp.
	u[0] = byte(ms >> 40)
	u[1] = byte(ms >> 32)
	u[2] = byte(ms >> 24)
	u[3] = byte(ms >> 16)
	u[4] = byte(ms >> 8)
	u[5] = byte(ms)
	// version 7 (high nibble) + 12-bit counter (rand_a) across bytes 6-7.
	u[6] = 0x70 | byte((c>>8)&0x0F)
	u[7] = byte(c)
	// variant 10 (top 2 bits of byte 8) + 62 bits rand_b.
	u[8] = 0x80 | (rb[0] & 0x3F)
	u[9] = rb[1]
	u[10] = rb[2]
	u[11] = rb[3]
	u[12] = rb[4]
	u[13] = rb[5]
	u[14] = rb[6]
	u[15] = rb[7]
	return u
}

// nextUUIDStamp advances uuidStamp past the current wall clock (unix ms) and
// returns the claimed (ms, counter). Lock-free: a failed CAS means another
// goroutine claimed the candidate stamp, so retry against its result.
func nextUUIDStamp(now int64) (ms int64, counter uint16) {
	for {
		cur := uuidStamp.Load()
		var nxt uint64
		if now > int64(cur>>16) {
			nxt = uint64(now) << 16 // fresh millisecond: counter restarts at 0
		} else {
			// Same millisecond, or the clock went backwards: stay on the
			// stamped ms and increment so IDs never regress. On counter
			// exhaustion borrow from the next millisecond.
			nxt = cur + 1
			if nxt&0xFFFF > 0x0FFF {
				nxt = (cur>>16 + 1) << 16
			}
		}
		if uuidStamp.CompareAndSwap(cur, nxt) {
			return int64(nxt >> 16), uint16(nxt & 0xFFFF)
		}
	}
}

// uuidRandShards must be a power of two so the shard pick is a mask.
const uuidRandShards = 8

// uuidRandShard bulk-buffers randomness for rand_b: one crypto/rand read
// serves many UUIDs, so the hot path is an 8-byte copy under the shard lock,
// not a syscall. 4080 bytes = 510 UUIDs per refill (all shards together hold
// 32 KB, negligible); the buffer is a multiple of 8 so a request never
// straddles a refill boundary. Sharding keeps one shard's refill syscall
// from stalling the others, and each struct spans multiple cache lines so
// the shard locks never false-share.
type uuidRandShard struct {
	mu  sync.Mutex
	pos int // counts down; the zero value forces a refill on first use
	buf [4080]byte
}

var uuidRand [uuidRandShards]uuidRandShard

// read8 copies 8 random bytes into dst, refilling the shard buffer in bulk
// when drained.
func (p *uuidRandShard) read8(dst *[8]byte) {
	p.mu.Lock()
	if p.pos < 8 {
		if _, err := rand.Read(p.buf[:]); err != nil {
			panic("hazedb: crypto/rand failed: " + err.Error())
		}
		p.pos = len(p.buf)
	}
	p.pos -= 8
	copy(dst[:], p.buf[p.pos:p.pos+8])
	p.mu.Unlock()
}
