package hazedb

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// UUID is a 128-bit identifier (RFC 9562). It is a fixed array so it is
// comparable and usable directly as a map key with no allocation — the
// primary key index is map[UUID]uint64.
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
	hex.Encode(b[0:8], u[0:4])
	b[8] = '-'
	hex.Encode(b[9:13], u[4:6])
	b[13] = '-'
	hex.Encode(b[14:18], u[6:8])
	b[18] = '-'
	hex.Encode(b[19:23], u[8:10])
	b[23] = '-'
	hex.Encode(b[24:36], u[10:16])
	return string(b[:])
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
	var u UUID
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return u, fmt.Errorf("%w: invalid UUID %q", ErrParse, s)
	}
	for i := 0; i < 16; i++ {
		o := uuidByteOff[i]
		hi, lo := hexNibble[s[o]], hexNibble[s[o+1]]
		if hi == 0xFF || lo == 0xFF {
			return UUID{}, fmt.Errorf("%w: invalid UUID %q", ErrParse, s)
		}
		u[i] = hi<<4 | lo
	}
	return u, nil
}

// --- monotonic UUIDv7 generator (RFC 9562 §5.7 + monotonic counter) ---

var uuidGen = newUUIDGenerator()

// NewUUIDv7 returns a fresh UUIDv7 with within-millisecond monotonicity: IDs
// from this process sort by creation time, including inside one millisecond,
// via a 12-bit counter in rand_a (RFC 9562 "fixed-length dedicated counter").
// Beyond 4096 IDs in a single ms the timestamp is nudged forward to keep the
// ordering total. Client-supplied UUIDs get no such guarantee.
func NewUUIDv7() UUID { return uuidGen.next(time.Now().UnixMilli()) }

type uuidGenerator struct {
	mu      sync.Mutex
	lastMs  int64
	counter uint16 // 12-bit, within lastMs

	// Bulk-buffered randomness for rand_b: one crypto/rand read serves many
	// UUIDs, so the hot path is a copy under the lock, not a syscall.
	randBuf [504]byte
	randPos int
}

func newUUIDGenerator() *uuidGenerator {
	return &uuidGenerator{randPos: 504} // force a refill on first use
}

func (g *uuidGenerator) next(ms int64) UUID {
	g.mu.Lock()
	switch {
	case ms > g.lastMs:
		g.lastMs = ms
		g.counter = 0
	default:
		// Same millisecond, or the clock went backwards: stay on lastMs and
		// increment so IDs never regress. On counter exhaustion borrow from
		// the next millisecond.
		ms = g.lastMs
		g.counter++
		if g.counter > 0x0FFF {
			g.lastMs++
			ms = g.lastMs
			g.counter = 0
		}
	}
	c := g.counter

	var rb [8]byte
	g.fillRandLocked(rb[:])
	g.mu.Unlock()

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

// fillRandLocked copies len(dst) random bytes from the buffer, refilling in
// bulk when exhausted. Caller holds g.mu. randBuf is sized to a multiple of
// 8 so an 8-byte request never straddles a refill boundary.
func (g *uuidGenerator) fillRandLocked(dst []byte) {
	if g.randPos+len(dst) > len(g.randBuf) {
		if _, err := rand.Read(g.randBuf[:]); err != nil {
			panic("hazedb: crypto/rand failed: " + err.Error())
		}
		g.randPos = 0
	}
	copy(dst, g.randBuf[g.randPos:g.randPos+len(dst)])
	g.randPos += len(dst)
}
