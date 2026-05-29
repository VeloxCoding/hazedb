package spike

// Spike for RFC issue #2: would packing the ~64-72B Value struct into a
// ~32B unsafe tagged-union meaningfully cut row memory + clone cost?
//
// vpFat mirrors the real hazedb.Value layout; vpPacked overlaps the
// kind-exclusive fields (Int / String / Bytes / UUID) via two uint64 words +
// one unsafe.Pointer. UUID lives inline in the two words (no alloc, like
// today); string/bytes keep their data pointer in p so the GC still scans it.
// This measures size + clone speed only — not full operational correctness.

import (
	"testing"
	"unsafe"
)

type vpKind uint8

const (
	vpNull vpKind = iota
	vpInt
	vpStr
	vpBytes
	vpBool
	vpUUID
)

// vpFat: the current layout (Kind + int64 + string + []byte + [16]byte).
type vpFat struct {
	Kind vpKind
	I    int64
	S    string
	B    []byte
	U    [16]byte
}

// vpPacked: tagged union. a/b hold int, bool, len/cap, or the UUID's two
// halves; p holds a string/bytes data pointer (nil otherwise, so the GC only
// ever sees a real Go pointer or nil).
type vpPacked struct {
	kind vpKind
	a    uint64
	b    uint64
	p    unsafe.Pointer
}

func vpCloneFat(r []vpFat) []vpFat {
	c := make([]vpFat, len(r))
	copy(c, r)
	for i := range c {
		if c[i].Kind == vpBytes && c[i].B != nil {
			nb := make([]byte, len(c[i].B))
			copy(nb, c[i].B)
			c[i].B = nb
		}
	}
	return c
}

func vpClonePacked(r []vpPacked) []vpPacked {
	c := make([]vpPacked, len(r))
	copy(c, r)
	for i := range c {
		if c[i].kind == vpBytes && c[i].p != nil {
			n := int(c[i].a)
			src := unsafe.Slice((*byte)(c[i].p), n)
			nb := make([]byte, n)
			copy(nb, src)
			if n > 0 {
				c[i].p = unsafe.Pointer(&nb[0])
			}
		}
	}
	return c
}

// representative rows: a message-shaped row (id uuid, thread uuid, seq int,
// body bytes) and a wide row (uuid + 6 ints + 1 bytes).
func vpRows() (fatNarrow []vpFat, packedNarrow []vpPacked, fatWide []vpFat, packedWide []vpPacked) {
	body := []byte("hello world payload xyz!") // 24 bytes
	var u [16]byte
	for i := range u {
		u[i] = byte(i)
	}
	fatNarrow = []vpFat{
		{Kind: vpUUID, U: u},
		{Kind: vpUUID, U: u},
		{Kind: vpInt, I: 42},
		{Kind: vpBytes, B: body},
	}
	bp := unsafe.Pointer(&body[0])
	packedNarrow = []vpPacked{
		{kind: vpUUID, a: 1, b: 2},
		{kind: vpUUID, a: 1, b: 2},
		{kind: vpInt, a: 42},
		{kind: vpBytes, a: uint64(len(body)), p: bp},
	}
	fatWide = []vpFat{{Kind: vpUUID, U: u}}
	packedWide = []vpPacked{{kind: vpUUID, a: 1, b: 2}}
	for i := 0; i < 6; i++ {
		fatWide = append(fatWide, vpFat{Kind: vpInt, I: int64(i)})
		packedWide = append(packedWide, vpPacked{kind: vpInt, a: uint64(i)})
	}
	fatWide = append(fatWide, vpFat{Kind: vpBytes, B: body})
	packedWide = append(packedWide, vpPacked{kind: vpBytes, a: uint64(len(body)), p: bp})
	return
}

func TestValuePackSizes(t *testing.T) {
	t.Logf("sizeof vpFat    = %d bytes", unsafe.Sizeof(vpFat{}))
	t.Logf("sizeof vpPacked = %d bytes", unsafe.Sizeof(vpPacked{}))
	fn, pn, fw, pw := vpRows()
	t.Logf("narrow row (%d cols): fat backing = %d B, packed = %d B",
		len(fn), len(fn)*int(unsafe.Sizeof(vpFat{})), len(pn)*int(unsafe.Sizeof(vpPacked{})))
	t.Logf("wide row   (%d cols): fat backing = %d B, packed = %d B",
		len(fw), len(fw)*int(unsafe.Sizeof(vpFat{})), len(pw)*int(unsafe.Sizeof(vpPacked{})))
}

func BenchmarkVPCloneFatNarrow(b *testing.B) {
	r, _, _, _ := vpRows()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = vpCloneFat(r)
	}
}

func BenchmarkVPClonePackedNarrow(b *testing.B) {
	_, r, _, _ := vpRows()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = vpClonePacked(r)
	}
}

func BenchmarkVPCloneFatWide(b *testing.B) {
	_, _, r, _ := vpRows()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = vpCloneFat(r)
	}
}

func BenchmarkVPClonePackedWide(b *testing.B) {
	_, _, _, r := vpRows()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = vpClonePacked(r)
	}
}
