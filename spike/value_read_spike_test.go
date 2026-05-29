package spike

// Faithful point-read model: PK map lookup + project two columns (name, age),
// the exact shape of SELECT name, age FROM users WHERE id = ?. Three storage
// strategies:
//   Fat            — today: fat arena, fat result          (baseline)
//   PackedToPacked — option A: packed arena, packed result (smaller + fewer bytes copied)
//   PackedToFat    — option B: packed arena, fat result    (memory-only; result rebuilt fat)
// Isolates the row-materialisation delta; the map lookup is identical in all.

import (
	"encoding/binary"
	"testing"
	"unsafe"
)

func vpBuildFat(n int) (map[[16]byte]int, [][]vpFat) {
	idx := make(map[[16]byte]int, n)
	rows := make([][]vpFat, n)
	for i := 0; i < n; i++ {
		var u [16]byte
		binary.LittleEndian.PutUint64(u[:8], uint64(i))
		rows[i] = []vpFat{{Kind: vpUUID, U: u}, {Kind: vpStr, S: "alice"}, {Kind: vpInt, I: int64(i)}}
		idx[u] = i
	}
	return idx, rows
}

func vpBuildPacked(n int) (map[[16]byte]int, [][]vpPacked) {
	idx := make(map[[16]byte]int, n)
	rows := make([][]vpPacked, n)
	name := "alice"
	np := unsafe.Pointer(unsafe.StringData(name))
	for i := 0; i < n; i++ {
		var u [16]byte
		binary.LittleEndian.PutUint64(u[:8], uint64(i))
		rows[i] = []vpPacked{
			{kind: vpUUID, a: uint64(i)},
			{kind: vpStr, a: uint64(len(name)), p: np},
			{kind: vpInt, a: uint64(i)},
		}
		idx[u] = i
	}
	return idx, rows
}

func vpProjFat(r []vpFat, ords []int) []vpFat {
	pr := make([]vpFat, len(ords))
	for j, o := range ords {
		v := r[o]
		if v.Kind == vpBytes && v.B != nil {
			nb := make([]byte, len(v.B))
			copy(nb, v.B)
			v.B = nb
		}
		pr[j] = v
	}
	return pr
}

func vpProjPacked(r []vpPacked, ords []int) []vpPacked {
	pr := make([]vpPacked, len(ords))
	for j, o := range ords {
		c := r[o]
		if c.kind == vpBytes && c.p != nil {
			n := int(c.a)
			nb := make([]byte, n)
			copy(nb, unsafe.Slice((*byte)(c.p), n))
			if n > 0 {
				c.p = unsafe.Pointer(&nb[0])
			}
		}
		pr[j] = c
	}
	return pr
}

func vpProjPackedToFat(r []vpPacked, ords []int) []vpFat {
	pr := make([]vpFat, len(ords))
	for j, o := range ords {
		c := r[o]
		var v vpFat
		switch c.kind {
		case vpInt, vpBool:
			v = vpFat{Kind: c.kind, I: int64(c.a)}
		case vpUUID:
			var u [16]byte
			binary.LittleEndian.PutUint64(u[:8], c.a)
			binary.LittleEndian.PutUint64(u[8:], c.b)
			v = vpFat{Kind: vpUUID, U: u}
		case vpStr:
			if c.p != nil {
				v = vpFat{Kind: vpStr, S: unsafe.String((*byte)(c.p), int(c.a))}
			} else {
				v = vpFat{Kind: vpStr}
			}
		case vpBytes:
			if c.p != nil {
				n := int(c.a)
				nb := make([]byte, n)
				copy(nb, unsafe.Slice((*byte)(c.p), n))
				v = vpFat{Kind: vpBytes, B: nb}
			} else {
				v = vpFat{Kind: vpBytes}
			}
		default:
			v = vpFat{Kind: vpNull}
		}
		pr[j] = v
	}
	return pr
}

const vpReadN = 10000

var vpReadOrds = []int{1, 2} // name, age

func BenchmarkVPReadFat(b *testing.B) {
	idx, rows := vpBuildFat(vpReadN)
	var key [16]byte
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(key[:8], uint64(i%vpReadN))
		_ = vpProjFat(rows[idx[key]], vpReadOrds)
	}
}

func BenchmarkVPReadPackedToPacked(b *testing.B) {
	idx, rows := vpBuildPacked(vpReadN)
	var key [16]byte
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(key[:8], uint64(i%vpReadN))
		_ = vpProjPacked(rows[idx[key]], vpReadOrds)
	}
}

func BenchmarkVPReadPackedToFat(b *testing.B) {
	idx, rows := vpBuildPacked(vpReadN)
	var key [16]byte
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(key[:8], uint64(i%vpReadN))
		_ = vpProjPackedToFat(rows[idx[key]], vpReadOrds)
	}
}
