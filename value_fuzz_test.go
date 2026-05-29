package hazedb

import "testing"

// Round-trip every Value kind through the typed accessors AND the WAL cell
// codec (encodeCell → decodeCell), exercising the unsafe packing/unpacking.
// A decoded cell must Equal the original, and a Bytes payload must survive
// independently (encode copies into the WAL, decode allocates a fresh slice).
func FuzzValueRoundTrip(f *testing.F) {
	f.Add(int64(42), "hello", []byte("bytes"), uint8(1))
	f.Add(int64(-1<<40), "", []byte(nil), uint8(2))
	f.Add(int64(0), "unicode \xe2\x9c\x93 \x00\xff", []byte{0, 1, 2, 255}, uint8(3))
	f.Fuzz(func(t *testing.T, i int64, s string, b []byte, k uint8) {
		var v Value
		var wantUUID UUID
		switch ValueKind(k % 6) {
		case KindNull:
			v = Null()
		case KindInt:
			v = Int(i)
			if v.Int() != i {
				t.Fatalf("Int accessor: %d != %d", v.Int(), i)
			}
		case KindString:
			v = Str(s)
			if v.Str() != s {
				t.Fatalf("Str accessor: %q != %q", v.Str(), s)
			}
		case KindBytes:
			v = Bytes(b)
			if string(v.Bytes()) != string(b) {
				t.Fatalf("Bytes accessor: %q != %q", v.Bytes(), b)
			}
		case KindBool:
			v = Bool(i&1 == 1)
			if v.Bool() != (i&1 == 1) {
				t.Fatalf("Bool accessor")
			}
		case KindUUID:
			for j := 0; j < 16 && j < len(s); j++ {
				wantUUID[j] = s[j]
			}
			v = UUIDVal(wantUUID)
			if v.UUID() != wantUUID {
				t.Fatalf("UUID accessor: %v != %v", v.UUID(), wantUUID)
			}
		}

		enc := encodeCell(nil, v)
		got, n, err := decodeCell(enc)
		if err != nil {
			t.Fatalf("decodeCell: %v", err)
		}
		if n != len(enc) {
			t.Fatalf("decodeCell consumed %d of %d bytes", n, len(enc))
		}
		if !got.Equal(v) {
			t.Fatalf("cell round-trip mismatch: kind=%d %+v -> %+v", v.Kind, v, got)
		}
	})
}
