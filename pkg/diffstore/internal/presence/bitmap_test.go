package presence

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func TestBitmapBoundariesAndCorruption(t *testing.T) {
	for _, size := range []int64{PageSize, 7 * PageSize, 8 * PageSize, 9 * PageSize, int64(PayloadSize) * 8 * PageSize, MaxSize} {
		bitmap := make([]byte, Bytes(size))
		Set(bitmap, 0, true)
		Set(bitmap, uint64(size/PageSize-1), true)
		reopened := make([]byte, len(bitmap))
		for i := 0; i < Pages(size); i++ {
			b := Encode(bitmap, size, i)
			part, err := Decode(b, size, i)
			if err != nil {
				t.Fatal(err)
			}
			copy(reopened[i*PayloadSize:], part)
			b[HeaderSize] ^= 1
			if _, err = Decode(b, size, i); err == nil {
				t.Fatal("accepted corrupt bitmap")
			}
		}
		if !Has(reopened, 0) || !Has(reopened, uint64(size/PageSize-1)) {
			t.Fatal("lost boundary bit")
		}
	}
	for _, size := range []int64{-PageSize, 0, 1, MaxSize + PageSize} {
		if ValidateSize(size) == nil {
			t.Fatalf("accepted invalid capacity %d", size)
		}
	}
	b := Encode([]byte{1}, PageSize, 0)
	b[HeaderSize] = 3
	binary.LittleEndian.PutUint32(b[PageSize-4:], crc32.ChecksumIEEE(b[:PageSize-4]))
	if _, err := Decode(b, PageSize, 0); err == nil {
		t.Fatal("accepted out-of-capacity presence bit with valid checksum")
	}
	if Bytes(MaxSize) != 1<<20 || Pages(MaxSize) != 259 {
		t.Fatal("maximum bitmap layout changed")
	}
}
