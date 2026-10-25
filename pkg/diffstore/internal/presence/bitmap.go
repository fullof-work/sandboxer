// Package presence owns the bounded, page-aligned logical presence bitmap used
// by both writable DIFF formats. Payload locations remain backend-specific.
package presence

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	PageSize    = 4096
	MaxSize     = int64(32 << 30)
	HeaderSize  = 32
	PayloadSize = PageSize - HeaderSize - 4
)

var magic = [8]byte{'K', 'D', 'P', 'R', 'E', 'S', '1', '\n'}
var le = binary.LittleEndian

func ValidateSize(size int64) error {
	if size <= 0 || size%PageSize != 0 || size > MaxSize {
		return fmt.Errorf("diffstore: writable capacity must be a positive 4 KiB multiple no larger than 32 GiB")
	}
	return nil
}

// Bytes and Pages require a validated capacity.
func Bytes(size int64) int                 { return int((size/PageSize + 7) / 8) }
func Pages(size int64) int                 { return (Bytes(size) + PayloadSize - 1) / PayloadSize }
func Has(bitmap []byte, block uint64) bool { return bitmap[block/8]&(1<<(block%8)) != 0 }
func Set(bitmap []byte, block uint64) {
	bitmap[block/8] |= byte(1 << (block % 8))
}

// End returns the exclusive end of the logical-presence run starting at block.
// It does not describe whether the run has physical payload.
func End(bitmap []byte, block, limit uint64) uint64 {
	present := Has(bitmap, block)
	fill := byte(0)
	if present {
		fill = 255
	}
	block++
	for block < limit {
		if block%8 == 0 && limit-block >= 8 && bitmap[block/8] == fill {
			block += 8
			continue
		}
		if Has(bitmap, block) != present {
			break
		}
		block++
	}
	return block
}

// Encode copies one bounded bitmap page. The full logical length, ordinal and
// page count are repeated so a page cannot silently move within a bitmap.
func Encode(bitmap []byte, size int64, index int) []byte {
	if len(bitmap) != Bytes(size) || index < 0 || index >= Pages(size) {
		panic("diffstore: invalid presence page")
	}
	b := make([]byte, PageSize)
	copy(b, magic[:])
	le.PutUint32(b[8:], uint32(index))
	le.PutUint32(b[12:], uint32(Pages(size)))
	le.PutUint64(b[16:], uint64(size))
	start := index * PayloadSize
	copy(b[HeaderSize:PageSize-4], bitmap[start:min(start+PayloadSize, len(bitmap))])
	le.PutUint32(b[PageSize-4:], crc32.ChecksumIEEE(b[:PageSize-4]))
	return b
}

// Decode validates before returning the useful bytes. The caller copies them
// into its complete bitmap; no partially validated bitmap may be published.
func Decode(b []byte, size int64, index int) ([]byte, error) {
	if err := ValidateSize(size); err != nil {
		return nil, err
	}
	if len(b) != PageSize || index < 0 || index >= Pages(size) ||
		!bytes.Equal(b[:8], magic[:]) || le.Uint32(b[8:]) != uint32(index) ||
		le.Uint32(b[12:]) != uint32(Pages(size)) || le.Uint64(b[16:]) != uint64(size) ||
		le.Uint64(b[24:]) != 0 || le.Uint32(b[PageSize-4:]) != crc32.ChecksumIEEE(b[:PageSize-4]) {
		return nil, fmt.Errorf("diffstore: invalid presence bitmap page")
	}
	n := min(PayloadSize, Bytes(size)-index*PayloadSize)
	for _, v := range b[HeaderSize+n : PageSize-4] {
		if v != 0 {
			return nil, fmt.Errorf("diffstore: nonzero presence bitmap padding")
		}
	}
	p := b[HeaderSize : HeaderSize+n]
	if index == Pages(size)-1 && size/PageSize%8 != 0 && p[n-1]>>uint(size/PageSize%8) != 0 {
		return nil, fmt.Errorf("diffstore: presence bitmap extends beyond logical capacity")
	}
	return p, nil
}
