package memory

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
)

// Dirty tracks changes since a fixed runtime baseline. It is shared by Go
// backend, CH collector and UFFD, and deliberately has no reset operation.
// All addresses are byte offsets in the one RAM memfd, never GPA or host VA.
type Dirty struct {
	size  uint64
	words []atomic.Uint64
}

const DirtyPageSize = uint64(4096)

func NewDirty(size uint64) (*Dirty, error) {
	if size == 0 || size%DirtyPageSize != 0 || size > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("dirty: invalid RAM size %d", size)
	}
	return &Dirty{size: size, words: make([]atomic.Uint64, (size/DirtyPageSize+63)/64)}, nil
}
func (d *Dirty) Size() uint64 { return d.size }

// Contains reports whether the page containing offset has ever been marked.
func (d *Dirty) Contains(offset uint64) bool {
	if offset >= d.size {
		return false
	}
	page := offset / DirtyPageSize
	return d.words[page/64].Load()&(uint64(1)<<(page%64)) != 0
}

// Mark records the entire writable range before a caller exposes it. Partial
// writes and failures remain conservatively represented. Unaligned byte
// ranges expand only inside this runtime page tracker, never old S geometry.
func (d *Dirty) Mark(offset, length uint64) error {
	if offset > d.size || length > d.size-offset {
		return fmt.Errorf("dirty: range [0x%x,+0x%x) exceeds RAM 0x%x", offset, length, d.size)
	}
	if length == 0 {
		return nil
	}
	first, end := offset/DirtyPageSize, (offset+length+DirtyPageSize-1)/DirtyPageSize
	for first < end {
		word := first / 64
		last := min(end, (word+1)*64)
		mask := (^uint64(0) << (first % 64)) & (^uint64(0) >> (64 - (last - word*64)))
		d.words[word].Or(mask)
		first = last
	}
	return nil
}

// MergeWords consumes a region-relative CH bitmap. Mapping validation belongs
// to the API decoder; this repeats bounds/alignment checks before updating D.
func (d *Dirty) MergeWords(offset, size uint64, words []uint64) error {
	if offset%DirtyPageSize != 0 || size == 0 || size%DirtyPageSize != 0 || offset > d.size || size > d.size-offset || uint64(len(words)) != (size/DirtyPageSize+63)/64 {
		return fmt.Errorf("dirty: invalid bitmap range/length")
	}
	if rem := size / DirtyPageSize % 64; rem != 0 && words[len(words)-1]>>rem != 0 {
		return fmt.Errorf("dirty: nonzero unused bits")
	}
	first := offset / DirtyPageSize
	for i, word := range words {
		idx := first/64 + uint64(i)
		shift := first % 64
		d.words[idx].Or(word << shift)
		if shift != 0 && idx+1 < uint64(len(d.words)) {
			d.words[idx+1].Or(word >> (64 - shift))
		}
	}
	return nil
}

// Snapshot copies the monotone set. Callers establish the pause/quiesce and
// installation barriers before asking for a capture-consistent view.
func (d *Dirty) Snapshot() []byte {
	out := make([]byte, (d.size/DirtyPageSize+7)/8)
	for i := range d.words {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], d.words[i].Load())
		copy(out[i*8:], b[:])
	}
	return out
}
