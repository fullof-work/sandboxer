package snapshotfile

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

const (
	MemoryDirtyName = "memory.dirty"
	DirtyPageSize   = uint64(4096)
	DirtyHeaderSize = 16
	// The bitmap has its own limit, independently of snapshot.cfg. The whole
	// canonical ZIP (including every header) must also fit MaxTailBytes.
	MaxDirtyBytes = 64 << 20
	MaxTailBytes  = 64 << 20
)

// EncodeDirty encodes the exact bitset, including a present all-zero bitmap.
// A nil entry, rather than an all-zero entry, means all explicit bytes dirty.
func EncodeDirty(memorySize uint64, bitmap []byte) ([]byte, error) {
	if memorySize == 0 || memorySize%DirtyPageSize != 0 ||
		uint64(len(bitmap)) != (memorySize/DirtyPageSize+7)/8 || len(bitmap) > MaxDirtyBytes-DirtyHeaderSize {
		return nil, errors.New("memory.dirty: invalid memory size or bitmap length")
	}
	body := make([]byte, DirtyHeaderSize+len(bitmap))
	binary.LittleEndian.PutUint32(body, 1)
	binary.LittleEndian.PutUint32(body[4:], 12)
	binary.LittleEndian.PutUint64(body[8:], memorySize)
	copy(body[DirtyHeaderSize:], bitmap)
	if err := validateDirtyBits(body, memorySize); err != nil {
		return nil, err
	}
	return body, nil
}

// validateDirtyHeader is also used by ReadConfig, which deliberately does not
// fetch the bitmap body. The complete reader checks unused bits and coverage.
func validateDirtyHeader(header []byte, length, memorySize uint64) error {
	if len(header) < DirtyHeaderSize || length < DirtyHeaderSize || length > MaxDirtyBytes ||
		binary.LittleEndian.Uint32(header) != 1 || binary.LittleEndian.Uint32(header[4:]) != 12 ||
		memorySize == 0 || memorySize%DirtyPageSize != 0 || binary.LittleEndian.Uint64(header[8:]) != memorySize ||
		length != DirtyHeaderSize+(memorySize/DirtyPageSize+7)/8 {
		return errors.New("memory.dirty: invalid encoding, geometry or length")
	}
	return nil
}

func validateDirtyBits(body []byte, memorySize uint64) error {
	if err := validateDirtyHeader(body, uint64(len(body)), memorySize); err != nil {
		return err
	}
	if remainder := memorySize / DirtyPageSize % 8; remainder != 0 && body[len(body)-1]>>remainder != 0 {
		return errors.New("memory.dirty: nonzero unused bits")
	}
	return nil
}

// ValidateDirty validates a present entry against authoritative three-state
// memory metadata. Coverage can cross arbitrarily split Data and Zero runs.
// It never reads memory payload. Missing metadata preserves legacy geometry.
func ValidateDirty(memory sparse.Source, body []byte) error {
	if body == nil {
		return nil
	}
	if memory == nil {
		return errors.New("memory.dirty: missing memory")
	}
	if err := validateDirtyBits(body, memory.Size()); err != nil {
		return err
	}
	// Scan the sparse map once. A Hole may touch only part of a page; any set
	// bit for that page is invalid, even if the other bytes are explicit.
	for off := uint64(0); off < memory.Size(); {
		run, err := memory.RunAt(off, memory.Size()-off)
		if err != nil {
			return fmt.Errorf("memory.dirty coverage: %w", err)
		}
		if run == nil || run.Offset() != off || run.End() <= off || run.End() > memory.Size() || run.Kind() > sparse.Data {
			return errors.New("memory.dirty: invalid memory run")
		}
		if run.Kind() == sparse.Hole && bitmapAny(body[DirtyHeaderSize:], off/DirtyPageSize, (run.End()+DirtyPageSize-1)/DirtyPageSize) {
			return fmt.Errorf("memory.dirty: dirty page overlaps Hole [%d,%d)", off, run.End())
		}
		off = run.End()
	}
	return nil
}

func bitmapAny(bitmap []byte, first, end uint64) bool {
	for first < end && first%8 != 0 {
		if bitmap[first/8]&(1<<(first%8)) != 0 {
			return true
		}
		first++
	}
	for end-first >= 8 {
		if bitmap[first/8] != 0 {
			return true
		}
		first += 8
	}
	return first < end && bitmap[first/8]&byte((1<<(end-first))-1) != 0
}
