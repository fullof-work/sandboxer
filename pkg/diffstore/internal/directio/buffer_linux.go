// Package directio provides bounded, shared mappings for direct I/O. Allocation
// policy and ownership (per-disk workspaces or a process pool) belong to callers.
package directio

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Buffer must not be copied. Bytes may be borrowed only while its owner excludes
// Close/reuse. MAP_SHARED is required for concurrent fork and O_DIRECT safety.
type Buffer struct{ Mapping, Bytes []byte }

func New(size, alignment, limit int) (*Buffer, error) {
	if alignment <= 0 || alignment > limit || size <= 0 || size > limit {
		return nil, fmt.Errorf("diffstore: invalid direct buffer size/alignment %d/%d", size, alignment)
	}
	reserve := size
	if os.Getpagesize()%alignment != 0 {
		reserve += alignment - 1
	}
	raw, err := unix.Mmap(-1, 0, reserve, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_ANON)
	if err != nil {
		return nil, fmt.Errorf("diffstore: mmap direct buffer: %w", err)
	}
	pad := (uintptr(alignment) - uintptr(unsafe.Pointer(&raw[0]))%uintptr(alignment)) % uintptr(alignment)
	return &Buffer{Mapping: raw, Bytes: raw[int(pad) : int(pad)+size]}, nil
}
func (b *Buffer) Close() error {
	if b == nil || b.Mapping == nil {
		return nil
	}
	clear(b.Mapping)
	err := unix.Munmap(b.Mapping)
	b.Mapping, b.Bytes = nil, nil
	return err
}
