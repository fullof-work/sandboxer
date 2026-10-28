package memory

import (
	"errors"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"golang.org/x/sys/unix"
)

// CaptureView is frozen after CH pause, backend quiescence, CH snapshot writes
// and final dirty collection. Later clean lazy fills cannot change its M/D/R.
// Bitmaps use the same memfd-relative 4 KiB page numbering.
type CaptureView struct {
	Size         uint64
	Materialized []byte
	Dirty        []byte
	Released     []byte
}

func WalkHoles(fd int, size int64) ([]sparse.Extent, error) {
	if fd < 0 || size <= 0 {
		return nil, fmt.Errorf("memory: invalid residency source")
	}
	var holes []sparse.Extent
	for off := int64(0); off < size; {
		data, err := unix.Seek(fd, off, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			holes = append(holes, sparse.Extent{Offset: uint64(off), Size: uint64(size - off)})
			return holes, nil
		}
		if err != nil {
			return nil, fmt.Errorf("SEEK_DATA at %d: %w", off, err)
		}
		if data < off {
			return nil, fmt.Errorf("memory: nonmonotone SEEK_DATA")
		}
		if data > size {
			data = size
		}
		if data > off {
			holes = append(holes, sparse.Extent{Offset: uint64(off), Size: uint64(data - off)})
		}
		if data == size {
			break
		}
		hole, err := unix.Seek(fd, data, unix.SEEK_HOLE)
		if err != nil {
			return nil, fmt.Errorf("SEEK_HOLE at %d: %w", data, err)
		}
		if hole <= data {
			return nil, fmt.Errorf("memory: nonmonotone SEEK_HOLE")
		}
		off = min(hole, size)
	}
	return holes, nil
}
