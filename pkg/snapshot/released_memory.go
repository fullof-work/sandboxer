package snapshot

import (
	"context"
	"fmt"
	"io"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// MemoryLayout samples physical residency and the existing UFFD page state
// together while CH and backends are paused. Holes inherit a parent; zeros must
// mask it. It neither consumes nor resets the running handler's state.
type MemoryLayout func() (holes, zeros []sparse.Extent, err error)

// releasedMemorySource uses pread, so layered readers can read arbitrary offsets
// concurrently. It borrows the live memfd, whose lifetime and VM freeze belong
// to Take's caller. It adds no snapshot metadata or dirty-page ledger.
type releasedMemorySource struct {
	fd           int
	size         uint64
	holes, zeros []sparse.Extent
}

func (s *releasedMemorySource) Size() uint64 { return s.size }
func (s *releasedMemorySource) Close() error { return nil }

func (s *releasedMemorySource) RunAt(off, limit uint64) (sparse.Run, error) {
	if off >= s.size {
		return nil, io.EOF
	}
	if limit == 0 {
		return nil, fmt.Errorf("memory: zero run limit")
	}
	hole, he := holeRun(int64(off), s.holes, int64(s.size))
	zero, ze := holeRun(int64(off), s.zeros, int64(s.size))
	end := min(off+min(limit, s.size-off), uint64(he), uint64(ze))
	kind := sparse.Data
	if hole {
		kind = sparse.Hole
	} else if zero {
		kind = sparse.Zero
	}
	return releasedMemoryRun{s, off, end, kind}, nil
}

func (s *releasedMemorySource) ReadAt(ctx context.Context, p []byte, off uint64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if off >= s.size {
		return 0, io.EOF
	}
	want := min(uint64(len(p)), s.size-off)
	n := 0
	for uint64(n) < want {
		r, err := s.RunAt(off+uint64(n), want-uint64(n))
		if err != nil {
			return n, err
		}
		end := n + int(r.End()-r.Offset())
		read, err := r.ReadAt(ctx, p[n:end], 0)
		n += read
		if err != nil {
			return n, err
		}
		if n != end {
			return n, io.ErrUnexpectedEOF
		}
	}
	if n != len(p) {
		return n, io.EOF
	}
	return n, nil
}

type releasedMemoryRun struct {
	source   *releasedMemorySource
	off, end uint64
	kind     sparse.RunKind
}

func (r releasedMemoryRun) Offset() uint64       { return r.off }
func (r releasedMemoryRun) End() uint64          { return r.end }
func (r releasedMemoryRun) Kind() sparse.RunKind { return r.kind }
func (r releasedMemoryRun) ReadAt(ctx context.Context, p []byte, off uint64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if off > r.end-r.off || uint64(len(p)) > r.end-r.off-off {
		return 0, fmt.Errorf("memory: read outside run")
	}
	if r.kind != sparse.Data {
		clear(p)
		return len(p), nil
	}
	return fdReaderAt(r.source.fd).ReadAt(p, int64(r.off+off))
}

func explicitMemoryBytes(source sparse.Source) (uint64, error) {
	var explicit uint64
	for off := uint64(0); off < source.Size(); {
		r, err := source.RunAt(off, source.Size()-off)
		if err != nil {
			return 0, err
		}
		if r == nil || r.Offset() != off || r.End() <= off || r.End() > source.Size() {
			return 0, fmt.Errorf("memory: invalid run at %d", off)
		}
		if r.Kind() != sparse.Hole {
			explicit += r.End() - off
		}
		off = r.End()
	}
	return explicit, nil
}
