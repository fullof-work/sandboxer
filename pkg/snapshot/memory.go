package snapshot

import (
	"context"
	"fmt"
	"io"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/memory"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
)

// memorySource keeps a fixed three-state map, delegating only Data reads. Its
// metadata functions do not read payload or allocate an extent per page.
type memorySource struct {
	size    uint64
	resolve func(uint64, uint64) (sparse.Run, error)
}

func (s *memorySource) Size() uint64 { return s.size }
func (s *memorySource) RunAt(off, limit uint64) (sparse.Run, error) {
	if off >= s.size {
		return nil, io.EOF
	}
	if limit == 0 {
		return nil, fmt.Errorf("memory: zero run limit")
	}
	return s.resolve(off, min(limit, s.size-off))
}
func (s *memorySource) ReadAt(ctx context.Context, b []byte, off uint64) (int, error) {
	if off >= s.size {
		return 0, io.EOF
	}
	want := min(uint64(len(b)), s.size-off)
	n := uint64(0)
	for n < want {
		if err := ctx.Err(); err != nil {
			return int(n), err
		}
		run, err := s.RunAt(off+n, want-n)
		if err != nil {
			return int(n), err
		}
		if err = validMemoryRun(run, off+n, off+want); err != nil {
			return int(n), err
		}
		count := run.End() - run.Offset()
		m, err := run.ReadAt(ctx, b[n:n+count], 0)
		n += uint64(m)
		if err != nil {
			return int(n), err
		}
		if uint64(m) != count {
			return int(n), io.ErrUnexpectedEOF
		}
	}
	if want < uint64(len(b)) {
		return int(want), io.EOF
	}
	return int(want), nil
}

type memoryRun struct {
	off, end uint64
	kind     sparse.RunKind
	read     func(context.Context, []byte, uint64) (int, error)
}

func (r memoryRun) Offset() uint64       { return r.off }
func (r memoryRun) End() uint64          { return r.end }
func (r memoryRun) Kind() sparse.RunKind { return r.kind }
func (r memoryRun) ReadAt(ctx context.Context, b []byte, inner uint64) (int, error) {
	if inner > r.end-r.off || uint64(len(b)) > r.end-r.off-inner {
		return 0, fmt.Errorf("memory: read outside run")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if r.kind != sparse.Data {
		clear(b)
		return len(b), nil
	}
	return r.read(ctx, b, r.off+inner)
}
func validMemoryRun(r sparse.Run, off, end uint64) error {
	if r == nil || r.Offset() != off || r.End() <= off || r.End() > end || r.Kind() > sparse.Data {
		return fmt.Errorf("memory: invalid sparse run")
	}
	return nil
}
func pageBit(bitmap []byte, page uint64) bool { return bitmap[page/8]&(1<<(page%8)) != 0 }

// CaptureMemory builds C or W from immutable M/D/R. D outside M does not imply
// Zero: only released holes override B. The optional bitmap labels W's clean
// copies; C is entirely dirty and needs no entry.
func CaptureMemory(reader io.ReaderAt, v *memory.CaptureView, workingSet bool) (sparse.Source, []byte, error) {
	if v == nil || v.Size == 0 || v.Size%4096 != 0 {
		return nil, nil, fmt.Errorf("memory: invalid capture size")
	}
	length := (v.Size/4096 + 7) / 8
	for _, b := range [][]byte{v.Materialized, v.Dirty, v.Released} {
		if uint64(len(b)) != length {
			return nil, nil, fmt.Errorf("memory: invalid capture bitmap length")
		}
		if rem := v.Size / 4096 % 8; rem != 0 && b[len(b)-1]>>rem != 0 {
			return nil, nil, fmt.Errorf("memory: invalid capture bitmap tail")
		}
	}
	kindAt := func(page uint64) sparse.RunKind {
		if pageBit(v.Materialized, page) {
			if workingSet || pageBit(v.Dirty, page) {
				return sparse.Data
			}
		} else if pageBit(v.Released, page) {
			return sparse.Zero
		}
		return sparse.Hole
	}
	read := func(ctx context.Context, b []byte, off uint64) (int, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		n, err := reader.ReadAt(b, int64(off))
		if n == len(b) && err == io.EOF {
			err = nil
		}
		return n, err
	}
	source := &memorySource{size: v.Size}
	source.resolve = func(off, limit uint64) (sparse.Run, error) {
		kind := kindAt(off / 4096)
		end := min(v.Size, off+limit)
		cursor := min(end, (off/4096+1)*4096)
		for cursor < end && kindAt(cursor/4096) == kind {
			cursor = min(end, cursor+4096)
		}
		return memoryRun{off: off, end: cursor, kind: kind, read: read}, nil
	}
	var dirty []byte
	if workingSet {
		bits := make([]byte, length)
		clean := false
		for i := range bits {
			bits[i] = v.Materialized[i]&v.Dirty[i] | v.Released[i]&^v.Materialized[i]
			clean = clean || v.Materialized[i]&^v.Dirty[i] != 0
		}
		if clean {
			var err error
			dirty, err = snapshotfile.EncodeDirty(v.Size, bits)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	return source, dirty, nil
}

// DirtyMemory selects dirty pages from a validated S; absent metadata selects
// exact legacy byte ranges. This view is for candidate selection, not restore.
func DirtyMemory(source sparse.Source, dirty []byte) (sparse.Source, error) {
	if dirty == nil {
		return source, nil
	}
	if err := snapshotfile.ValidateDirty(source, dirty); err != nil {
		return nil, err
	}
	bitmap := dirty[snapshotfile.DirtyHeaderSize:]
	selected := &memorySource{size: source.Size()}
	selected.resolve = func(off, limit uint64) (sparse.Run, error) {
		set := pageBit(bitmap, off/4096)
		end := off + limit
		cursor := min(end, (off/4096+1)*4096)
		for cursor < end && pageBit(bitmap, cursor/4096) == set {
			cursor = min(end, cursor+4096)
		}
		if !set {
			return memoryRun{off: off, end: cursor, kind: sparse.Hole}, nil
		}
		return source.RunAt(off, cursor-off)
	}
	return selected, nil
}

// SelectMemory uses the candidate union U only to select addresses. All final
// Data/Zero values come from the original complete layered view V.
func SelectMemory(values, candidates sparse.Source) (sparse.Source, error) {
	if values.Size() != candidates.Size() {
		return nil, fmt.Errorf("memory: candidate/value size mismatch")
	}
	out := &memorySource{size: values.Size()}
	out.resolve = func(off, limit uint64) (sparse.Run, error) {
		selected, err := candidates.RunAt(off, limit)
		if err != nil {
			return nil, err
		}
		if err = validMemoryRun(selected, off, off+limit); err != nil {
			return nil, err
		}
		if selected.Kind() == sparse.Hole {
			return memoryRun{off: off, end: selected.End(), kind: sparse.Hole}, nil
		}
		value, err := values.RunAt(off, selected.End()-off)
		if err != nil {
			return nil, err
		}
		if err = validMemoryRun(value, off, selected.End()); err != nil {
			return nil, err
		}
		if value.Kind() == sparse.Hole {
			return nil, fmt.Errorf("memory: candidate has no explicit value")
		}
		return value, nil
	}
	return out, nil
}
