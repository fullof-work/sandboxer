package snapshot

import (
	"context"
	"fmt"
	"io"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/internal/readretry"
)

// payloadMergeBase keeps the existing artifact retry policy while exposing the
// parent's original run kinds and bounding it to payload bytes, without a second
// extent index or a byte-only adapter that would expand logical Zero to Data.
type payloadMergeBase struct {
	fetch.Stream
	size uint64
	ctx  context.Context
}

func (s *payloadMergeBase) Size() uint64 { return s.size }
func (s *payloadMergeBase) RunAt(off, limit uint64) (sparse.Run, error) {
	if off >= s.size {
		return nil, io.EOF
	}
	if limit == 0 {
		return nil, fmt.Errorf("merge base: zero run limit")
	}
	limit = min(limit, s.size-off)
	var run sparse.Run
	err := readretry.Do(s.ctx, func() error {
		var err error
		run, err = s.Stream.RunAt(off, limit)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return readerr.Mark(err, false)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	if run == nil || run.Offset() != off || run.End() <= off || run.End() > off+limit {
		return nil, readerr.Mark(fmt.Errorf("merge base: invalid run"), false)
	}
	return payloadMergeRun{run}, nil
}
func (s *payloadMergeBase) ReadAt(ctx context.Context, buf []byte, off uint64) (int, error) {
	if off >= s.size {
		return 0, io.EOF
	}
	n := min(uint64(len(buf)), s.size-off)
	read, err := readretry.ReadAt(ctx, int(n), func() (int, error) { return s.Stream.ReadAt(ctx, buf[:n], off) })
	if err == nil && read < len(buf) {
		err = io.EOF
	}
	return read, err
}

type payloadMergeRun struct{ sparse.Run }

func (r payloadMergeRun) ReadAt(ctx context.Context, buf []byte, off uint64) (int, error) {
	return readretry.ReadAt(ctx, len(buf), func() (int, error) { return r.Run.ReadAt(ctx, buf, off) })
}
