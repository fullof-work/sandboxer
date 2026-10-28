package snapshot

import (
	"context"
	"errors"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
)

// Match tarstream's charged index representation, with an explicit bound on
// planning memory. The planner below only walks metadata and allocates no
// extent slice; the writer repeats its own authoritative validation.
const memoryIndexBudget = uint64(256 << 20)
const memoryExtentBudget = uint64(1 << 20) // includes the GNU trailing sentinel
var errMemoryBudget = errors.New("snapshot memory carrier budget")

type MemoryStorage struct {
	DataBytes      uint64
	BitmapBytes    uint64
	ZIPBytes       uint64
	ExtraDataBytes uint64
	Fallback       string
}

func memoryDataBytes(ctx context.Context, source sparse.Source) (uint64, error) {
	var data uint64
	for off := uint64(0); off < source.Size(); {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		r, err := source.RunAt(off, source.Size()-off)
		if err != nil {
			return 0, err
		}
		if err = validMemoryRun(r, off, source.Size()); err != nil {
			return 0, err
		}
		if r.Kind() == sparse.Data {
			data += r.End() - off
		}
		off = r.End()
	}
	return data, nil
}

func checkMemoryCarrier(ctx context.Context, source sparse.Source, extentLimit, indexLimit uint64) error {
	provider, ok := source.(tarstream.IdentityProvider)
	if !ok {
		return fmt.Errorf("snapshot memory lacks payload commitment")
	}
	payload, _, _ := provider.PayloadCommitment()
	if payload > 1<<40 {
		return fmt.Errorf("snapshot tarstream memory exceeds 1 TiB")
	}
	if payload > source.Size() {
		return fmt.Errorf("snapshot invalid payload boundary")
	}
	charge := uint64(2<<20) + uint64(len("snapshot"))*8 + (payload/4096+7)/8 + 4096 + source.Size() - payload
	var count, last uint64
	appendExtent := func(start, end uint64) error {
		if end <= start {
			return nil
		}
		if count != 0 && last == start {
			last = end
			return nil
		}
		count++
		last = end
		if count+1 > extentLimit {
			return fmt.Errorf("%w: extent limit including sentinel", errMemoryBudget)
		}
		charge += 512
		if charge > indexLimit {
			return fmt.Errorf("%w: index memory limit", errMemoryBudget)
		}
		return nil
	}
	if charge > indexLimit {
		return fmt.Errorf("%w: metadata index memory limit", errMemoryBudget)
	}
	for off := uint64(0); off < source.Size(); {
		if err := ctx.Err(); err != nil {
			return err
		}
		r, err := source.RunAt(off, source.Size()-off)
		if err != nil {
			return err
		}
		if err = validMemoryRun(r, off, source.Size()); err != nil {
			return err
		}
		end := r.End()
		switch r.Kind() {
		case sparse.Hole:
			if off >= payload || end > payload || off%4096 != 0 || (end != payload && end%4096 != 0) {
				return fmt.Errorf("snapshot invalid tarstream hole geometry")
			}
		case sparse.Data:
			err = appendExtent(off, end)
		case sparse.Zero:
			start := (off + 4095) / 4096 * 4096
			stop := min(end, payload) / 4096 * 4096
			if start < stop {
				err = appendExtent(off, start)
				if err == nil {
					err = appendExtent(stop, end)
				}
			} else {
				err = appendExtent(off, end)
			}
		}
		if err != nil {
			return err
		}
		off = end
	}
	return nil
}

// MemoryFallback retains exactly the untrimmed artifact's binary coverage P.
// Values always come from F, including zero bytes. Outside P only required
// logical Zero survives. Neither input may contain external-owned fill.
func MemoryFallback(required, coverage, values sparse.Source) (sparse.Source, error) {
	if required == nil || coverage == nil || values == nil || required.Size() != coverage.Size() || required.Size() != values.Size() {
		return nil, fmt.Errorf("memory fallback size mismatch")
	}
	out := &memorySource{size: required.Size()}
	out.resolve = func(off, limit uint64) (sparse.Run, error) {
		cover, err := coverage.RunAt(off, limit)
		if err != nil {
			return nil, err
		}
		if err = validMemoryRun(cover, off, off+limit); err != nil {
			return nil, err
		}
		if cover.Kind() != sparse.Hole {
			value, err := values.RunAt(off, cover.End()-off)
			if err != nil {
				return nil, err
			}
			if err = validMemoryRun(value, off, cover.End()); err != nil {
				return nil, err
			}
			if value.Kind() == sparse.Hole {
				return nil, fmt.Errorf("memory fallback coverage has no value")
			}
			return memoryRun{off: off, end: value.End(), kind: sparse.Data, read: values.ReadAt}, nil
		}
		r, err := required.RunAt(off, cover.End()-off)
		if err != nil {
			return nil, err
		}
		if err = validMemoryRun(r, off, cover.End()); err != nil {
			return nil, err
		}
		if r.Kind() == sparse.Data {
			return nil, fmt.Errorf("memory fallback Data exceeds permitted coverage")
		}
		return r, nil
	}
	return out, nil
}

// DataCoverage selects M from W without accidentally including R outside M.
func DataCoverage(source sparse.Source) sparse.Source {
	out := &memorySource{size: source.Size()}
	out.resolve = func(off, limit uint64) (sparse.Run, error) {
		r, err := source.RunAt(off, limit)
		if err != nil {
			return nil, err
		}
		if err = validMemoryRun(r, off, off+limit); err != nil {
			return nil, err
		}
		if r.Kind() != sparse.Data {
			return memoryRun{off: off, end: r.End(), kind: sparse.Hole}, nil
		}
		return r, nil
	}
	return out
}

// BuildCaptureSource is exclusively for new capture artifacts. Equivalent
// publication continues using BuildSourceWithDirty and never drops metadata.
func BuildCaptureSource(ctx context.Context, sink ArtifactSink, mem sparse.Source, dirty, config, state, cfg []byte, coverage, values sparse.Source, logf func(string, ...any)) (sparse.Source, MemoryStorage, error) {
	var stats MemoryStorage
	original, err := memoryDataBytes(ctx, mem)
	if err != nil {
		return nil, stats, err
	}
	source, err := snapshotfile.BuildSourceWithDirty(mem, config, state, cfg, dirty)
	if dirty != nil && errors.Is(err, snapshotfile.ErrTailBudget) {
		source, err = snapshotfile.BuildSource(mem, config, state, cfg)
		if err == nil {
			stats.Fallback = "bitmap exceeds ZIP tail budget; all explicit pages dirty"
			dirty = nil
		}
	}
	if err != nil {
		return nil, stats, err
	}
	if _, file := sink.(*FileSink); file {
		err = checkMemoryCarrier(ctx, source, memoryExtentBudget, memoryIndexBudget)
		if errors.Is(err, errMemoryBudget) {
			reason := err.Error()
			mem, err = MemoryFallback(mem, coverage, values)
			if err != nil {
				return nil, stats, err
			}
			dirty = nil
			source, err = snapshotfile.BuildSource(mem, config, state, cfg)
			if err != nil {
				return nil, stats, err
			}
			if err = checkMemoryCarrier(ctx, source, memoryExtentBudget, memoryIndexBudget); err != nil {
				return nil, stats, fmt.Errorf("memory fallback still exceeds carrier budget: %w", err)
			}
			if stats.Fallback != "" {
				stats.Fallback += "; "
			}
			stats.Fallback += reason
		}
		if err != nil {
			return nil, stats, err
		}
	}
	stats.DataBytes, err = memoryDataBytes(ctx, mem)
	if err != nil {
		return nil, stats, err
	}
	stats.BitmapBytes = uint64(len(dirty))
	stats.ZIPBytes = source.Size() - mem.Size()
	if stats.DataBytes < original {
		return nil, stats, fmt.Errorf("memory fallback lost Data")
	}
	stats.ExtraDataBytes = stats.DataBytes - original
	if logf != nil {
		logf("snapshot memory: data=%d bitmap=%d zip=%d extra_data=%d fallback=%q", stats.DataBytes, stats.BitmapBytes, stats.ZIPBytes, stats.ExtraDataBytes, stats.Fallback)
	}
	return source, stats, nil
}
