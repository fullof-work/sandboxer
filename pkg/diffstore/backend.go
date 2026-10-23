package diffstore

import (
	"context"
	"io"
	"math/bits"
)

// pageWrite borrows a complete plaintext page. Fresh describes logical upper
// presence before this accepted write, for backend-specific failure cleanup.
type pageWrite struct {
	Block uint64
	Data  []byte
	Fresh bool
}
type batchPolicy struct {
	disjointWrites bool
	materialPages  int
}

// diffBackend owns physical storage only. It never owns/cache-mutates borrowed
// pages. A write returns only after all I/O/commit work has stopped using them.
// Failure cleanup runs after the cache publishes its fatal state and before it
// releases frozen pages or dirty charges. Sync is distinct from guest FLUSH.
type diffBackend interface {
	io.ReaderAt
	io.WriterAt
	Size() int64
	name() string
	policy() batchPolicy
	withDirectRead(int, int64, func([]byte) error) error
	writePages(context.Context, []pageWrite) error
	cleanupWriteFailure([]pageWrite) error
	scan(context.Context, func(uint64) error) error
	scanDirtyBlocks() ([]uint64, error)
	presence(context.Context, uint64, []bool) error
	punchHole(int64, int64) error
	markModified(context.Context) error
	err() error
	Sync() error
	Close() error
}

func (d *diffFile) Size() int64                            { return d.logicalSize }
func (d *diffFile) policy() batchPolicy                    { return batchPolicy{materialPages: 1} }
func (d *diffFile) markModified(ctx context.Context) error { return ctx.Err() }
func (d *diffFile) err() error                             { return nil }
func (d *diffFile) scan(ctx context.Context, visit func(uint64) error) error {
	bitmap, err := d.scanDirtyBlocks()
	if err != nil {
		return err
	}
	for i, word := range bitmap {
		if err := ctx.Err(); err != nil {
			return err
		}
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			if err := visit(uint64(i*64 + bit)); err != nil {
				return err
			}
			word &= word - 1
		}
	}
	return nil
}
func (d *diffFile) presence(ctx context.Context, start uint64, dst []bool) error {
	bitmap, err := d.scanDirtyBlocks()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for i := range dst {
		block := start + uint64(i)
		dst[i] = block < uint64(d.logicalSize/cowBlockSize) && bitmap[block/64]&(1<<(block%64)) != 0
	}
	return nil
}
func (d *checkpointDiff) Size() int64  { return d.logicalSize }
func (d *checkpointDiff) name() string { return d.path }
func (d *checkpointDiff) policy() batchPolicy {
	return batchPolicy{disjointWrites: true, materialPages: maxWritebackPages}
}
func (d *checkpointDiff) cleanupWriteFailure([]pageWrite) error { return nil }
func (d *checkpointDiff) punchHole(off, n int64) error {
	return d.store.Discard(context.Background(), off, n)
}
func (d *checkpointDiff) scanDirtyBlocks() ([]uint64, error) {
	bitmap := make([]uint64, (d.logicalSize/cowBlockSize+63)/64)
	err := d.scan(context.Background(), func(block uint64) error { bitmap[block/64] |= 1 << (block % 64); return nil })
	return bitmap, err
}

var _ diffBackend = (*diffFile)(nil)
var _ diffBackend = (*checkpointDiff)(nil)
