package diffstore

import (
	"context"
	"io"
)

// pageWrite borrows a complete plaintext page. Before starting I/O the single
// layer backend updates Fresh from effective payload existence for failure
// cleanup: logical presence alone cannot distinguish Data from Zero.
type pageWrite struct {
	Block uint64
	Data  []byte
	Fresh bool
}
type batchPolicy struct {
	materialPages int
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
	scanDirtyBlocks() ([]uint64, error)
	payloadPresence(context.Context, uint64, []bool) error
	discard(context.Context, int64, int64, bool) error
	markModified(context.Context) error
	err() error
	Sync() error
	Close() error
}

func (d *diffFile) Size() int64 { return d.logicalSize }
func (d *diffFile) policy() batchPolicy {
	// Payload runs remain separate physical writes, but one bounded batch can
	// publish each changed P page once. Otherwise random first writes rewrite
	// the same bitmap page for every singleton in the dirty queue.
	return batchPolicy{materialPages: 1}
}
func (d *diffFile) markModified(ctx context.Context) error { return ctx.Err() }
func (d *diffFile) err() error                             { return nil }
func (d *checkpointDiff) Size() int64                      { return d.logicalSize }
func (d *checkpointDiff) payloadPresence(ctx context.Context, start uint64, dst []bool) error {
	return d.store.PayloadPresence(ctx, start, dst)
}
func (d *diffFile) payloadPresence(ctx context.Context, start uint64, dst []bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if start > uint64(d.logicalSize/cowBlockSize) || uint64(len(dst)) > uint64(d.logicalSize/cowBlockSize)-start {
		return io.EOF
	}
	d.stateMu.RLock()
	defer d.stateMu.RUnlock()
	for i := range dst {
		dst[i] = bitmapBlockDirty(d.payload, int64(start)+int64(i))
	}
	return nil
}
func (d *checkpointDiff) name() string { return d.path }
func (d *checkpointDiff) policy() batchPolicy {
	return batchPolicy{materialPages: maxWritebackPages}
}
func (d *checkpointDiff) cleanupWriteFailure([]pageWrite) error { return nil }
func (d *checkpointDiff) discard(ctx context.Context, off, n int64, hole bool) error {
	return d.store.Discard(ctx, off, n, hole)
}
func (d *checkpointDiff) scanDirtyBlocks() ([]uint64, error) {
	bitmap := make([]uint64, (d.logicalSize/cowBlockSize+63)/64)
	p, err := d.store.PresenceBitmap(context.Background())
	if err != nil {
		return nil, err
	}
	for i, v := range p {
		bitmap[i/8] |= uint64(v) << (8 * (i % 8))
	}
	return bitmap, nil
}

var _ diffBackend = (*diffFile)(nil)
var _ diffBackend = (*checkpointDiff)(nil)
