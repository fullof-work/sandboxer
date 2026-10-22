package vhost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/internal/diffstore"
)

// diffBackend is the private seam between BlockCOW's cache/presence policy and
// the versioned physical store. Raw/v1 retain their existing implementation.
type diffBackend interface {
	io.ReaderAt
	io.WriterAt
	withRead(int, int64, func([]byte) error) error
	writePages(context.Context, []*cachePage) error
	scan(context.Context, func(uint64) error) error
	presence(context.Context, uint64, []bool) error
	discard(context.Context, int64, int64) error
	markModified(context.Context) error
	err() error
	Sync() error
	Close() error
}

type checkpointBlockCOWOption struct{ diffID, baseID string }

// WithCheckpoint selects the persistent v2 DIFF format and binds it to its
// stable disk identity and immutable base. Reopening never adopts a raw/v1 DIFF.
func WithCheckpoint(diffID, baseID string) BlockCOWOption {
	return checkpointBlockCOWOption{diffID, baseID}
}

func (o checkpointBlockCOWOption) applyBlockCOW(options *blockCOWOptions) error {
	if o.diffID == "" || options.checkpoint != nil {
		return fmt.Errorf("vhost: empty or duplicate checkpoint disk identity")
	}
	options.checkpoint = &o
	return nil
}

type checkpointDiff struct {
	store  *diffstore.Store
	readMu sync.Mutex
	read   []byte
}

func (d *checkpointDiff) ReadAt(p []byte, off int64) (int, error)  { return d.store.ReadAt(p, off) }
func (d *checkpointDiff) WriteAt(p []byte, off int64) (int, error) { return d.store.WriteAt(p, off) }
func (d *checkpointDiff) scan(ctx context.Context, visit func(uint64) error) error {
	return d.store.Scan(ctx, visit)
}
func (d *checkpointDiff) presence(ctx context.Context, start uint64, present []bool) error {
	return d.store.Presence(ctx, start, present)
}
func (d *checkpointDiff) discard(ctx context.Context, off, length int64) error {
	return d.store.Discard(ctx, off, length)
}
func (d *checkpointDiff) markModified(ctx context.Context) error { return d.store.MarkModified(ctx) }
func (d *checkpointDiff) Sync() error                            { return d.store.Sync() }
func (d *checkpointDiff) err() error                             { return d.store.Err() }
func (d *checkpointDiff) Close() error {
	d.readMu.Lock()
	defer d.readMu.Unlock()
	clear(d.read)
	d.read = nil
	return d.store.Close()
}

func (d *checkpointDiff) withRead(length int, offset int64, consume func([]byte) error) error {
	if length <= 0 || length > maxDiffScratchSize {
		return fmt.Errorf("vhost: invalid bounded checkpoint read")
	}
	d.readMu.Lock()
	defer d.readMu.Unlock()
	if d.read == nil {
		return os.ErrClosed
	}
	p := d.read[:length]
	defer clear(p)
	if err := readFullAt(d.store, p, offset); err != nil {
		return err
	}
	return consume(p)
}

func (d *checkpointDiff) writePages(ctx context.Context, pages []*cachePage) error {
	if len(pages) == 0 || len(pages) > maxWritebackPages {
		return fmt.Errorf("vhost: invalid checkpoint write batch")
	}
	batch := make([]diffstore.Page, len(pages))
	for i, p := range pages {
		batch[i] = diffstore.Page{Block: uint64(p.key.block), Data: p.data[:]}
	}
	// Store.WritePages includes the necessary mapping commit before success.
	// Frozen cache pages remain charged until this returns.
	return d.store.WritePages(ctx, batch)
}

func checkpointStoreOptions(options blockCOWOptions, size int64, path string) diffstore.Options {
	return diffstore.Options{DiffID: options.checkpoint.diffID, BaseID: options.checkpoint.baseID,
		LogicalSize: size, Key: options.checkpointKey, RequireEncryption: options.required, LockPath: path + ".lock"}
}

func wrapCheckpointDiff(store *diffstore.Store, path string) *diffFile {
	info := store.Info()
	return &diffFile{backend: &checkpointDiff{store: store, read: make([]byte, maxDiffScratchSize)},
		path: path, logicalSize: store.Size(), encrypted: info.Encrypted}
}

func openCheckpointDiff(path string, init DiffInit, options blockCOWOptions) (*diffFile, *freshDiffInit, error) {
	if path == "" {
		return nil, nil, fmt.Errorf("vhost: empty checkpoint diff path")
	}
	if init.Existing {
		store, err := diffstore.Open(path, checkpointStoreOptions(options, init.CreateSize, path))
		if err != nil {
			return nil, nil, err
		}
		info := store.Info()
		// Info resolves Current and records metadata failures in store health.
		// Lazy presence no longer runs Scan/healthy before cache attachment.
		if err := store.Err(); err != nil {
			return nil, nil, errors.Join(fmt.Errorf("vhost: inspect checkpoint diff: %w", err), store.Close())
		}
		if info.DiffID != options.checkpoint.diffID || info.BaseID != options.checkpoint.baseID {
			return nil, nil, errors.Join(fmt.Errorf("vhost: checkpoint identity/base mismatch"), store.Close())
		}
		return wrapCheckpointDiff(store, path), nil, nil
	}
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() || info.Size() != 0 {
			return nil, nil, os.ErrExist
		}
	} else if !os.IsNotExist(err) {
		return nil, nil, err
	}
	claim, claimPath, err := acquireFreshDiffClaim(path)
	if err != nil {
		return nil, nil, err
	}
	fresh := &freshDiffInit{finalPath: path, claim: claim, claimPath: claimPath}
	fail := func(err error) (*diffFile, *freshDiffInit, error) { return nil, nil, errors.Join(err, fresh.abort()) }
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() || info.Size() != 0 {
			return fail(os.ErrExist)
		}
	} else if !os.IsNotExist(err) {
		return fail(err)
	}
	size := init.CreateSize
	if init.TemplatePath != "" {
		template, err := openLazyDiffTemplate(init.TemplatePath, options.encryption)
		if err != nil {
			return fail(err)
		}
		fresh.template = template
		if template.Size() > uint64(^uint64(0)>>1) {
			return fail(fmt.Errorf("vhost: checkpoint template too large"))
		}
		size = int64(template.Size())
	}
	if err := validateDiffLogicalSize(size); err != nil {
		return fail(err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.partial")
	if err != nil {
		return fail(err)
	}
	fresh.tmpPath = tmp.Name()
	if err := tmp.Close(); err != nil {
		return fail(err)
	}
	if err := os.Remove(fresh.tmpPath); err != nil {
		return fail(err)
	}
	store, err := diffstore.Create(fresh.tmpPath, checkpointStoreOptions(options, size, path))
	if err != nil {
		return fail(err)
	}
	fresh.diff = wrapCheckpointDiff(store, path)
	if fresh.template != nil {
		return fresh.diff, fresh, nil
	}
	if err := fresh.diff.Sync(); err != nil {
		return fail(err)
	}
	if err := commitFreshDiff(fresh.tmpPath, path); err != nil {
		return fail(err)
	}
	fresh.committed = true
	if err := fresh.releaseClaim(); err != nil {
		return fail(err)
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return fail(err)
	}
	return fresh.diff, nil, nil
}

func (d *diffFile) name() string {
	if d.path != "" {
		return d.path
	}
	return d.f.Name()
}
func (d *diffFile) cleanupWriteFailure(pages []*cachePage) error {
	// v2 owns allocation and commit boundaries. In particular a short write
	// cannot be cleaned by punching a guest logical offset in a mixed file.
	if d.backend != nil {
		return nil
	}
	var err error
	for _, p := range pages {
		if p.fresh {
			err = errors.Join(err, d.punchHole(p.key.block*cowBlockSize, cowBlockSize))
		}
	}
	return err
}

func (c *BlockCOW) markModified(ctx context.Context) error {
	if c.diff.backend == nil {
		return nil
	}
	if err := c.diff.backend.markModified(ctx); err != nil {
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return err
		}
		c.cache.mu.Lock()
		report := c.cache.failLocked(fmt.Errorf("vhost: invalidate checkpoint continuation: %w", err))
		fatal := c.cache.err
		c.cache.mu.Unlock()
		if report != nil {
			report(fatal)
		}
		return fatal
	}
	return nil
}

type CheckpointID string

const ActiveDiff CheckpointID = ""

var ErrCheckpointUnsupported = errors.New("checkpoint_unsupported")

type ExportView interface {
	sparse.Source
	io.Closer
}

func (c *BlockCOW) checkpointBackend() (*checkpointDiff, error) {
	backend, ok := c.diff.backend.(*checkpointDiff)
	if !ok {
		return nil, ErrCheckpointUnsupported
	}
	return backend, nil
}

// CheckpointInfo returns the immutable identity used by the shared disk plan.
func (c *BlockCOW) CheckpointInfo() (diffID, baseID string, enabled bool) {
	d, err := c.checkpointBackend()
	if err != nil {
		return "", "", false
	}
	info := d.store.Info()
	return info.DiffID, info.BaseID, true
}

func (c *BlockCOW) CanResumeCheckpoint(id CheckpointID) bool {
	d, err := c.checkpointBackend()
	if err != nil {
		return false
	}
	_, end, err := c.begin(nil)
	if err != nil {
		return false
	}
	defer c.end(end)
	return d.store.CanResume(string(id))
}

// Checkpoint pauses frontend admission only after the template has reached
// Ready, so ongoing guest writes cannot starve entry to this boundary. The
// worker and materializer never take opMu, and can finish Drain/publication.
func (c *BlockCOW) Checkpoint(ctx context.Context, id CheckpointID) error {
	d, err := c.checkpointBackend()
	if err != nil {
		return err
	}
	if id == ActiveDiff {
		return fmt.Errorf("vhost: empty checkpoint ID")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.WaitMaterialReady(ctx); err != nil {
		return err
	}
	c.opMu.Lock()
	defer c.opMu.Unlock()
	if err := c.Err(); err != nil {
		return err
	}
	if err := c.cache.drain(ctx, c); err != nil {
		return err
	}
	if err := c.WaitMaterialized(ctx); err != nil {
		return err
	}
	return d.store.Checkpoint(ctx, string(id))
}

func (c *BlockCOW) ListCheckpoints(ctx context.Context) ([]CheckpointID, error) {
	d, err := c.checkpointBackend()
	if err != nil {
		return nil, err
	}
	ctx, end, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer c.end(end)
	ids, err := d.store.ListCheckpoints(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]CheckpointID, len(ids))
	for i, id := range ids {
		result[i] = CheckpointID(id)
	}
	return result, nil
}

func (c *BlockCOW) DeleteCheckpoint(ctx context.Context, id CheckpointID) error {
	return c.DeleteCheckpoints(ctx, []CheckpointID{id})
}

// DeleteCheckpoints revokes a complete selection before permitting the store's
// background collector to transfer ownership between the selected versions.
func (c *BlockCOW) DeleteCheckpoints(ctx context.Context, ids []CheckpointID) error {
	d, err := c.checkpointBackend()
	if err != nil {
		return err
	}
	selected := make([]string, len(ids))
	for i, id := range ids {
		if id == ActiveDiff {
			return fmt.Errorf("vhost: empty checkpoint ID")
		}
		selected[i] = string(id)
	}
	ctx, end, err := c.begin(ctx)
	if err != nil {
		return err
	}
	defer c.end(end)
	return d.store.DeleteCheckpoints(ctx, selected)
}

type activeExportView struct {
	source sparse.Source
	size   uint64
	mu     sync.Mutex
}

func (v *activeExportView) Size() uint64 { return v.size }
func (v *activeExportView) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.source = nil
	return nil
}
func (v *activeExportView) RunAt(offset, limit uint64) (sparse.Run, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.source == nil {
		return nil, os.ErrClosed
	}
	run, err := v.source.RunAt(offset, limit)
	if err != nil {
		return nil, err
	}
	return activeExportRun{source: v, offset: run.Offset(), end: run.End(), kind: run.Kind()}, nil
}
func (v *activeExportView) ReadAt(ctx context.Context, p []byte, offset uint64) (int, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.source == nil {
		return 0, os.ErrClosed
	}
	return v.source.ReadAt(ctx, p, offset)
}

type activeExportRun struct {
	source      *activeExportView
	offset, end uint64
	kind        sparse.RunKind
}

func (r activeExportRun) Offset() uint64       { return r.offset }
func (r activeExportRun) End() uint64          { return r.end }
func (r activeExportRun) Kind() sparse.RunKind { return r.kind }
func (r activeExportRun) ReadAt(ctx context.Context, p []byte, inner uint64) (int, error) {
	if inner > r.end-r.offset || uint64(len(p)) > r.end-r.offset-inner {
		return 0, fmt.Errorf("vhost: export run read out of bounds")
	}
	return r.source.ReadAt(ctx, p, r.offset+inner)
}

// Export(ActiveDiff) shares the private active view with SnapshotView; its
// caller keeps the backend quiescent. Named views own independent reader pins
// and remain usable after the active BlockCOW closes or resumes writing.
func (c *BlockCOW) Export(ctx context.Context, id CheckpointID) (ExportView, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if id == ActiveDiff {
		reader, holes, err := c.activeView(ctx)
		if err != nil {
			return nil, err
		}
		source, err := sparse.NewSource(reader, uint64(c.size), holes)
		if err != nil {
			return nil, err
		}
		return &activeExportView{source: source, size: uint64(c.size)}, nil
	}
	d, err := c.checkpointBackend()
	if err != nil {
		return nil, err
	}
	ctx, end, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer c.end(end)
	return d.store.Export(ctx, string(id))
}

// openCheckpointPreflight authenticates metadata and reads a quiescent active
// view without acquiring write ownership or replaying metadata in place.
func openCheckpointPreflight(ctx context.Context, path string, options blockCOWOptions) (diffTemplateSource, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	info, err := diffstore.Probe(path)
	if err != nil {
		return nil, err
	}
	if info.DiffID != options.checkpoint.diffID || info.BaseID != options.checkpoint.baseID {
		return nil, fmt.Errorf("vhost: checkpoint identity/base mismatch")
	}
	if options.required && !info.Encrypted {
		return nil, ErrDiffPlaintextForbidden
	}
	return diffstore.OpenExport(ctx, path, "", options.checkpointKey)
}

// Template writes use bounded metadata batches too. Read template bytes before
// taking stripes, then recheck presence under the same locks as frontend writes.
// The cache worker does not take stripes, so metadata completion can progress.
func (c *BlockCOW) materializeCheckpointRange(ctx context.Context, template diffTemplateSource, start, end uint64, staging []byte) error {
	backend, err := c.checkpointBackend()
	if err != nil {
		return err
	}
	for offset := start; offset < end; {
		if err := ctx.Err(); err != nil {
			return err
		}
		next := min(end, offset+uint64(len(staging)))
		length := int(next - offset)
		n, err := template.ReadAt(ctx, staging[:length], offset)
		if err != nil && !(errors.Is(err, io.EOF) && n == length) {
			return err
		}
		if n != length {
			return io.ErrUnexpectedEOF
		}
		first, last := int64(offset)/cowBlockSize, int64(next-1)/cowBlockSize
		// Numeric order prevents inversion with bounded frontend read ranges.
		var locked [cowLockStripes]bool
		for block := first; block <= last; block++ {
			locked[block%cowLockStripes] = true
		}
		for stripe := range locked {
			if locked[stripe] {
				c.blockMu[stripe].Lock()
			}
		}
		pages := make([]diffstore.Page, 0, length/cowBlockSize)
		for pos := offset; pos < next; pos += cowBlockSize {
			block := int64(pos) / cowBlockSize
			if !c.blockDirty(block) && !c.materialBlockResolved(block) {
				pages = append(pages, diffstore.Page{Block: uint64(block), Data: staging[pos-offset : pos-offset+cowBlockSize]})
			}
		}
		if len(pages) > 0 {
			err = backend.store.WritePages(ctx, pages)
		}
		if err == nil {
			for _, page := range pages {
				c.markDirty(int64(page.Block))
			}
		}
		for stripe := len(locked) - 1; stripe >= 0; stripe-- {
			if locked[stripe] {
				c.blockMu[stripe].Unlock()
			}
		}
		if err != nil {
			return err
		}
		offset = next
	}
	return nil
}
