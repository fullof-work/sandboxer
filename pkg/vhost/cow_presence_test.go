package vhost

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type presenceHookBackend struct {
	diffBackend
	hook         func(context.Context, uint64, []bool) error
	scanHook     func(context.Context, func(uint64) error) error
	modifiedHook func()
}

func (b *presenceHookBackend) presence(ctx context.Context, start uint64, present []bool) error {
	return b.hook(ctx, start, present)
}

func (b *presenceHookBackend) scan(ctx context.Context, visit func(uint64) error) error {
	if b.scanHook != nil {
		return b.scanHook(ctx, visit)
	}
	return b.diffBackend.scan(ctx, visit)
}

func (b *presenceHookBackend) markModified(ctx context.Context) error {
	err := b.diffBackend.markModified(ctx)
	if b.modifiedHook != nil {
		b.modifiedHook()
	}
	return err
}

func reopenPresenceCOW(t *testing.T, encrypted bool, extra ...BlockCOWOption) (*BlockCOW, []byte) {
	t.Helper()
	const size = 520 * cowBlockSize
	base := &fakeReader{data: bytes.Repeat([]byte{0x61}, size)}
	path := filepath.Join(t.TempDir(), "active.diff")
	cow := checkpointCOW(t, path, base, size, encrypted)
	want := append([]byte(nil), base.data...)
	for _, block := range []int{0, 2, 255, 256, 519} {
		page := bytes.Repeat([]byte{byte(block)}, cowBlockSize)
		if _, err := cow.WriteAt(page, int64(block*cowBlockSize)); err != nil {
			t.Fatal(err)
		}
		copy(want[block*cowBlockSize:], page)
	}
	if err := cow.Checkpoint(context.Background(), "seed"); err != nil {
		t.Fatal(err)
	}
	if err := cow.Close(); err != nil {
		t.Fatal(err)
	}
	opts := []BlockCOWOption{WithCheckpoint("disk-under-test", "immutable-base")}
	if encrypted {
		opts = append(opts, WithDiffEncryption([32]byte{1, 2, 3}, true))
	}
	opts = append(opts, extra...)
	cow, err := OpenBlockCOW(path, base, DiffInit{Existing: true, CreateSize: size}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cow.Close() })
	return cow, want
}

func TestCheckpointLazyPresenceReadModifyExport(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			cow, want := reopenPresenceCOW(t, encrypted)
			if stats := cow.BackendStats(); stats["diff_presence_known_blocks"] != 0 || stats["diff_presence_complete"] != false {
				t.Fatal("eager presence", stats)
			}
			// Registering another retained root must not scan the upper bitmap.
			if err := cow.Checkpoint(context.Background(), "unscanned"); err != nil {
				t.Fatal(err)
			}
			if cow.BackendStats()["diff_presence_known_blocks"] != 0 {
				t.Fatal("checkpoint resolved unrelated presence")
			}
			// Partial writes to unknown allocated-zero, upper and base blocks.
			for _, block := range []int{0, 1, 2} {
				off := block*cowBlockSize + 37
				if _, err := cow.WriteAt([]byte{9, 8, 7}, int64(off)); err != nil {
					t.Fatal(err)
				}
				copy(want[off:], []byte{9, 8, 7})
			}
			// Discard an unknown upper block; later resolution must keep it absent.
			if err := cow.Discard(255*cowBlockSize, cowBlockSize); err != nil {
				t.Fatal(err)
			}
			copy(want[255*cowBlockSize:256*cowBlockSize], bytes.Repeat([]byte{0x61}, cowBlockSize))
			// Snapshot must discover the untouched tail and preserve exact holes.
			view, err := cow.Export(context.Background(), ActiveDiff)
			if err != nil {
				t.Fatal(err)
			}
			defer view.Close()
			upper := readExport(t, view)
			for block := 0; block < 520; block++ {
				present := block == 0 || block == 1 || block == 2 || block == 256 || block == 519
				page := upper[block*cowBlockSize : (block+1)*cowBlockSize]
				if present {
					if !bytes.Equal(page, want[block*cowBlockSize:(block+1)*cowBlockSize]) {
						t.Fatalf("export block %d", block)
					}
				} else if !bytes.Equal(page, make([]byte, cowBlockSize)) {
					t.Fatalf("export hole %d contains base", block)
				}
			}
			if stats := cow.BackendStats(); stats["diff_presence_complete"] != true || stats["diff_dirty_blocks"] != 5 {
				t.Fatal(stats)
			}
			got := make([]byte, len(want))
			if _, err := cow.ReadAt(got, 0); err != nil || !bytes.Equal(got, want) {
				t.Fatal("live view differs", err)
			}
		})
	}
}

func TestCheckpointLazyPresenceAcceptedWriteWins(t *testing.T) {
	gate := make(chan struct{})
	var release sync.Once
	cache, err := newCOWCache(8*cowBlockSize, 4*cowBlockSize, cacheHooks{beforeWrite: func([]*cachePage) error { <-gate; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	cow, want := reopenPresenceCOW(t, false, WithCOWCache(cache))
	defer cow.Close()
	defer release.Do(func() { close(gate) })
	backend := cow.diff.backend
	var calls atomic.Int64
	cow.diff.backend = &presenceHookBackend{diffBackend: backend, hook: func(ctx context.Context, start uint64, present []bool) error {
		calls.Add(1)
		return backend.presence(ctx, start, present)
	}}
	page := bytes.Repeat([]byte{0xc7}, cowBlockSize)
	if _, err := cow.WriteAt(page, cowBlockSize); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("full overwrite queried old presence")
	}
	copy(want[cowBlockSize:], page)
	// The query includes a known cached write between two unknown blocks. The
	// persisted index still says hole, but must not erase accepted presence.
	got := make([]byte, 3*cowBlockSize)
	if _, err := cow.ReadAt(got, 0); err != nil || !bytes.Equal(got, want[:len(got)]) {
		t.Fatal("resolved index hid accepted cache write", err)
	}
	view, _, err := cow.SnapshotView()
	if err != nil {
		t.Fatal(err)
	}
	upper, err := io.ReadAll(view)
	if err != nil || !bytes.Equal(upper[cowBlockSize:2*cowBlockSize], page) || !bytes.Equal(upper[519*cowBlockSize:], want[519*cowBlockSize:]) {
		t.Fatal("full scan hid accepted write or untouched tail", err)
	}
}

func TestCheckpointLazyPresenceSerializesDiscard(t *testing.T) {
	cow, _ := reopenPresenceCOW(t, false)
	backend := cow.diff.backend
	queried, release := make(chan struct{}), make(chan struct{})
	modified := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	cow.diff.backend = &presenceHookBackend{diffBackend: backend, modifiedHook: func() { close(modified) }, hook: func(ctx context.Context, start uint64, present []bool) error {
		err := backend.presence(ctx, start, present)
		close(queried)
		<-release
		return err
	}}
	read := make(chan error, 1)
	go func() { _, err := cow.ReadAt(make([]byte, cowBlockSize), 2*cowBlockSize); read <- err }()
	select {
	case <-queried:
	case <-time.After(5 * time.Second):
		t.Fatal("query blocked")
	}
	if cow.blockLock(2).TryLock() {
		cow.blockLock(2).Unlock()
		t.Fatal("presence publication is not protected by the block stripe")
	}
	discard := make(chan error, 1)
	go func() { discard <- cow.Discard(2*cowBlockSize, cowBlockSize) }()
	select {
	case <-modified:
	case <-time.After(5 * time.Second):
		t.Fatal("discard did not enter mutation path")
	}
	select {
	case err := <-discard:
		t.Fatalf("discard crossed unresolved read: %v", err)
	default:
	}
	once.Do(func() { close(release) })
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	if err := <-discard; err != nil {
		t.Fatal(err)
	}
	got := make([]byte, cowBlockSize)
	if _, err := cow.ReadAt(got, 2*cowBlockSize); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{0x61}, cowBlockSize)) {
		t.Fatal("discard resurrected stale upper", err)
	}
}

func TestCheckpointLazyPresenceCanceledExportPublishesNothing(t *testing.T) {
	cow, _ := reopenPresenceCOW(t, false)
	backend := cow.diff.backend
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cow.diff.backend = &presenceHookBackend{diffBackend: backend, scanHook: func(ctx context.Context, visit func(uint64) error) error {
		return backend.scan(ctx, func(block uint64) error {
			if err := visit(block); err != nil {
				return err
			}
			cancel()
			return ctx.Err()
		})
	}}
	if view, holes, err := cow.activeView(ctx); view != nil || holes != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled export returned a partial view", view, holes, err)
	}
	if cow.Err() != nil || cow.BackendStats()["diff_presence_known_blocks"] != 0 || cow.DirtyCount() != 0 {
		t.Fatal("partial scan was published or poisoned owner", cow.Err(), cow.BackendStats())
	}
	cow.diff.backend = backend
	if _, _, err := cow.SnapshotView(); err != nil {
		t.Fatal(err)
	}
	if cow.DirtyCount() != 5 || cow.BackendStats()["diff_presence_complete"] != true {
		t.Fatal("retry lost untouched mappings", cow.BackendStats())
	}
}

func TestCheckpointLazyPresenceFailureAndCancellation(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprint(canceled), func(t *testing.T) {
			cow, _ := reopenPresenceCOW(t, false)
			backend := cow.diff.backend
			failure := io.ErrUnexpectedEOF
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cow.diff.backend = &presenceHookBackend{diffBackend: backend, hook: func(context.Context, uint64, []bool) error {
				if canceled {
					cancel()
					return context.Canceled
				}
				return failure
			}}
			var reported atomic.Int64
			cow.SetFatalHandler(func(error) { reported.Add(1) })
			n, err := cow.readAt(ctx, make([]byte, cowBlockSize), 0)
			if canceled {
				if n != 0 || !errors.Is(err, context.Canceled) || cow.Err() != nil || reported.Load() != 0 {
					t.Fatal("cancellation poisoned store", n, err, cow.Err())
				}
				cow.diff.backend = backend
				if _, err := cow.ReadAt(make([]byte, cowBlockSize), 0); err != nil {
					t.Fatal(err)
				}
			} else {
				if n != 0 || !errors.Is(err, failure) || !errors.Is(cow.Flush(), failure) || reported.Load() != 1 {
					t.Fatal("index failure was not sticky", n, err, cow.Err(), reported.Load())
				}
				if _, err := cow.WriteAt([]byte{1}, 0); !errors.Is(err, failure) {
					t.Fatal(err)
				}
			}
			if cow.BackendStats()["diff_presence_complete"] != false {
				t.Fatal("failed resolution completed bitmap")
			}
		})
	}
}
