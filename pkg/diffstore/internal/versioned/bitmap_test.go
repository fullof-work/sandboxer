package versioned

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore/internal/presence"
)

func TestZeroPresenceSnapshotsAndReaderRelease(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			var key *[32]byte
			if encrypted {
				key = &[32]byte{9}
			}
			s, path := newTestStore(t, 64, key)
			stopTestGC(s)
			putPages(t, s, []uint64{1}, 1)
			if err := s.Discard(ctx, 2*PageSize, PageSize); err != nil {
				t.Fatal(err)
			}
			if err := s.Checkpoint(ctx, "zero"); err != nil {
				t.Fatal(err)
			}
			v, err := s.Export(ctx, "zero")
			if err != nil {
				t.Fatal(err)
			}
			defer v.Close()
			for block, want := range map[uint64]sparse.RunKind{0: sparse.Hole, 1: sparse.Data, 2: sparse.Zero, 3: sparse.Hole} {
				run, err := v.RunAt(block*PageSize, PageSize)
				if err != nil || run.Kind() != want || run.End() != (block+1)*PageSize {
					t.Fatalf("block %d: %v %v", block, run, err)
				}
			}
			if err := s.Discard(ctx, PageSize, 2*PageSize); err != nil {
				t.Fatal(err)
			}
			checkView(t, v, map[uint64][]byte{1: pageFor(1, 1), 2: nil})
			checkActive(t, s, map[uint64][]byte{1: nil, 2: nil})
			if err := s.Checkpoint(ctx, "after-discard"); err != nil {
				t.Fatal(err)
			}
			afterDiscard, err := s.Export(ctx, "after-discard")
			if err != nil {
				t.Fatal(err)
			}
			checkView(t, afterDiscard, map[uint64][]byte{1: nil, 2: nil})
			if err := afterDiscard.Close(); err != nil {
				t.Fatal(err)
			}
			putPages(t, s, []uint64{3}, 2)
			if err := s.DeleteCheckpoint(ctx, "zero"); err != nil {
				t.Fatal(err)
			}
			result := finishTestGC(t, s, 1)
			if result.State != BlockedByReaders {
				t.Fatal(result)
			}
			before := s.Stats()
			for range 3 {
				if r := finishTestGC(t, s, 1); r.State != BlockedByReaders {
					t.Fatal(r)
				}
			}
			if s.Stats().RedoWritten != before.RedoWritten {
				t.Fatal("reader-blocked bitmap GC wrote metadata")
			}
			buf := bytes.Repeat([]byte{255}, PageSize)
			if _, err := v.ReadAt(ctx, buf, 2*PageSize); err != nil || !bytes.Equal(buf, make([]byte, PageSize)) {
				t.Fatalf("pinned Zero lost: %v", err)
			}
			if err := v.Close(); err != nil {
				t.Fatal(err)
			}
			if r := finishTestGC(t, s, 1); r.State != Complete || r.FreedSlots < 1 {
				t.Fatal(r)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(path, Options{Key: key})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			bitmap, err := s.PresenceBitmap(ctx)
			if err != nil || !presence.Has(bitmap, 1) || !presence.Has(bitmap, 2) || !presence.Has(bitmap, 3) {
				t.Fatalf("reopen presence: %x %v", bitmap, err)
			}
			checkActive(t, s, map[uint64][]byte{1: nil, 2: nil, 3: pageFor(3, 2)})
		})
	}
}

func TestFragmentedMaximumBitmapCopyAndPartialGCReopen(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			var key *[32]byte
			if encrypted {
				key = &[32]byte{71}
			}
			path := filepath.Join(t.TempDir(), "fragmented")
			s, err := Create(path, Options{DiffID: "fragmented", LogicalSize: presence.MaxSize, Key: key})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close() }()
			stopTestGC(s)
			// Model a fully reclaimed allocator whose first free record in each
			// group is offered before the remaining positions. All slots remain
			// on the free chain; no live or unreachable payload is fabricated.
			groups := presence.Pages(s.Size()) + 1
			order := make([]uint64, 0, groups*slotsPerGroup)
			for g := 0; g < groups; g++ {
				order = append(order, uint64(g*slotsPerGroup+1))
			}
			for g := 0; g < groups; g++ {
				for n := 2; n <= slotsPerGroup; n++ {
					order = append(order, uint64(g*slotsPerGroup+n))
				}
			}
			if err := s.file.Truncate(int64(slotPage(uint64(len(order)))+1) * PageSize); err != nil {
				t.Fatal(err)
			}
			s.mu.Lock()
			err = s.mutate(ctx, func(tx *transaction) error {
				tx.ctrl.Next = uint64(len(order)) + 1
				tx.ctrl.Free = order[0]
				for g := 0; g < groups; g++ {
					page := descriptorPage(uint64(g*slotsPerGroup + 1))
					b := make([]byte, PageSize)
					copy(b, "KDDESC4")
					sealPage(b)
					tx.pages[page] = b
					tx.fresh[page] = true
				}
				for i, slot := range order {
					next := uint64(0)
					if i+1 < len(order) {
						next = order[i+1]
					}
					if err := tx.setDesc(slot, descriptor{Next: next, Kind: kindFree}); err != nil {
						return err
					}
				}
				return nil
			})
			s.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			before := s.Stats()
			start := time.Now()
			if err := s.Checkpoint(ctx, "max-fragmented"); err != nil {
				t.Fatal(err)
			}
			after := s.Stats()
			if after.Commits-before.Commits != 1 || after.DataWritten != before.DataWritten || after.Reused-before.Reused != uint64(groups) {
				t.Fatal("non-atomic or copying payload", before, after)
			}
			t.Logf("32 GiB capacity, 1 MiB bitmap, %d distinct descriptor groups: checkpoint=%s redo_bytes=%d", groups, time.Since(start), after.RedoWritten-before.RedoWritten)
			if err := s.DeleteCheckpoint(ctx, "max-fragmented"); err != nil {
				t.Fatal(err)
			}
			var freed uint64
			for i := 0; i < 21; i++ {
				r, err := s.GC(ctx, 1)
				if err != nil || r.State != More {
					t.Fatal(r, err)
				}
				freed += r.FreedSlots
			}
			s.mu.Lock()
			_, cp, err := s.findCheckpoint("max-fragmented", nil)
			s.mu.Unlock()
			if err != nil || cp.BitmapDone == 0 || cp.Bitmap == 0 || !cp.Pending {
				t.Fatal("partial bitmap cursor not durable", cp, err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			// Metadata GC must resume this encrypted bitmap without a data key.
			s, err = OpenMetadata(path)
			if err != nil {
				t.Fatal(err)
			}
			for calls := 0; ; calls++ {
				if calls > 600 {
					t.Fatal("bitmap GC stalled after reopen")
				}
				r, err := s.GC(ctx, 1)
				if err != nil || r.FreedSlots > 1 {
					t.Fatal(r, err)
				}
				freed += r.FreedSlots
				if r.State == Complete {
					break
				}
				if r.State != More {
					t.Fatal(r)
				}
			}
			if freed != uint64(presence.Pages(s.Size())) {
				t.Fatal("exclusive bitmap reclamation count", freed)
			}
		})
	}
}

func TestBitmapOnlyChangeAndRedoRecovery(t *testing.T) {
	s, path := newTestStore(t, 64, nil)
	stopTestGC(s)
	if err := s.Checkpoint(ctx, "empty"); err != nil {
		t.Fatal(err)
	}
	if err := s.Discard(ctx, 0, PageSize); err != nil {
		t.Fatal(err)
	}
	if s.CanResume("empty") || s.ctrl.Root != 0 {
		t.Fatal("bitmap-only change did not invalidate continuation")
	}
	before := s.Stats()
	if err := s.Discard(ctx, 0, PageSize); err != nil {
		t.Fatal(err)
	}
	if s.Stats().Commits != before.Commits {
		t.Fatal("unchanged bitmap caused a commit")
	}
	injected := errors.New("stop after committed redo")
	s.beforeIO = func(stage string) error {
		if stage == "apply" {
			return injected
		}
		return nil
	}
	if err := s.Discard(ctx, PageSize, PageSize); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	_ = s.Close()
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	bitmap, err := s.PresenceBitmap(ctx)
	if err != nil || !presence.Has(bitmap, 0) || !presence.Has(bitmap, 1) {
		t.Fatalf("bitmap redo not replayed: %x %v", bitmap, err)
	}
	v, err := s.Export(ctx, "empty")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	run, err := v.RunAt(0, 2*PageSize)
	if err != nil || run.Kind() != sparse.Hole {
		t.Fatalf("old snapshot bitmap changed: %v %v", run, err)
	}
}

func TestMaximumPresenceCopyBudgetAndBoundedGC(t *testing.T) {
	s, err := Create(filepath.Join(t.TempDir(), "max.diff"), Options{DiffID: "max", LogicalSize: presence.MaxSize})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	stopTestGC(s)
	if err := s.Discard(ctx, presence.MaxSize-PageSize, PageSize); err != nil {
		t.Fatal(err)
	}
	before := s.Stats()
	if err := s.Checkpoint(ctx, "max"); err != nil {
		t.Fatal(err)
	}
	after := s.Stats()
	if after.DataWritten != before.DataWritten || after.DataRead != before.DataRead || after.Commits-before.Commits != 1 {
		t.Fatal("presence copy used data I/O or multiple commits")
	}
	s.mu.Lock()
	_, cp, err := s.findCheckpoint("max", nil)
	bitmap, readErr := readSnapshotPresence(s.readPage, cp, s.Size(), s.ctrl.Next)
	s.mu.Unlock()
	if err != nil || readErr != nil || len(bitmap) != 1<<20 || !presence.Has(bitmap, uint64(s.Size()/PageSize-1)) {
		t.Fatalf("max copy: %v %v", err, readErr)
	}
	if err := s.DeleteCheckpoint(ctx, "max"); err != nil {
		t.Fatal(err)
	}
	var freed uint64
	for calls := 0; ; calls++ {
		if calls > 600 {
			t.Fatal("budget-one bitmap reclamation did not complete")
		}
		r, err := s.GC(ctx, 1)
		if err != nil || r.FreedSlots > 1 {
			t.Fatalf("GC budget: %+v %v", r, err)
		}
		freed += r.FreedSlots
		if r.State == Complete {
			break
		}
		if r.State != More {
			t.Fatal(r)
		}
	}
	if freed != uint64(presence.Pages(s.Size())) {
		t.Fatalf("freed %d bitmap slots", freed)
	}
}
