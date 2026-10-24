package versioned

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestNegativeControlDetectsMissingRetainedProtection(t *testing.T) {
	s, _ := newTestStore(t, 64, nil)
	blocks := []uint64{1, 2, 17, 44}
	want := make(map[uint64][]byte)
	for _, block := range blocks {
		want[block] = pageFor(block, 1)
	}
	putPages(t, s, blocks, 1)
	if err := s.Checkpoint(ctx, "pinned"); err != nil {
		t.Fatal(err)
	}
	v, err := s.Export(ctx, "pinned")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if err = s.DeleteCheckpoint(ctx, "pinned"); err != nil {
		t.Fatal(err)
	}
	checkView(t, v, want)
	// Deliberately violate the production invariant via a package-private test
	// fixture. A pin is still held, but force the allocator to regard its root as
	// unprotected. There is no corresponding production option or bypass API.
	s.mu.Lock()
	err = s.mutate(ctx, func(tx *transaction) error {
		slot, cp, err := s.findCheckpoint("pinned", tx)
		if err != nil {
			return err
		}
		if !cp.Retained {
			return fmt.Errorf("production deletion discarded a pinned root")
		}
		cp.Retained = false
		tx.ctrl.Protected = 0
		return tx.putCheckpoint(slot, cp)
	})
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	before := s.Stats()
	putPages(t, s, blocks, 2)
	if s.Stats().Allocated != before.Allocated {
		t.Fatal("negative fixture did not exercise an incorrect private overwrite")
	}
	err = compareView(v, want)
	if err == nil || !strings.Contains(err.Error(), "content mismatch") {
		t.Fatalf("full historical payload oracle missed broken CoW: %v", err)
	}
	t.Logf("negative control detected the deliberately broken retained-root rule: %v", err)
}

func TestMetadataIOFailureIsStickyButMissingIDIsNot(t *testing.T) {
	s, _ := newTestStore(t, 64, nil)
	if err := s.DeleteCheckpoint(ctx, "absent"); !errors.Is(err, ErrNotExist) {
		t.Fatal(err)
	}
	if err := s.Err(); err != nil {
		t.Fatal("missing ID poisoned owner", err)
	}
	putPages(t, s, []uint64{1}, 1)
	if err := s.Discard(ctx, PageSize, PageSize, true); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.beforeIO = func(stage string) error {
		if stage == "gc-punch" {
			return unix.EIO
		}
		return nil
	}
	s.mu.Unlock()
	if _, err := s.GC(ctx, 64); !errors.Is(err, unix.EIO) {
		t.Fatal(err)
	}
	if !errors.Is(s.Err(), unix.EIO) {
		t.Fatal("background metadata failure not exposed", s.Err())
	}
}

func TestDirectoryRejectsDiffsSymlink(t *testing.T) {
	dir, other := t.TempDir(), t.TempDir()
	if err := os.Symlink(other, filepath.Join(dir, "diffs")); err != nil {
		t.Fatal(err)
	}
	if owner, err := LockDirectory(dir); err == nil {
		owner.Close()
		t.Fatal("accepted symlinked DIFF directory")
	}
}

func TestRetiredRenamedStoreRejectsCacheMutation(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "published.diff")
	partial := filepath.Join(dir, "partial.diff")
	s, err := Create(partial, Options{DiffID: "published", LogicalSize: 64 * PageSize, LockPath: final + ".lock"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = os.Rename(partial, final); err != nil {
		t.Fatal(err)
	}
	v, err := s.Export(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Retire(ctx); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	if err = s.MarkModified(ctx); !errors.Is(err, ErrRetired) {
		t.Fatal("retired owner allowed external cache mutation", err)
	}
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	if err = s.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(final); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("published inode remains after retirement", err)
	}
}

func TestInfoAfterCloseKeepsIdentityWithoutContinuation(t *testing.T) {
	s, _ := newTestStore(t, 64, nil)
	if err := s.Checkpoint(ctx, "current"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	info := s.Info()
	if info.DiffID != "test" || info.LogicalSize != 64*PageSize || info.CurrentCheckpoint != "" || s.CanResume("current") {
		t.Fatalf("closed store identity/continuation: %+v", info)
	}
}
