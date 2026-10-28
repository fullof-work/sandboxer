package uffd

import (
	"context"
	"runtime"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"golang.org/x/sys/unix"
)

func TestRealWriteProtectCleanFillDirtyWriteAndReleaseCapture(t *testing.T) {
	if runtime.GOARCH != "amd64" {
		t.Skip("atomic shmem WP is the x86 path; ARM uses native KVM log")
	}
	h, backend, cpu := realShmemHandler(t, 32, newRecordingSnapshot(32*PageSize, sparse.Data, 0x37), true)
	if cpu[0] != 0x37 {
		t.Fatal("urgent fill content")
	}
	if err := h.EnsureLoaded(context.Background(), 20*PageSize, PageSize); err != nil {
		t.Fatal(err)
	}
	v, err := h.CaptureView()
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range v.Dirty {
		if b != 0 {
			t.Fatalf("clean COPY/WP materialization dirty: %x", v.Dirty)
		}
	}
	cpu[0] = 0x71
	if backend[0] != 0x71 {
		t.Fatal("WP write content")
	}
	v, err = h.CaptureView()
	if err != nil {
		t.Fatal(err)
	}
	if v.Dirty[0]&1 == 0 {
		t.Fatal("WP branch lost write")
	}
	// Real release ordering, including an already sparse parent page.
	if err := unix.Madvise(cpu[24*PageSize:25*PageSize], unix.MADV_DONTNEED); err != nil {
		t.Fatal(err)
	}
	waitRemove(t, h)
	if err := unix.Fallocate(h.cfg.MemfdFD, unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, 24*PageSize, PageSize); err != nil {
		t.Fatal(err)
	}
	v, err = h.CaptureView()
	if err != nil {
		t.Fatal(err)
	}
	if v.Released[3]&1 == 0 || v.Materialized[3]&1 != 0 {
		t.Fatalf("released hole: %+v", v)
	}
	if err := h.EnsureLoaded(context.Background(), 24*PageSize, PageSize); err != nil {
		t.Fatal(err)
	}
	if backend[24*PageSize] != 0 {
		t.Fatal("released parent refilled stale bytes")
	}
	cpu[24*PageSize] = 0x5d
	v, err = h.CaptureView()
	if err != nil {
		t.Fatal(err)
	}
	if v.Released[3]&1 == 0 || v.Materialized[3]&1 == 0 || v.Dirty[3]&1 == 0 {
		t.Fatal("reuse cleared R/D or lost Data")
	}
}
func TestRealWriteProtectZeroAndTailStayClean(t *testing.T) {
	if runtime.GOARCH != "amd64" {
		t.Skip("x86 atomic WP path")
	}
	for _, source := range []SnapshotReader{ZeroSource{}, newRecordingSnapshot(32*PageSize, sparse.Zero, 0), newRecordingSnapshot(32*PageSize, sparse.Hole, 0)} {
		h, backend, cpu := realShmemHandler(t, 32, source, true)
		if cpu[0] != 0 {
			t.Fatal("zero content")
		}
		if err := h.EnsureLoaded(context.Background(), 20*PageSize, PageSize); err != nil {
			t.Fatal(err)
		}
		v, err := h.CaptureView()
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range v.Dirty {
			if b != 0 {
				t.Fatalf("clean Zero/Hole dirty: %x", v.Dirty)
			}
		}
		cpu[20*PageSize] = 9
		if backend[20*PageSize] != 9 {
			t.Fatal("zero clean page not writable")
		}
		v, err = h.CaptureView()
		if err != nil {
			t.Fatal(err)
		}
		if v.Dirty[2]&(1<<4) == 0 {
			t.Fatal("zero WP write lost")
		}
	}
}
func TestCaptureRejectsUnexplainedLoadedHole(t *testing.T) {
	h, _, _ := realShmemHandler(t, 1, ZeroSource{})
	h.installMu.Lock()
	h.state.Set(0, StateLoaded)
	h.installMu.Unlock()
	if _, err := h.CaptureView(); err == nil {
		t.Fatal("Loaded hole without R accepted")
	}
}
