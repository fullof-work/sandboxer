package diffstore

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// Run on a newly created, finite ext4 filesystem with
// DIFFSTORE_FULL_FS=/path/to/owned/mount. The usual unit suite must not fill a
// shared developer filesystem. This check writes actual allocated payload until
// the kernel, rather than a fault hook, reports ENOSPC.
func TestRealFilesystemFullRecovery(t *testing.T) {
	dir := os.Getenv("DIFFSTORE_FULL_FS")
	if dir == "" {
		t.Skip("requires isolated finite ext4 filesystem")
	}
	var fs unix.Statfs_t
	if err := unix.Statfs(dir, &fs); err != nil {
		t.Fatal(err)
	}
	if fs.Type != unix.EXT4_SUPER_MAGIC {
		t.Fatalf("expected ext4, got %x", fs.Type)
	}
	capacity := uint64(fs.Blocks) * uint64(fs.Bsize)
	if capacity > 256<<20 || capacity < 32<<20 {
		t.Fatalf("unsafe test filesystem size: %d", capacity)
	}
	path := filepath.Join(dir, "full.diff")
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("full.diff must not exist")
	}
	s, err := Create(path, Options{DiffID: "full", LogicalSize: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close(); os.Remove(path); os.Remove(path + ".lock") }()
	const blocks = 8192
	write := func(g byte) error {
		for start := 0; start < blocks; start += MaxBatchPages {
			pages := make([]Page, MaxBatchPages)
			for i := range pages {
				b := uint64(start + i)
				pages[i] = Page{b, pageFor(b, g)}
			}
			if err := s.WritePages(ctx, pages); err != nil {
				return err
			}
		}
		return nil
	}
	if err = write(1); err != nil {
		t.Fatal("initialize 32MiB", err)
	}
	if err = s.Checkpoint(ctx, "protected"); err != nil {
		t.Fatal(err)
	}
	err = write(2)
	if !errors.Is(err, unix.ENOSPC) {
		t.Fatalf("expected actual ENOSPC, got %v", err)
	}
	t.Logf("kernel ENOSPC: %v; stats=%+v", err, s.Stats())
	s.Close()
	v, err := OpenExport(ctx, path, "protected", nil)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, PageSize)
	for block := uint64(0); block < blocks; block++ {
		if _, err = v.ReadAt(ctx, buf, block*PageSize); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf, pageFor(block, 1)) {
			t.Fatalf("protected content corrupted at %d", block)
		}
	}
	v.Close()
	s, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteCheckpoints(ctx, []string{"protected"}); err != nil {
		t.Fatal("delete at full filesystem", err)
	}
	for i := 0; i < 1024; i++ {
		if _, err = s.GC(ctx, 64); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		pending := s.ctrl.Pending + s.ctrl.Garbage
		s.mu.Unlock()
		if pending == 0 {
			break
		}
	}
	if err = write(3); err != nil {
		t.Fatal("write after GC at full filesystem", err)
	}
	if _, err = s.ReadAt(buf, 10*PageSize); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, pageFor(10, 3)) {
		t.Fatal("reused slot content mismatch")
	}
	if err = unix.Statfs(dir, &fs); err != nil {
		t.Fatal(err)
	}
	t.Logf("full version checked: %d bytes; successful post-GC private overwrite; free=%d", blocks*PageSize, uint64(fs.Bavail)*uint64(fs.Bsize))
	fmt.Fprintln(os.Stdout, "REAL_EXT4_ENOSPC_RECOVERED")
}
