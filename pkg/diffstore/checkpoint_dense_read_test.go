package diffstore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"golang.org/x/sys/unix"
)

// This probe observes the frontend/backend boundary, not physical syscalls.
// The diffstore read tests independently count real O_DIRECT pread calls;
// together they ensure COW delivers bounded runs to that tested implementation.
type checkpointReadRunProbe struct {
	diffBackend
	calls, bytes atomic.Int64
}

func (p *checkpointReadRunProbe) withDirectRead(length int, off int64, consume func([]byte) error) error {
	p.calls.Add(1)
	p.bytes.Add(int64(length))
	return p.diffBackend.withDirectRead(length, off, consume)
}

func requireCheckpointDirectFD(t *testing.T, path string) {
	t.Helper()
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
		if err != nil || target != path {
			continue
		}
		n, err := strconv.Atoi(fd.Name())
		if err != nil {
			t.Fatal(err)
		}
		flags, err := unix.FcntlInt(uintptr(n), unix.F_GETFL, 0)
		if err != nil || flags&unix.O_DIRECT == 0 {
			t.Fatalf("active checkpoint fd is not O_DIRECT: flags=%#x err=%v", flags, err)
		}
		return
	}
	t.Fatal("active checkpoint fd not found")
}

func reopenDenseReadCOW(t *testing.T, encrypted bool) (*Disk, []byte) {
	t.Helper()
	const size = 1<<20 + 3*cowBlockSize
	path := filepath.Join(t.TempDir(), "active.diff")
	cow := checkpointCOW(t, path, nil, size, encrypted)
	want := patternedBytes(size)
	if n, err := cow.WriteAt(want, 0); err != nil || n != len(want) {
		t.Fatal(n, err)
	}
	if err := cow.Checkpoint(context.Background(), "retained"); err != nil {
		t.Fatal(err)
	}
	if err := cow.Close(); err != nil {
		t.Fatal(err)
	}
	cache := testCache(t, 512, 256, cacheHooks{})
	options := []DiskOption{WithCheckpoint("disk-under-test", "immutable-base"), WithCache(cache)}
	if encrypted {
		options = append(options, WithDiffEncryption([32]byte{1, 2, 3}, true))
	}
	cow, err := OpenDisk(path, nil, DiffInit{Existing: true}, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cow.Close() })
	requireCheckpointDirectFD(t, path)
	return cow, want
}

func TestCheckpointCOWDenseColdAndHotRead(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			cow, want := reopenDenseReadCOW(t, encrypted)
			backend, err := cow.checkpointBackend()
			if err != nil {
				t.Fatal(err)
			}
			probe := &checkpointReadRunProbe{diffBackend: backend}
			cow.diff = probe
			got := make([]byte, 1<<20)
			before := backend.store.Stats().DataRead
			if n, err := cow.ReadAt(got, 0); err != nil || n != len(got) || !bytes.Equal(got, want[:len(got)]) {
				t.Fatal("cold full read", n, err)
			}
			if probe.calls.Load() != int64(len(got)/maxDiffScratchSize) || probe.bytes.Load() != int64(len(got)) {
				t.Fatalf("frontend did not deliver bounded cold runs: calls=%d bytes=%d", probe.calls.Load(), probe.bytes.Load())
			}
			after := backend.store.Stats().DataRead
			if after-before != uint64(len(got)) {
				t.Fatalf("cold payload bytes=%d want=%d", after-before, len(got))
			}
			calls := probe.calls.Load()
			clear(got)
			if n, err := cow.ReadAt(got, 0); err != nil || n != len(got) || !bytes.Equal(got, want[:len(got)]) {
				t.Fatal("hot full read", n, err)
			}
			if probe.calls.Load() != calls || backend.store.Stats().DataRead != after {
				t.Fatal("hot COW read reached storage")
			}
			// Partial edges cross the warmed prefix and a cold tail, as well as
			// the frontend's bounded-run boundary. No caller alignment is needed.
			off := int64(cowBlockSize - 17)
			got = make([]byte, len(want)-int(off)-13)
			if n, err := cow.ReadAt(got, off); err != nil || n != len(got) || !bytes.Equal(got, want[off:off+int64(len(got))]) {
				t.Fatal("unaligned mixed cold/hot read", n, err)
			}
		})
	}
}

func TestCheckpointCOWDenseConcurrentReadWriteRetainsVersion(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			cow, want := reopenDenseReadCOW(t, encrypted)
			view, err := cow.Export(context.Background(), "retained")
			if err != nil {
				t.Fatal(err)
			}
			defer view.Close()
			start := make(chan struct{})
			done := make(chan error, 3)
			updated := bytes.Repeat([]byte{0xe7}, cowBlockSize)
			go func() {
				<-start
				for block := 0; block < len(want)/cowBlockSize; block += 2 {
					if _, err := cow.WriteAt(updated, int64(block*cowBlockSize)); err != nil {
						done <- err
						return
					}
				}
				done <- cow.Drain(context.Background())
			}()
			for _, retained := range []bool{false, true} {
				go func() {
					<-start
					got := make([]byte, len(want))
					for pass := 0; pass < 4; pass++ {
						var n int
						var err error
						if retained {
							n, err = view.ReadAt(context.Background(), got, 0)
						} else {
							n, err = cow.ReadAt(got, 0)
						}
						if err != nil || n != len(got) {
							done <- fmt.Errorf("retained=%t read %d: %v", retained, n, err)
							return
						}
						for block := 0; block < len(got)/cowBlockSize; block++ {
							page := got[block*cowBlockSize : (block+1)*cowBlockSize]
							old := want[block*cowBlockSize : (block+1)*cowBlockSize]
							if !bytes.Equal(page, old) && (retained || block%2 != 0 || !bytes.Equal(page, updated)) {
								done <- fmt.Errorf("retained=%t torn or stale page %d", retained, block)
								return
							}
						}
					}
					done <- nil
				}()
			}
			close(start)
			for i := 0; i < 3; i++ {
				if err := <-done; err != nil {
					t.Error(err)
				}
			}
			if got := readExport(t, view); !bytes.Equal(got, want) {
				t.Fatal("writeback changed retained checkpoint")
			}
			for block := 0; block < len(want)/cowBlockSize; block += 2 {
				copy(want[block*cowBlockSize:], updated)
			}
			got := make([]byte, len(want))
			if n, err := cow.ReadAt(got, 0); err != nil || n != len(got) || !bytes.Equal(got, want) {
				t.Fatal("completed writes not visible", n, err)
			}
		})
	}
}
