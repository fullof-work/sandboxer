package diffstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
)

type contractBase struct {
	data    []byte
	closed  atomic.Int32
	entered chan struct{}
}

func (b *contractBase) Size() int64  { return int64(len(b.data)) }
func (b *contractBase) Close() error { b.closed.Add(1); return nil }
func (b *contractBase) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(b.data)) {
		return 0, io.EOF
	}
	n := copy(p, b.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
func (b *contractBase) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	if b.entered != nil {
		close(b.entered)
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return b.ReadAt(p, off)
}

func TestDiskBackendPublicContract(t *testing.T) {
	for _, versioned := range []bool{false, true} {
		for _, encrypted := range []bool{false, true} {
			t.Run(fmt.Sprintf("checkpoint=%t/xts=%t", versioned, encrypted), func(t *testing.T) {
				ctx := context.Background()
				cache, err := diffstore.NewCache(16*4096, 8*4096)
				if err != nil {
					t.Fatal(err)
				}
				defer cache.Close()
				options := []diffstore.DiskOption{diffstore.WithCache(cache)}
				if versioned {
					options = append(options, diffstore.WithCheckpoint("contract", "base"))
				}
				if encrypted {
					options = append(options, diffstore.WithDiffEncryption([32]byte{77}, true))
				}
				base := &contractBase{data: bytes.Repeat([]byte{0x31}, 4*4096)}
				path := filepath.Join(t.TempDir(), "active.diff")
				disk, err := diffstore.OpenDisk(path, base, diffstore.DiffInit{CreateSize: 4 * 4096}, options...)
				if err != nil {
					t.Fatal(err)
				}
				defer disk.Close()
				manager, enabled := disk.Checkpoints()
				if enabled != versioned || (manager != nil) != versioned {
					t.Fatal("incorrect capability")
				}
				patch := bytes.Repeat([]byte{0x72}, 73)
				if n, err := disk.WriteAtContext(ctx, patch, 4090); err != nil || n != len(patch) {
					t.Fatal(n, err)
				}
				if err := disk.Drain(ctx); err != nil {
					t.Fatal(err)
				}
				want := append([]byte(nil), base.data...)
				copy(want[4090:], patch)
				got := make([]byte, len(want))
				if _, err := disk.ReadAtContext(ctx, got, 0); err != nil || !bytes.Equal(got, want) {
					t.Fatal("partial write lost lower data", err)
				}
				active, err := disk.OpenActiveView(ctx)
				if err != nil {
					t.Fatal(err)
				}
				upper := make([]byte, len(want))
				if _, err := active.ReadAt(ctx, upper, 0); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(upper[:8192], want[:8192]) || !bytes.Equal(upper[8192:], make([]byte, 8192)) {
					t.Fatal("active view included lower holes")
				}
				if err := active.Close(); err != nil {
					t.Fatal(err)
				}
				var pin diffstore.ExportView
				if manager != nil {
					if err := manager.Checkpoint(ctx, "saved"); err != nil {
						t.Fatal(err)
					}
					pin, err = manager.OpenCheckpoint(ctx, "saved")
					if err != nil {
						t.Fatal(err)
					}
					defer pin.Close()
					if _, err := manager.OpenCheckpoint(ctx, ""); err == nil {
						t.Fatal("empty named ID accepted")
					}
				}
				if err := disk.Discard(4096, 4096); err != nil {
					t.Fatal(err)
				}
				if _, err := disk.ReadAt(got, 0); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got[4096:8192], base.data[4096:8192]) {
					t.Fatal("discard did not reveal base")
				}
				// Closing one owner cannot close another owner's shared cache.
				other, err := diffstore.OpenDisk(filepath.Join(t.TempDir(), "other.diff"), nil, diffstore.DiffInit{CreateSize: 4096}, diffstore.WithCache(cache))
				if err != nil {
					t.Fatal(err)
				}
				defer other.Close()
				if err := disk.Close(); err != nil {
					t.Fatal(err)
				}
				if base.closed.Load() != 0 {
					t.Fatal("Disk closed a borrowed base")
				}
				if err := base.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := other.WriteAt(patch, 0); err != nil {
					t.Fatal(err)
				}
				if err := other.Drain(ctx); err != nil {
					t.Fatal(err)
				}
				if pin != nil {
					clear(got)
					if _, err := pin.ReadAt(ctx, got, 0); err != nil || !bytes.Equal(got, upper) {
						t.Fatal("named pin did not survive mutation/owner close", err)
					}
				}
			})
		}
	}
}

func TestDiskContextCancellationCrossesPackageBoundary(t *testing.T) {
	for _, versioned := range []bool{false, true} {
		t.Run(fmt.Sprint(versioned), func(t *testing.T) {
			base := &contractBase{data: make([]byte, 4096), entered: make(chan struct{})}
			var options []diffstore.DiskOption
			if versioned {
				options = append(options, diffstore.WithCheckpoint("cancel", "base"))
			}
			disk, err := diffstore.OpenDisk(filepath.Join(t.TempDir(), "active.diff"), base, diffstore.DiffInit{CreateSize: 4096}, options...)
			if err != nil {
				t.Fatal(err)
			}
			defer disk.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := disk.WriteAtContext(ctx, []byte{1}, 17); done <- err }()
			select {
			case <-base.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("base did not receive request")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("cancellation lost across storage interface")
			}
			if disk.DirtyCount() != 0 {
				t.Fatal("canceled partial write became visible")
			}
		})
	}
}
