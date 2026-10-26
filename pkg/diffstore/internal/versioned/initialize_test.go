package versioned

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore/internal/presence"
)

// The initializer is stopped before its first syscall. Cancellation exercises
// every context-bearing physical entry without relying on scheduling sleeps.
func TestPendingInitializationGatesPhysicalOperations(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "disk")
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			o := Options{DiffID: "pending", BaseID: "immutable", LogicalSize: 32 << 20}
			if encrypted {
				o.Key = &[32]byte{9}
			}
			s, err := create(path, o, true, func(stage string) error {
				if stage == "initialize" {
					close(entered)
					<-release
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			defer unblock()
			<-entered
			if s.Size() != o.LogicalSize || Identity(s).DiffID != o.DiffID || s.Err() != nil {
				t.Fatal("immutable identity/health unavailable during initialization")
			}
			if err := s.MarkModified(context.Background()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			for name, call := range map[string]func() error{
				"write":      func() error { return s.WritePages(ctx, []Page{{Block: 0, Data: make([]byte, PageSize)}}) },
				"discard":    func() error { return s.Discard(ctx, 0, PageSize) },
				"checkpoint": func() error { return s.Checkpoint(ctx, "saved") },
				"list":       func() error { _, e := s.ListCheckpoints(ctx); return e },
				"delete":     func() error { return s.DeleteCheckpoint(ctx, "saved") },
				"retire":     func() error { return s.Retire(ctx) },
				"export":     func() error { _, e := s.Export(ctx, ""); return e },
				"scan": func() error {
					return s.Scan(ctx, func(uint64) error { t.Fatal("visited uninitialized index"); return nil })
				},
				"presence": func() error { return s.Presence(ctx, 0, make([]bool, 1)) },
				"bitmap":   func() error { _, e := s.PresenceBitmap(ctx); return e },
				"payload":  func() error { return s.PayloadPresence(ctx, 0, make([]bool, 1)) },
				"gc":       func() error { _, e := s.GC(ctx, 1); return e },
			} {
				if err := call(); !errors.Is(err, context.Canceled) {
					t.Fatalf("%s: %v", name, err)
				}
			}
			if st, err := os.Stat(path); err != nil || st.Size() != 0 {
				t.Fatal("physical work escaped initializer", st, err)
			}
			s.mu.Lock()
			startedGC := s.stop != nil || s.done != nil
			s.mu.Unlock()
			if startedGC {
				t.Fatal("GC started before initialization")
			}
			unblock()
			if err := WaitInitialized(s, context.Background()); err != nil {
				t.Fatal(err)
			}
			s.mu.Lock()
			startedGC = s.stop != nil && s.done != nil
			s.mu.Unlock()
			if !startedGC {
				t.Fatal("completion preceded GC ownership")
			}
			data := bytes.Repeat([]byte{0x79}, PageSize)
			for _, off := range []int64{0, s.Size() - PageSize} {
				if _, err := s.WriteAt(data, off); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Discard(context.Background(), PageSize, PageSize); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(path, o)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			for _, off := range []int64{0, o.LogicalSize - PageSize} {
				got := make([]byte, PageSize)
				if _, err := reopened.ReadAt(got, off); err != nil || !bytes.Equal(got, data) {
					t.Fatal("reopen Data", err)
				}
			}
			p := make([]bool, 3)
			if err := reopened.Presence(context.Background(), 0, p); err != nil || !p[0] || !p[1] || p[2] {
				t.Fatal("Data/Zero/Hole presence", p, err)
			}
		})
	}
}

func TestPendingInitializationActualShortWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pending.diff")
	cmd := exec.Command(os.Args[0], "-test.run=^TestProcessHelper$")
	cmd.Env = append(os.Environ(), "DIFFSTORE_HELPER=short-pending-initialization", "DIFFSTORE_PATH="+path)
	out, err := cmd.CombinedOutput()
	if err != nil || !bytes.Contains(out, []byte("PENDING_SHORT_WITHOUT_RETRY")) {
		t.Fatalf("actual short initialization: %v\n%s", err, out)
	}
	// The incomplete private file cannot be accepted as an existing owner.
	if s, err := Open(path, Options{}); err == nil {
		s.Close()
		t.Fatal("reopened an incomplete format")
	}
}

func TestPendingInitializationCloseAndStickyFailure(t *testing.T) {
	for _, failure := range []error{nil, io.ErrShortWrite, fmt.Errorf("reserve: %w", os.ErrPermission)} {
		t.Run(fmt.Sprint(failure), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "disk")
			entered, release := make(chan struct{}), make(chan struct{})
			s, err := create(path, Options{DiffID: "pending", LogicalSize: 4096}, true, func(stage string) error {
				if stage == "initialize-reserve" {
					close(entered)
					<-release
					return failure
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			<-entered
			closed := make(chan error, 1)
			go func() { closed <- s.Close() }()
			// Close must join the owner rather than canceling its admitted I/O.
			if _, err := s.file.Stat(); err != nil {
				t.Fatal("closed live initializer FD", err)
			}
			if s.closedFlag.Load() {
				t.Fatal("close escaped initialization")
			}
			close(release)
			if err := <-closed; !errors.Is(err, failure) {
				t.Fatalf("close lost initialization result: %v", err)
			}
			if failure != nil && (s.stop != nil || s.done != nil) {
				t.Fatal("failed initializer started GC")
			}
			if s.done != nil {
				<-s.done
			}
			if _, err := s.file.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatal("FD retained after Close", err)
			}
		})
	}
}

func TestSynchronousCreateUsesSameInitializer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "disk")
	s, err := create(path, Options{DiffID: "sync", LogicalSize: PageSize}, false, func(stage string) error {
		if stage == "initialize-presence" {
			return io.ErrShortWrite
		}
		return nil
	})
	if s != nil || !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(s, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("failed synchronous creation retained file", err)
	}
	// Failure must release the stable owner lock too.
	s, err = Create(path, Options{DiffID: "sync", LogicalSize: PageSize})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	select {
	case <-Initialized(s):
	default:
		t.Fatal("synchronous Create returned pending")
	}
}

func TestCreateRejectsOversizedCapacitySynchronously(t *testing.T) {
	size := maxWritableSize + PageSize
	if err := presence.ValidateSize(size); err != nil {
		t.Fatalf("shared bitmap capacity rejected the checkpoint boundary fixture: %v", err)
	}
	for _, test := range []struct {
		name   string
		create func(string, Options) (*Store, error)
	}{
		{name: "Create", create: Create},
		{name: "CreatePending", create: CreatePending},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "oversized.diff")
			s, err := test.create(path, Options{
				DiffID: "oversized", LogicalSize: size, LockPath: path + ".lock",
			})
			if s != nil {
				_ = s.Close()
				t.Fatal("oversized checkpoint escaped synchronous validation")
			}
			if err == nil || !strings.Contains(err.Error(), "32 GiB") {
				t.Fatalf("expected checkpoint capacity rejection, got %v", err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("capacity rejection created files or locks: %v", entries)
			}
		})
	}
}
