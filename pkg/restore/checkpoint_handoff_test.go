package restore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/sandboxer/internal/diffstore"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"github.com/kuasar-sandbox/sandboxer/pkg/vhost"
	"golang.org/x/sys/unix"
)

// Query the real OFD view-lifetime read lock at byte 2. GC/Delete acquire
// separate version locks, so querying only this byte avoids their transient
// write locks and does not acquire a lock that could interfere with them.
// A successful List/Delete alone cannot prove reader release.
func restoreReadersHeld(path string) (bool, error) {
	f, err := os.OpenFile(path+".lock", os.O_RDWR, 0)
	if err != nil {
		return false, err
	}
	defer f.Close()
	lock := unix.Flock_t{Type: unix.F_WRLCK, Whence: 0, Start: 2, Len: 1}
	if err := unix.FcntlFlock(f.Fd(), unix.F_OFD_GETLK, &lock); err != nil {
		return false, err
	}
	return lock.Type == unix.F_RDLCK, nil
}

func assertRestoreReaders(t *testing.T, paths []string, want bool) {
	t.Helper()
	for _, path := range paths {
		if held, err := restoreReadersHeld(path); err != nil || held != want {
			t.Fatalf("readers for %s: held=%t err=%v, want %t", path, held, err, want)
		}
	}
}

func restoreHandoffPlan(t *testing.T, opts Options, source *openedSandboxSource) (Options, *config.SandboxConfig, []string, [32]byte) {
	t.Helper()
	cfg := &config.SandboxConfig{}
	for _, disk := range source.Root.Portable.Boot.Disks {
		cfg.Boot.Disks = append(cfg.Boot.Disks, config.DiskConfig{Name: disk.Name})
	}
	directory := filepath.Join(opts.BaseRoot, opts.PathID, "checkpoint")
	var err error
	opts.checkpointPlan, err = sandbox.PrepareCheckpointDiskPlan(cfg, source.Root.Portable, directory, opts.checkpointReopen)
	if err != nil {
		t.Fatal(err)
	}
	var key [32]byte
	if opts.CustomerKeyFn != nil {
		key, err = opts.CustomerKeyFn()
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := preflightRestoreDiskGraph(context.Background(), cfg, opts, source.RelativeDir, key); err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, disk := range opts.checkpointPlan.Disks {
		paths = append(paths, disk.Path)
	}
	return opts, cfg, paths, key
}

func TestRestoreCheckpointSourcesHandOffAfterEveryWriter(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%t", encrypted), func(t *testing.T) {
			ctx := context.Background()
			opts, source, want := restoreCheckpointDiskFixture(t, encrypted, 2)
			opts, source, release, err := prepareRestoreCheckpoint(ctx, opts, source)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			opts, cfg, paths, key := restoreHandoffPlan(t, opts, source)
			assertRestoreReaders(t, paths, true) // ReopenIDs has not released any pin.
			var storeKey *[32]byte
			if encrypted {
				storeKey = &key
			}
			earlyExporter, err := diffstore.OpenExport(ctx, paths[0], "cp", storeKey)
			if err != nil {
				t.Fatal(err)
			}
			defer earlyExporter.Close()
			releaseSources := opts.checkpointReleaseSources
			calls := 0
			opts.checkpointReleaseSources = func() error {
				calls++
				for _, path := range paths {
					if held, err := restoreReadersHeld(path); err != nil || !held {
						return fmt.Errorf("source released before handoff: %s %t %v", path, held, err)
					}
					store, err := diffstore.Open(path, diffstore.Options{Key: storeKey})
					if store != nil {
						_ = store.Close()
					}
					if !errors.Is(err, diffstore.ErrBusy) {
						return fmt.Errorf("writer not bound at handoff: %s: %v", path, err)
					}
				}
				return releaseSources()
			}
			cache, err := vhost.NewCOWCache(32768, 16384)
			if err != nil {
				t.Fatal(err)
			}
			defer cache.Close()
			disks, cleanup, err := reconstructRestoreDisks(ctx, opts, key, cfg, func(string, ...any) {}, cache)
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()
			if calls != 1 || len(disks) != 3 {
				t.Fatalf("handoff calls=%d disks=%d", calls, len(disks))
			}
			assertRestoreReaders(t, paths[1:], false)
			assertRestoreReaders(t, paths[:1], true) // Independent exporter survives.
			if owner, err := diffstore.LockDirectory(opts.checkpointPlan.Directory); !errors.Is(err, diffstore.ErrBusy) {
				if owner != nil {
					owner.Close()
				}
				t.Fatalf("target owner was released with source pins: %v", err)
			}
			lateExporter, err := diffstore.OpenExport(ctx, paths[0], "cp", storeKey)
			if err != nil {
				t.Fatal(err)
			}
			defer lateExporter.Close()
			for _, disk := range disks {
				got := make([]byte, len(want))
				if _, err := disk.Cow.ReadAt(got, 0); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("active disk after handoff: %v", err)
				}
				ids, err := disk.Cow.ListCheckpoints(ctx)
				if err != nil || len(ids) != 1 || ids[0] != "cp" {
					t.Fatalf("handoff changed visible names: %v %v", ids, err)
				}
				if err := disk.Cow.DeleteCheckpoint(ctx, "cp"); err != nil {
					t.Fatal(err)
				}
				if _, err := disk.Cow.WriteAt([]byte{0x98}, 71); err != nil {
					t.Fatal(err)
				}
				if err := disk.Cow.Drain(ctx); err != nil {
					t.Fatal(err)
				}
			}
			for _, view := range []*diffstore.View{earlyExporter, lateExporter} {
				got := make([]byte, len(want))
				if _, err := view.ReadAt(ctx, got, 0); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("independent fixed export changed: %v", err)
				}
			}
			if err := earlyExporter.Close(); err != nil {
				t.Fatal(err)
			}
			assertRestoreReaders(t, paths[:1], true)
			if err := lateExporter.Close(); err != nil {
				t.Fatal(err)
			}
			assertRestoreReaders(t, paths, false)
			if err := releaseSources(); err != nil { // Exit cleanup may repeat this.
				t.Fatal(err)
			}
		})
	}
}

func TestRestoreCheckpointFailedHandoffCleansEveryWriter(t *testing.T) {
	for _, failure := range []string{"last-writer", "release-close", "canceled"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opts, source, _ := restoreCheckpointDiskFixture(t, false, 2)
			opts, source, release, err := prepareRestoreCheckpoint(ctx, opts, source)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			opts, cfg, paths, key := restoreHandoffPlan(t, opts, source)
			closeFailure := errors.New("injected source close failure")
			releaseSources := opts.checkpointReleaseSources
			calls := 0
			opts.checkpointReleaseSources = func() error {
				calls++
				return errors.Join(releaseSources(), closeFailure)
			}
			var conflict *diffstore.Store
			if failure == "last-writer" {
				conflict, err = diffstore.Open(paths[2], diffstore.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer conflict.Close()
			}
			cache, err := vhost.NewCOWCache(32768, 16384)
			if err != nil {
				t.Fatal(err)
			}
			defer cache.Close()
			logf := func(format string, args ...any) {
				if failure == "canceled" && fmt.Sprintf(format, args...) == "disk disk0 image: opening captured top" {
					cancel() // The root writer has already opened.
				}
			}
			_, cleanup, err := reconstructRestoreDisks(ctx, opts, key, cfg, logf, cache)
			if err == nil {
				t.Fatal("failed handoff succeeded")
			}
			if failure == "release-close" {
				if !errors.Is(err, closeFailure) || calls != 1 {
					t.Fatalf("close failure not propagated: calls=%d err=%v", calls, err)
				}
				assertRestoreReaders(t, paths, false)
			} else {
				if calls != 0 {
					t.Fatalf("partial handoff released sources: %d", calls)
				}
				assertRestoreReaders(t, paths, true)
			}
			if failure == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
			if conflict != nil {
				if err := conflict.Close(); err != nil {
					t.Fatal(err)
				}
			}
			for _, path := range paths {
				store, err := diffstore.Open(path, diffstore.Options{})
				if err != nil {
					t.Fatalf("partial writer leaked: %s: %v", path, err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := cleanup(); err != nil {
				t.Fatal("repeat disk cleanup", err)
			}
			if err := release(); err != nil {
				t.Fatal(err)
			}
			assertRestoreReaders(t, paths, false)
			owner, err := diffstore.LockDirectory(opts.checkpointPlan.Directory)
			if err != nil {
				t.Fatal("directory owner leaked", err)
			}
			owner.Close()
		})
	}
}

func TestRestoreCheckpointMaterializationReleasesOnlySources(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		for _, scenario := range []string{"cross-enabled", "cross-disabled", "historical-enabled", "same-disabled"} {
			t.Run(fmt.Sprintf("%s/encrypted=%t", scenario, encrypted), func(t *testing.T) {
				ctx := context.Background()
				opts, original, want := restoreCheckpointDiskFixture(t, encrypted, 2)
				var paths []string
				for _, id := range []string{"root", "data-0", "data-1"} {
					paths = append(paths, filepath.Join(original.RelativeDir, "diffs", id+".diff"))
				}
				if scenario == "historical-enabled" {
					storeOpts := diffstore.Options{}
					if encrypted {
						key, _ := opts.CustomerKeyFn()
						storeOpts.Key = &key
					}
					store, err := diffstore.Open(paths[0], storeOpts)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := store.WriteAt(bytes.Repeat([]byte{0x75}, 4096), 0); err != nil {
						t.Fatal(err)
					}
					if err := store.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "cross-enabled" || scenario == "cross-disabled" {
					opts.BaseRoot = t.TempDir()
				}
				opts.EnableCheckpoint = scenario == "cross-enabled" || scenario == "historical-enabled"
				opts, source, release, err := prepareRestoreCheckpoint(ctx, opts, original)
				if err != nil {
					t.Fatal(err)
				}
				defer release()
				if source.Root.Checkpoint != nil || len(opts.checkpointReopen) != 0 || opts.checkpointReleaseSources != nil {
					t.Fatal("materialization retained same-directory handoff")
				}
				assertRestoreReaders(t, paths, false)
				got := make([]byte, len(want))
				if _, err := source.Root.Payload.ReadAt(ctx, got, 0); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("materialized root depends on released source: %v", err)
				}
				opts.ArtifactRelativeDir = source.RelativeDir
				for _, disk := range source.Root.Portable.Boot.Disks {
					stream, err := openDiskRefStream(ctx, disk.Base, opts)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := stream.ReadAt(ctx, got, 0); err != nil || !bytes.Equal(got, want) {
						t.Fatalf("materialized data disk depends on released source: %v", err)
					}
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
				}
				ref, err := manifest.ParseRef(source.SelectedRef)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(ref.Path); err != nil {
					t.Fatal("materialized file prematurely removed", err)
				}
				if err := release(); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(filepath.Dir(ref.Path)); !os.IsNotExist(err) {
					t.Fatal("materialization directory leaked", err)
				}
			})
		}
	}
}

func TestRestoreCheckpointPartialPinFailureClosesSources(t *testing.T) {
	opts, source, _ := restoreCheckpointDiskFixture(t, false, 2)
	paths := []string{filepath.Join(source.RelativeDir, "diffs", "root.diff"), filepath.Join(source.RelativeDir, "diffs", "data-0.diff")}
	source.Root.Checkpoint.Disks[1].DiffID = "missing"
	if _, _, _, err := prepareRestoreCheckpoint(context.Background(), opts, source); err == nil {
		t.Fatal("missing final source accepted")
	}
	assertRestoreReaders(t, paths, false)
}

func TestRestoreCheckpointFailedMaterializationClosesSources(t *testing.T) {
	opts, source, _ := restoreCheckpointDiskFixture(t, false, 2)
	var paths []string
	for _, id := range []string{"root", "data-0", "data-1"} {
		paths = append(paths, filepath.Join(source.RelativeDir, "diffs", id+".diff"))
	}
	opts.BaseRoot = t.TempDir()
	// Pinning uses the already opened metadata. Force publication to fail only
	// after all pins, the target owner and the temporary directory exist.
	source.SelectedRef = "file://" + filepath.Join(t.TempDir(), "missing.sandbox")
	if _, _, _, err := prepareRestoreCheckpoint(context.Background(), opts, source); err == nil || !strings.Contains(err.Error(), "materialize Checkpoint") {
		t.Fatalf("expected materialization failure: %v", err)
	}
	assertRestoreReaders(t, paths, false)
	directory := filepath.Join(opts.BaseRoot, opts.PathID, "checkpoint")
	owner, err := diffstore.LockDirectory(directory)
	if err != nil {
		t.Fatal("failed materialization leaked target owner", err)
	}
	owner.Close()
	material, err := filepath.Glob(filepath.Join(opts.BaseRoot, opts.PathID, ".checkpoint-restore-*"))
	if err != nil || len(material) != 0 {
		t.Fatalf("failed materialization leaked files: %v %v", material, err)
	}
}
