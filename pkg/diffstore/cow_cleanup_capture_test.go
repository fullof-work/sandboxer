package diffstore_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
	"github.com/kuasar-sandbox/sandboxer/pkg/memory"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
)

func TestCacheCleanupCannotHideCaptureFailure(t *testing.T) {
	for _, captureMemory := range []bool{false, true} {
		t.Run(fmt.Sprint(captureMemory), func(t *testing.T) {
			writeRelease, cleanupEntered, cleanupRelease := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var writeOnce, cleanupOnce sync.Once
			openWrite := func() { writeOnce.Do(func() { close(writeRelease) }) }
			openCleanup := func() { cleanupOnce.Do(func() { close(cleanupRelease) }) }
			defer openWrite()
			defer openCleanup()
			fatal := errors.New("write failed before blocked cleanup")
			cache, err := diffstore.NewCOWCacheForTest(4096, 4096, nil,
				func() error { <-writeRelease; return fatal },
				func() error { close(cleanupEntered); <-cleanupRelease; return nil })
			if err != nil {
				t.Fatal(err)
			}
			defer cache.Close()
			cow, err := diffstore.OpenDisk(filepath.Join(t.TempDir(), "diff"), nil, diffstore.DiffInit{CreateSize: 8192}, diffstore.WithCache(cache))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { openWrite(); openCleanup(); _ = cow.Close() }()
			reported := make(chan error, 1)
			cow.SetFatalHandler(func(err error) { reported <- err })
			if _, err := cow.WriteAt([]byte("latest"), 4096); err != nil {
				t.Fatal(err)
			}
			sock, staging := captureAPI(t)
			sink := &failAfterCaptureSink{ArtifactSink: snapshot.NewFileSink(t.TempDir(), "blocked-cleanup", nil, false, nil), fail: func() {
				openWrite() // fail only after every cached payload byte was captured
				select {
				case <-cleanupEntered:
				case <-time.After(10 * time.Second):
					t.Fatal("cleanup did not start")
				}
			}}
			diffs := []snapshot.DiskDiff{{SnapshotView: cow.SnapshotView, CheckError: cow.Err}}
			// Keep the context healthy: final publication must inspect cache health.
			if captureMemory {
				mfd, err := memory.Create("blocked-cleanup", 4096)
				if err != nil {
					t.Fatal(err)
				}
				defer mfd.Close()
				out, err := snapshot.Take(snapshot.Sources{Context: context.Background(), SandboxID: "blocked", APISock: sock, StagingDir: staging, MemfdFD: mfd.FD(), MemfdSize: 4096, PortableConfig: captureConfig(), Diffs: diffs, Quiescer: captureQuiescer{}}, sink, false)
				if out != nil || !errors.Is(err, fatal) {
					t.Fatalf("snapshot published during failed cleanup: %v %v", out, err)
				}
			} else {
				out, err := snapshot.Export(context.Background(), snapshot.ExportSources{SandboxID: "blocked", APISock: sock, PortableConfig: captureConfig(), Diffs: diffs, Quiescer: captureQuiescer{}}, sink, false)
				if out != nil || !errors.Is(err, fatal) {
					t.Fatalf("export published during failed cleanup: %v %v", out, err)
				}
			}
			if sink.commits != 0 {
				t.Fatal("known-failed capture committed a root")
			}
			select {
			case err := <-reported:
				if !errors.Is(err, fatal) {
					t.Fatalf("owner error: %v", err)
				}
			default:
				t.Fatal("owner not notified before cleanup")
			}
			if cache.Stats().DirtyUsed != 4096 || cache.Stats().Writeback != 4096 {
				t.Fatal("cleanup released in-flight page ownership")
			}
		})
	}
}
