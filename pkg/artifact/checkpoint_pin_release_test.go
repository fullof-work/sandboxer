package artifact

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"golang.org/x/sys/unix"
)

// This is a real-backend causal test, not a substitute for the restore/Runtime
// handoff tests: it explicitly closes the same ProcessStorage pin collection
// those entry points own, and observes allocation, reclamation and fixed data.
func TestCheckpointRestorePinReleaseEffects(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		for _, exporters := range []bool{false, true} {
			t.Run(fmt.Sprintf("xts=%v/exporters=%v", encrypted, exporters), func(t *testing.T) {
				f := newCheckpointPinEffectFixture(t, encrypted)
				var beforeRestore, afterRestore []*diffstore.View
				if exporters {
					beforeRestore = f.export(t)
				}
				pins, err := f.storage.PinDiffCheckpoints(context.Background(), f.dir, f.cp)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = pins.Close() })
				if len(pins.Refs) != 3 || len(pins.Disks) != 2 {
					t.Fatal("incomplete source pin set")
				}
				if exporters {
					afterRestore = f.export(t)
				}
				for _, s := range f.stores {
					if err := s.DeleteCheckpoint(context.Background(), f.cp.CheckpointID); err != nil {
						t.Fatal(err)
					}
					if ids, err := s.ListCheckpoints(context.Background()); err != nil || len(ids) != 0 {
						t.Fatal("deleted name remains visible", ids, err)
					}
				}
				// The restore-only case proves that an invisible version still causes
				// CoW solely because the temporary restore readers remain alive.
				f.overwrite(t, "deleted-with-restore-pins", 0, true)
				f.checkViews(t, []*diffstore.View{pins.Root, pins.Disks["data-a"], pins.Disks["data-b"]})
				f.gc(t)
				for _, s := range f.stores {
					if s.Stats().GCPunched != 0 {
						t.Fatal("reclaimed an old page while restoration still pins it")
					}
				}
				if err := pins.Close(); err != nil {
					t.Fatal(err)
				}
				if err := pins.Close(); err != nil {
					t.Fatal("repeat source release", err)
				}
				for _, view := range []*diffstore.View{pins.Root, pins.Disks["data-a"], pins.Disks["data-b"]} {
					if _, err := view.ReadAt(context.Background(), make([]byte, 1), 0); !errors.Is(err, os.ErrClosed) {
						t.Fatal("source reader remains open", err)
					}
				}
				f.checkOwnershipAndAccess(t)
				privateBlock := uint64(1)
				if exporters {
					// Each independent exporter has its own OFD pin. Releasing the
					// restoration collection must not release either exporter's lock.
					f.gc(t)
					f.overwrite(t, "restore-released-two-exporters", 1, true)
					f.checkViews(t, beforeRestore)
					f.checkViews(t, afterRestore)
					closeCheckpointEffectViews(t, beforeRestore)
					f.gc(t)
					f.overwrite(t, "one-exporter-remains", 2, true)
					f.checkViews(t, afterRestore)
					for _, s := range f.stores {
						if s.Stats().GCPunched != 0 {
							t.Fatal("last real exporter lost retention")
						}
					}
					closeCheckpointEffectViews(t, afterRestore)
					privateBlock = 3
				}
				allocatedBeforeGC := f.allocatedBytes(t)
				f.gc(t)
				allocatedAfterGC := f.allocatedBytes(t)
				for i, s := range f.stores {
					if s.Stats().GCPunched < 2*diffstore.PageSize {
						t.Fatalf("disk %d did not physically reclaim the old payload and root: %+v", i, s.Stats())
					}
					t.Logf("PIN_RELEASE_GC disk=%s punched_bytes=%d allocated_before=%d allocated_after=%d", f.ids[i], s.Stats().GCPunched, allocatedBeforeGC[i], allocatedAfterGC[i])
				}
				// This page has not been overwritten since the original checkpoint.
				// Exclude MarkModified's fence before comparing its first overwrite.
				f.overwrite(t, "last-reader-released", privateBlock, false)
				f.checkActive(t)
				// Same format/key and seeded pages, but no retained version at all.
				for i := range f.stores {
					control := f.newStore(t, filepath.Join(t.TempDir(), "control.diff"), f.ids[i]+"-control", false, i)
					assertCheckpointEffectWrite(t, control, "never-checkpointed-control", f.ids[i], privateBlock, false)
				}
			})
		}
	}
}

type checkpointPinEffectFixture struct {
	dir            string
	ids            []string
	cp             *sandboxfile.Checkpoint
	stores         []*diffstore.Store
	want, original [][]byte
	storage        *ProcessStorage
	key            *[32]byte
	keyCalls       *int
	fetcher        *closeCountingFetcher
}

func newCheckpointPinEffectFixture(t *testing.T, encrypted bool) *checkpointPinEffectFixture {
	t.Helper()
	f := &checkpointPinEffectFixture{dir: t.TempDir(), ids: []string{"root", "data-a", "data-b"}, keyCalls: new(int), fetcher: &closeCountingFetcher{}}
	f.cp = &sandboxfile.Checkpoint{Version: 1, CheckpointID: "restore-source", Root: sandboxfile.CheckpointDisk{DiffID: "root"}, Disks: []sandboxfile.CheckpointNamedDisk{{Name: "data-a", DiffID: "data-a"}, {Name: "data-b", DiffID: "data-b"}}}
	owner, err := diffstore.LockDirectory(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	var codec tarstream.Codec
	if encrypted {
		f.key = &[32]byte{0x33, 0x35}
		codec, err = manifestcrypto.NewTarStreamCodec(*f.key)
		if err != nil {
			t.Fatal(err)
		}
	}
	storageKey := [32]byte{0x33, 0x35}
	f.storage, err = NewProcessStorageWithLocalAccess(nil, func() ([32]byte, error) { *f.keyCalls++; return storageKey, nil }, codec, encrypted, f.fetcher)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.storage.Close() })
	for i, id := range f.ids {
		s := f.newStore(t, filepath.Join(f.dir, "diffs", id+".diff"), id, true, i)
		f.stores = append(f.stores, s)
		original := checkpointEffectPayload(i)
		f.original = append(f.original, original)
		f.want = append(f.want, append([]byte(nil), original...))
	}
	return f
}

func checkpointEffectPayload(disk int) []byte {
	payload := make([]byte, 32*diffstore.PageSize)
	for block := 0; block < 32; block++ {
		copy(payload[block*diffstore.PageSize:], bytes.Repeat([]byte{byte(16*disk + block)}, diffstore.PageSize))
	}
	return payload
}

func (f *checkpointPinEffectFixture) newStore(t *testing.T, path, id string, checkpoint bool, disk int) *diffstore.Store {
	t.Helper()
	s, err := diffstore.Create(path, diffstore.Options{DiffID: id, BaseID: "pin-release-base", LogicalSize: 32 * diffstore.PageSize, Key: f.key, RequireEncryption: f.key != nil})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.WriteAt(checkpointEffectPayload(disk), 0); err != nil {
		t.Fatal(err)
	}
	if checkpoint {
		if err := s.Checkpoint(context.Background(), f.cp.CheckpointID); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func (f *checkpointPinEffectFixture) export(t *testing.T) []*diffstore.View {
	t.Helper()
	var views []*diffstore.View
	for _, s := range f.stores {
		v, err := s.Export(context.Background(), f.cp.CheckpointID)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = v.Close() })
		views = append(views, v)
	}
	return views
}

func closeCheckpointEffectViews(t *testing.T, views []*diffstore.View) {
	t.Helper()
	for _, v := range views {
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func (f *checkpointPinEffectFixture) checkViews(t *testing.T, views []*diffstore.View) {
	t.Helper()
	for i, v := range views {
		got := make([]byte, v.Size())
		if n, err := v.ReadAt(context.Background(), got, 0); err != nil || n != len(got) || !bytes.Equal(got, f.original[i]) {
			t.Fatalf("fixed exporter %s changed: n=%d err=%v", f.ids[i], n, err)
		}
	}
}

func (f *checkpointPinEffectFixture) checkActive(t *testing.T) {
	t.Helper()
	for i, s := range f.stores {
		got := make([]byte, s.Size())
		if n, err := s.ReadAt(got, 0); err != nil || n != len(got) || !bytes.Equal(got, f.want[i]) {
			t.Fatalf("active disk %s changed: n=%d err=%v", f.ids[i], n, err)
		}
	}
}

func (f *checkpointPinEffectFixture) overwrite(t *testing.T, phase string, block uint64, shared bool) {
	t.Helper()
	for i, s := range f.stores {
		assertCheckpointEffectWrite(t, s, phase, f.ids[i], block, shared)
		copy(f.want[i][int(block)*diffstore.PageSize:], bytes.Repeat([]byte{0xe0 + byte(block)}, diffstore.PageSize))
	}
}

func assertCheckpointEffectWrite(t *testing.T, s *diffstore.Store, phase, disk string, block uint64, shared bool) {
	t.Helper()
	if err := s.MarkModified(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := s.Stats()
	if _, err := s.WriteAt(bytes.Repeat([]byte{0xe0 + byte(block)}, diffstore.PageSize), int64(block)*diffstore.PageSize); err != nil {
		t.Fatal(err)
	}
	after := s.Stats()
	allocated, index, redo := after.Allocated-before.Allocated, after.IndexWritten-before.IndexWritten, after.RedoWritten-before.RedoWritten
	if shared {
		if allocated == 0 || index == 0 || redo == 0 {
			t.Fatalf("%s %s lost CoW protection: %+v -> %+v", phase, disk, before, after)
		}
	} else if allocated != 0 || index != 0 || redo != 0 {
		t.Fatalf("%s %s old-page first overwrite remains shared: %+v -> %+v", phase, disk, before, after)
	}
	if after.DataWritten-before.DataWritten != diffstore.PageSize {
		t.Fatal("unexpected payload write size")
	}
	t.Logf("PIN_RELEASE_WRITE phase=%s disk=%s allocated=%d index_bytes=%d redo_bytes=%d data_bytes=%d gc_bytes=%d", phase, disk, allocated, index, redo, after.DataWritten-before.DataWritten, after.GCWritten-before.GCWritten)
}

func (f *checkpointPinEffectFixture) gc(t *testing.T) {
	t.Helper()
	for _, s := range f.stores {
		// Two successive zero-progress calls handle a version transition followed
		// by its garbage list without relying on the background ticker or sleep.
		idle := 0
		for step := 0; idle < 2; step++ {
			if step == 64 {
				t.Fatal("bounded small-fixture GC did not settle")
			}
			n, err := s.GC(context.Background(), diffstore.MaxBatchPages)
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				idle++
			} else {
				idle = 0
			}
		}
	}
}

func (f *checkpointPinEffectFixture) allocatedBytes(t *testing.T) []int64 {
	t.Helper()
	result := make([]int64, len(f.ids))
	for i, id := range f.ids {
		var stat unix.Stat_t
		if err := unix.Stat(filepath.Join(f.dir, "diffs", id+".diff"), &stat); err != nil {
			t.Fatal(err)
		}
		result[i] = stat.Blocks * 512
	}
	return result
}

func (f *checkpointPinEffectFixture) checkOwnershipAndAccess(t *testing.T) {
	t.Helper()
	other, err := diffstore.LockDirectory(f.dir)
	if !errors.Is(err, diffstore.ErrBusy) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatal("source release also released target owner", err)
	}
	if f.storage.Fetcher() != f.fetcher || f.fetcher.closes != 0 {
		t.Fatal("source release closed borrowed storage access")
	}
	key, err := f.storage.CustomerKeyFunc()()
	if err != nil || key != [32]byte{0x33, 0x35} || *f.keyCalls != 1 {
		t.Fatal("source release changed customer-key capability", err, *f.keyCalls)
	}
	if f.storage.LocalRequired() != (f.key != nil) {
		t.Fatal("source release changed local crypto policy")
	}
}
