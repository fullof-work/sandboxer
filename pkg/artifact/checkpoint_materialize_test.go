package artifact

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/sandboxer/internal/diffstore"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
)

func TestCheckpointBundleSnapshotPublishesOrdinaryArtifacts(t *testing.T) {
	for _, external := range []bool{false, true} {
		t.Run(map[bool]string{false: "contained", true: "external"}[external], func(t *testing.T) {
			ctx := context.Background()
			f := newDiffCheckpointFixture(t)
			cfg, storage := manifestPublisherFixture(t)
			admission, err := cfg.WriteAdmission(ctx)
			if err != nil {
				t.Fatal(err)
			}
			bundleDir := f.dir
			if external {
				bundleDir = t.TempDir()
			}
			sink, err := snapshot.NewPlannedBundleSink(bundleDir, "bundle", cfg, storage.CustomerKeyFunc(), admission, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			cpSource, err := sandboxfile.BuildCheckpointSource(ctx, f.cfg, f.cp)
			if err != nil {
				t.Fatal(err)
			}
			e, _, err := sink.AbsorbSandbox(ctx, cpSource)
			if err != nil {
				t.Fatal(err)
			}
			if external {
				ref, _ := manifest.ParseRef(f.ref)
				ref.Location = "source"
				e = ref.String()
			}
			raw, err := snapshot.MarshalConfig(&snapshot.Config{Version: snapshot.SnapshotConfigVersion, SandboxRef: e})
			if err != nil {
				t.Fatal(err)
			}
			ss, err := snapshotfile.BuildSource(publishSource(t, bytes.Repeat([]byte{0x77}, 8192)), []byte("{}"), []byte("{}"), raw)
			if err != nil {
				t.Fatal(err)
			}
			sref, _, err := sink.AbsorbSnapshot(ctx, ss)
			if err != nil {
				t.Fatal(err)
			}
			if err := sink.CommitSnapshot(ctx, sref, ""); err != nil {
				t.Fatal(err)
			}
			if err := sink.Close(); err != nil {
				t.Fatal(err)
			}
			key, err := manifest.ParseKeyRef(sref)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(bundleDir, manifest.HexKey(key)+".bundle")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, store := range f.stores {
				if _, err := store.WriteAt(bytes.Repeat([]byte{0xaa}, 4096), 0); err != nil {
					t.Fatal(err)
				}
			}
			for _, rewrite := range []bool{false, true} {
				out := t.TempDir()
				p, err := NewLocationPublisher(storage, "published", out, config.RefLocations{"source": f.dir}, nil)
				if err != nil {
					t.Fatal(err)
				}
				var result PublishResult
				if rewrite {
					result, err = p.PublishWithOptions(ctx, path, RewriteOptions{ReduceAny: true})
				} else {
					result, err = p.Publish(ctx, path)
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := p.Close(); err != nil {
					t.Fatal(err)
				}
				ref, err := manifest.ParseRef(result.SandboxRef)
				if err != nil {
					t.Fatal(err)
				}
				stream, err := storage.OpenFileWithLocations(ctx, filepath.Join(out, ref.Path), ref, config.RefLocations{"published": out})
				if err != nil {
					t.Fatal(err)
				}
				root, err := sandboxfile.Open(ctx, stream)
				if err != nil {
					t.Fatal(err)
				}
				if root.Checkpoint != nil {
					t.Fatal("Bundle publication retained checkpoint.json")
				}
				buf := make([]byte, 4096)
				if _, err := root.Payload.ReadAt(ctx, buf, 0); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(buf, bytes.Repeat([]byte{1}, 4096)) {
					t.Fatal("Bundle publication lost pinned data")
				}
				if err := root.Close(); err != nil {
					t.Fatal(err)
				}
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("source Bundle changed")
			}
		})
	}
}

func TestCleanupCheckpointKeepsOnlyCurrentDiskVersions(t *testing.T) {
	ctx := context.Background()
	f := newDiffCheckpointFixture(t)
	storage, _ := NewProcessStorage(nil)
	defer storage.Close()
	for _, store := range f.stores {
		if err := store.Checkpoint(ctx, "obsolete"); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
	old, err := diffstore.Create(filepath.Join(f.dir, "diffs", "orphan.diff"), diffstore.Options{DiffID: "orphan", BaseID: "old", LogicalSize: 8192})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.WriteAt(make([]byte, 4096), 0); err != nil {
		t.Fatal(err)
	}
	if err := old.Checkpoint(ctx, "orphan-version"); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := storage.CleanupCheckpoint(ctx, Checkpoint{Directory: f.dir, SandboxID: "fixture", SandboxRef: f.ref}); err != nil {
		t.Fatal(err)
	}
	refs, err := storage.ListDiffCheckpoints(ctx, f.dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 3 {
		t.Fatalf("retained versions %v", refs)
	}
	for _, ref := range refs {
		if ref.ID != f.cp.CheckpointID {
			t.Fatalf("kept obsolete version %v", ref)
		}
	}
	if _, err := os.Stat(filepath.Join(f.dir, "diffs", "orphan.diff")); !os.IsNotExist(err) {
		t.Fatalf("orphan DIFF not retired: %v", err)
	}
}

func TestDiffCheckpointBaseIdentityIgnoresCarrierLocation(t *testing.T) {
	key := publishTestSHA
	a := config.PortableRootConfig{Base: "manifest://" + key}
	b := config.PortableRootConfig{Base: "file://elsewhere.bundle@manifest:" + key}
	if DiffCheckpointBaseID(a) != DiffCheckpointBaseID(b) {
		t.Fatal("same manifest under another carrier changed base identity")
	}
	c := config.PortableRootConfig{Base: "file://base.overlay@digest:" + key}
	d := c
	d.Base = "file://different.overlay@digest:" + key
	if DiffCheckpointBaseID(c) != DiffCheckpointBaseID(d) {
		t.Fatal("same immutable payload under another filename changed identity")
	}
	if DiffCheckpointBaseID(a) == DiffCheckpointBaseID(c) {
		t.Fatal("distinct identity algorithms conflated")
	}
}

func TestCleanupCheckpointValidatesEveryDIFFIdentityBeforeMutation(t *testing.T) {
	ctx := context.Background()
	f := newDiffCheckpointFixture(t)
	for _, s := range f.stores {
		if err := s.Checkpoint(ctx, "obsolete"); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.owner.Close(); err != nil {
		t.Fatal(err)
	}
	wrong, err := diffstore.Create(filepath.Join(f.dir, "diffs", "wrong.diff"), diffstore.Options{DiffID: "different", LogicalSize: 8192})
	if err != nil {
		t.Fatal(err)
	}
	if err := wrong.Close(); err != nil {
		t.Fatal(err)
	}
	storage, _ := NewProcessStorage(nil)
	defer storage.Close()
	if err := storage.CleanupCheckpoint(ctx, Checkpoint{Directory: f.dir, SandboxID: "fixture", SandboxRef: f.ref}); err == nil {
		t.Fatal("cleanup accepted a mismatched DIFF")
	}
	for _, id := range []string{"root-1", "data-1", "data-2"} {
		s, err := diffstore.OpenMetadata(filepath.Join(f.dir, "diffs", id+".diff"))
		if err != nil {
			t.Fatal(err)
		}
		ids, err := s.ListCheckpoints(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(ids) != 2 {
			t.Fatalf("cleanup mutated before validating complete keep plan: %v", ids)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCleanupRetiredDIFFKeepsRetryUntilExistingReaderCloses(t *testing.T) {
	ctx := context.Background()
	f := newDiffCheckpointFixture(t)
	for _, s := range f.stores {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.owner.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.dir, "diffs", "orphan.diff")
	s, err := diffstore.Create(path, diffstore.Options{DiffID: "orphan", LogicalSize: 8192})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteAt(bytes.Repeat([]byte{0x45}, 4096), 0); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(ctx, "held"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	view, err := diffstore.OpenExport(ctx, path, "held", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	storage, _ := NewProcessStorage(nil)
	defer storage.Close()
	current := Checkpoint{Directory: f.dir, SandboxID: "fixture", SandboxRef: f.ref}
	if err := storage.CleanupCheckpoint(ctx, current); !errors.Is(err, diffstore.ErrBusy) {
		t.Fatalf("retire lost retry ownership: %v", err)
	}
	b := make([]byte, 4096)
	if _, err := view.ReadAt(ctx, b, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, bytes.Repeat([]byte{0x45}, 4096)) {
		t.Fatal("retirement damaged existing reader")
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if err := storage.CleanupCheckpoint(ctx, current); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("retired DIFF still exists after retry: %v", err)
	}
}
