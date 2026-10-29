package snapshot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/bundle"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
)

type dirtyObjectKey struct {
	partition store.Partition
	key       store.ContentKey
}
type dirtyObjectStore struct {
	mu      sync.Mutex
	objects map[dirtyObjectKey][]byte
}

func (*dirtyObjectStore) AdmitWrite(context.Context) (store.WriteAdmission, error) {
	salt, err := store.SaltForGeneration("NONE")
	return store.WriteAdmission{Generation: "NONE", Salt: salt}, err
}
func (s *dirtyObjectStore) Put(_ context.Context, _ store.WriteAdmission, p store.Partition, k store.ContentKey, b []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[dirtyObjectKey{p, k}] = bytes.Clone(b)
	return true, nil
}
func (s *dirtyObjectStore) Get(_ context.Context, p store.Partition, k store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.objects[dirtyObjectKey{p, k}]
	if !ok {
		return cache.CacheMiss, nil, nil
	}
	return cache.CacheHit, cache.NewMemBlob(b), nil
}

func TestDirtyMemoryFileBundleIngestRoundTrip(t *testing.T) {
	ctx := context.Background()
	key := func() ([32]byte, error) { return [32]byte{42}, nil }
	cfg := &manifest.Config{Chunker: chunker.Config{Mode: "fixed", Fixed: chunker.FixedConfig{Size: "4KiB"}}, Crypto: manifestcrypto.Config{Chunk: "aes", Manifest: "aes"}}
	_, dec, err := manifestcrypto.New(cfg.Crypto)
	if err != nil {
		t.Fatal(err)
	}
	for _, carrier := range []string{"file", "bundle", "ingest"} {
		t.Run(carrier, func(t *testing.T) {
			identities := map[string]bool{}
			for _, variant := range []string{"absent", "all-clean", "mixed", "changed-content"} {
				t.Run(variant, func(t *testing.T) {
					dir := t.TempDir()
					values := []byte{9, 0, 7, 0}
					if variant == "changed-content" {
						values[0] = 11
					}
					memory := pageMemory([]sparse.RunKind{sparse.Data, sparse.Zero, sparse.Data, sparse.Hole}, values)
					var bitmap []byte
					if variant != "absent" {
						bits := byte(0)
						if variant != "all-clean" {
							bits = 5
						}
						bitmap, err = snapshotfile.EncodeDirty(memory.Size(), []byte{bits})
						if err != nil {
							t.Fatal(err)
						}
					}
					source, err := snapshotfile.BuildSourceWithDirty(memory, []byte("{}"), []byte("{}"), []byte("version: 1\nsandbox_ref: manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"), bitmap)
					if err != nil {
						t.Fatal(err)
					}
					objects := &dirtyObjectStore{objects: make(map[dirtyObjectKey][]byte)}
					var sink ArtifactSink
					switch carrier {
					case "file":
						sink = NewFileSink(dir, "dirty", nil, false, nil)
					case "bundle":
						sink, err = NewBundleSink(ctx, dir, "dirty", cfg, key, nil)
					case "ingest":
						writer, e := cfg.NewIngesterWithWriter(key, nil, objects)
						err = e
						if e == nil {
							sink = NewIngestSink(writer, nil)
						}
					}
					if err != nil {
						t.Fatal(err)
					}
					defer sink.Close()
					ref, path, err := sink.AbsorbSnapshot(ctx, source)
					if err != nil {
						t.Fatal(err)
					}
					if identities[ref] {
						t.Fatal("changing only bitmap/content reused artifact identity", ref)
					}
					identities[ref] = true
					if err = sink.CommitSnapshot(ctx, ref, path); err != nil {
						t.Fatal(err)
					}
					var stream fetch.Stream
					if carrier == "file" {
						stream, err = fetch.OpenTarStream(path)
					} else {
						manifestKey, e := manifest.ParseKeyRef(ref)
						if e != nil {
							t.Fatal(e)
						}
						var getter cache.Getter = objects
						if carrier == "bundle" {
							reader, e := bundle.Open(filepath.Join(dir, manifest.HexKey(manifestKey)+".bundle"))
							if e != nil {
								t.Fatal(e)
							}
							defer reader.Close()
							getter = reader.Getter()
							if err = reader.FullVerify(ctx, manifestKey, [32]byte{42}, dec, bundle.VerifyOptions{ExpectedManifests: reader.ManifestKeys()}); err != nil {
								t.Fatal(err)
							}
						}
						stream, err = fetch.NewFetcher([32]byte{42}, getter, dec).OpenManifest(ctx, manifestKey)
					}
					if err != nil {
						t.Fatal(err)
					}
					root, err := snapshotfile.Open(ctx, stream)
					if err != nil {
						t.Fatal(err)
					}
					defer root.Close()
					if (root.MemoryDirty == nil) != (bitmap == nil) || !bytes.Equal(root.MemoryDirty, bitmap) {
						t.Fatal("dirty presence/body changed")
					}
					for i, kind := range []sparse.RunKind{sparse.Data, sparse.Zero, sparse.Data, sparse.Hole} {
						run, e := root.Memory.RunAt(uint64(i)*4096, 4096)
						if e != nil || run.Kind() != kind {
							t.Fatalf("page %d kind: %v %v", i, run, e)
						}
					}
					base := pageMemory([]sparse.RunKind{sparse.Data, sparse.Data, sparse.Data, sparse.Data}, []byte{9, 0, 7, 0x77})
					full := fetch.NewLayered(root.Memory, memoryBorrow{base})
					got := make([]byte, 4*4096)
					want := make([]byte, len(got))
					for i, value := range []byte{values[0], 0, 7, 0x77} {
						copy(want[i*4096:], bytes.Repeat([]byte{value}, 4096))
					}
					if _, err = full.ReadAt(ctx, got, 0); err != nil || !bytes.Equal(got, want) {
						t.Fatal("complete logical restore differs", err)
					}
					t.Logf("%s %s full bytes=%d dirty=%d identity=%s", carrier, variant, len(got), len(bitmap), ref)
				})
			}
			if len(identities) != 4 {
				t.Fatal(fmt.Sprintf("identity variants=%d", len(identities)))
			}
		})
	}
}

func TestMemoryFallbackArtifactRolesThroughFileCarrier(t *testing.T) {
	ctx := context.Background()
	h := sparse.Hole
	d := sparse.Data
	z := sparse.Zero
	history := pageMemory([]sparse.RunKind{h, h, d, z, d, d, h, h}, []byte{0, 0, 2, 0, 4, 5, 0, 0})
	selected := pageMemory([]sparse.RunKind{h, h, d, h, d, h, h, h}, []byte{0, 0, 2, 0, 4, 0, 0, 0})
	current := pageMemory([]sparse.RunKind{d, h, h, h, h, h, z, h}, []byte{9, 0, 0, 0, 0, 0, 0, 0})
	working := pageMemory([]sparse.RunKind{d, d, h, h, h, h, z, h}, []byte{9, 1, 0, 0, 0, 0, 0, 0})
	for _, role := range []string{"true-self", "false-H", "false-self"} {
		t.Run(role, func(t *testing.T) {
			var required, coverage, values sparse.Source = selected, history, history
			limit := uint64(3)
			expected := []sparse.RunKind{h, h, d, d, d, d, h, h}
			if role == "true-self" {
				required = fetch.NewLayered(memoryBorrow{current}, memoryBorrow{selected})
				coverage = fetch.NewLayered(memoryBorrow{DataCoverage(working)}, memoryBorrow{history})
				values = fetch.NewLayered(memoryBorrow{working}, memoryBorrow{history})
				expected = []sparse.RunKind{d, d, d, d, d, d, z, h}
				limit = 4
			} else if role == "false-self" {
				// W already has every M byte. Converting its required Zero outside M
				// cannot reduce fragmentation without expanding P; reject such a limit.
				required = pageMemory([]sparse.RunKind{d, h, d, h, d, h, z, h}, []byte{9, 0, 2, 0, 4, 0, 0, 0})
				coverage = DataCoverage(required)
				values = required
			}
			sink := NewFileSink(t.TempDir(), role, nil, false, nil)
			defer sink.Close()
			source, stats, err := buildCaptureSource(ctx, sink, required, nil, []byte("{}"), []byte("{}"), []byte("{}"), coverage, values, nil, nil, limit, memoryIndexBudget)
			if role == "false-self" {
				if err == nil || !strings.Contains(err.Error(), "still exceeds") {
					t.Fatalf("expanded false W to escape budget: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			wantExtra := uint64(2 * 4096)
			if role == "true-self" {
				wantExtra = 3 * 4096
			}
			if stats.Fallback == "" || stats.ExtraDataBytes != wantExtra {
				t.Fatalf("fallback costs %+v", stats)
			}
			ref, path, err := sink.AbsorbSnapshot(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			if err = sink.CommitSnapshot(ctx, ref, path); err != nil {
				t.Fatal(err)
			}
			stream, err := fetch.OpenTarStream(path)
			if err != nil {
				t.Fatal(err)
			}
			root, err := snapshotfile.Open(ctx, stream)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			if root.MemoryDirty != nil {
				t.Fatal("extra fallback Data is not all dirty")
			}
			for page, kind := range expected {
				r, e := root.Memory.RunAt(uint64(page)*4096, 4096)
				if e != nil || r.Kind() != kind {
					t.Fatalf("%s page %d: %v %v", role, page, r, e)
				}
			}
			// The retained external page is supplied only by E on both sides.
			external := pageMemory([]sparse.RunKind{h, h, h, h, h, h, h, d}, []byte{0, 0, 0, 0, 0, 0, 0, 77})
			actual := fetch.NewLayered(root.Memory, memoryBorrow{external})
			wantSource := fetch.NewLayered(memoryBorrow{values}, memoryBorrow{external})
			got, want := make([]byte, 8*4096), make([]byte, 8*4096)
			if _, err = actual.ReadAt(ctx, got, 0); err != nil {
				t.Fatal(err)
			}
			if _, err = wantSource.ReadAt(ctx, want, 0); err != nil || !bytes.Equal(got, want) {
				t.Fatal("complete fallback bytes differ", err)
			}
			t.Logf("role=%s bytes=%d extra_data=%d reason=%s", role, stats.DataBytes, stats.ExtraDataBytes, stats.Fallback)
		})
	}
}

func TestFalseWorkingSetBitmapBudgetFallbackWritesCompleteCarrier(t *testing.T) {
	ctx := context.Background()
	const size = uint64(1 << 40)
	// The logical size creates a 32 MiB bitmap, but only one Data page is owned.
	// Validate the full sparse map plus every explicit byte, without reading a
	// trillion implicit zeros or widening the working-set ownership boundary.
	working := &memorySource{size: size, resolve: func(off, limit uint64) (sparse.Run, error) {
		if off < 4096 {
			return memoryRun{off: off, end: min(off+limit, 4096), kind: sparse.Data, read: func(_ context.Context, b []byte, _ uint64) (int, error) {
				for i := range b {
					b[i] = 0x79
				}
				return len(b), nil
			}}, nil
		}
		return memoryRun{off: off, end: off + limit, kind: sparse.Hole}, nil
	}}
	dirty, err := snapshotfile.EncodeDirty(size, make([]byte, size/4096/8))
	if err != nil {
		t.Fatal(err)
	}
	config, state := bytes.Repeat([]byte{' '}, 16<<20), bytes.Repeat([]byte{' '}, 16<<20)
	copy(config, "{}")
	copy(state, "{}")
	if _, err = snapshotfile.BuildSourceWithDirty(working, config, state, []byte("{}"), dirty); !errors.Is(err, snapshotfile.ErrTailBudget) {
		t.Fatal("ordinary reconstruction removed bitmap", err)
	}
	sink := NewFileSink(t.TempDir(), "false-top", nil, false, nil)
	defer sink.Close()
	source, stats, err := BuildCaptureSource(ctx, sink, working, dirty, config, state, []byte("{}"), working, working, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if stats.DataBytes != 4096 || stats.ExtraDataBytes != 0 || stats.BitmapBytes != 0 || !strings.Contains(stats.Fallback, "bitmap") {
		t.Fatalf("working set changed: %+v", stats)
	}
	ref, path, err := sink.AbsorbSnapshot(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if err = sink.CommitSnapshot(ctx, ref, path); err != nil {
		t.Fatal(err)
	}
	stream, err := fetch.OpenTarStream(path)
	if err != nil {
		t.Fatal(err)
	}
	root, err := snapshotfile.Open(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if root.Memory.Size() != size || root.MemoryDirty != nil || !bytes.Equal(config, root.ConfigJSON) || !bytes.Equal(state, root.StateJSON) {
		t.Fatal("fallback body/geometry changed")
	}
	data, err := root.Memory.RunAt(0, size)
	if err != nil || data.Kind() != sparse.Data || data.End() != 4096 {
		t.Fatal("owned Data map", data, err)
	}
	got := make([]byte, 4096)
	if _, err = data.ReadAt(ctx, got, 0); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{0x79}, 4096)) {
		t.Fatal("owned Data bytes", err)
	}
	hole, err := root.Memory.RunAt(4096, size-4096)
	if err != nil || hole.Kind() != sparse.Hole || hole.End() != size {
		t.Fatal("expanded false working set", hole, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("false top bitmap fallback: logical=%d data=%d bitmap=%d ZIP=%d physical=%d reason=%s", size, stats.DataBytes, stats.BitmapBytes, stats.ZIPBytes, info.Size(), stats.Fallback)
}
