package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
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
