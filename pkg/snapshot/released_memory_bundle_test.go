package snapshot

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	manifestbundle "github.com/kuasar-sandbox/accelerator/pkg/manifest/bundle"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
)

// Exercise the actual encrypted Bundle/ingest carrier: released Zero must mask
// a nonzero parent, while an untouched Hole must still inherit that parent.
func TestReleasedMemoryBundlePreservesParentMask(t *testing.T) {
	ctx := context.Background()
	const page = 4096
	dir := t.TempDir()
	f, err := os.CreateTemp(dir, "memory-")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	backing := bytes.Repeat([]byte{0x66}, 4*page)
	if _, err := f.Write(backing); err != nil {
		t.Fatal(err)
	}
	memory := &releasedMemorySource{
		fd: int(f.Fd()), size: uint64(len(backing)),
		zeros: []sparse.Extent{{Offset: page, Size: page}},
		holes: []sparse.Extent{{Offset: 2 * page, Size: page}},
	}
	if n, err := explicitMemoryBytes(memory); err != nil || n != 3*page {
		t.Fatalf("explicit coverage = %d, %v", n, err)
	}
	logical, err := snapshotfile.BuildSource(memory, []byte("{}"), []byte("{}"),
		[]byte("version: 1\nsandbox_ref: manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"))
	if err != nil {
		t.Fatal(err)
	}
	key := [32]byte{0x42}
	cfg := &manifest.Config{
		Chunker: chunker.Config{Mode: "fixed", Fixed: chunker.FixedConfig{Size: "4KiB"}},
		Crypto:  manifestcrypto.Config{Chunk: "aes", Manifest: "aes"},
	}
	sink, err := NewBundleSink(ctx, dir, "released", cfg, func() ([32]byte, error) { return key, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	ref, _, err := sink.AbsorbSnapshot(ctx, logical)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.CommitSnapshot(ctx, ref, ""); err != nil {
		t.Fatal(err)
	}
	rootKey, err := manifest.ParseKeyRef(ref)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := manifestbundle.Open(filepath.Join(dir, manifest.HexKey(rootKey)+".bundle"))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	_, decryptor, err := manifestcrypto.New(cfg.Crypto)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.FullVerify(ctx, rootKey, key, decryptor, manifestbundle.VerifyOptions{}); err != nil {
		t.Fatal(err)
	}
	local := fetch.NewFetcher(key, reader.Getter(), decryptor)
	stream, err := manifestbundle.NewManifestFetcher(reader, local, nil).OpenManifest(ctx, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	root, err := snapshotfile.Open(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	r, err := root.Memory.RunAt(page, page)
	if err != nil || r == nil || r.Kind() == sparse.Hole {
		t.Fatalf("released page inherited parent: %v, %v", r, err)
	}
	r, err = root.Memory.RunAt(2*page, page)
	if err != nil || r == nil || r.Kind() != sparse.Hole {
		t.Fatalf("untouched page lost inheritance: %v, %v", r, err)
	}
	parent := bytes.Repeat([]byte{0x77}, len(backing))
	layered := fetch.NewLayered(root.Memory, &testExportStream{Source: sparse.Dense(bytes.NewReader(parent), uint64(len(parent)))})
	defer layered.Close()
	want := append([]byte(nil), backing...)
	clear(want[page : 2*page])
	copy(want[2*page:3*page], parent[2*page:3*page])
	got := make([]byte, len(want))
	if n, err := layered.ReadAt(ctx, got, 0); err != nil || n != len(got) {
		t.Fatalf("restored memory = %d, %v", n, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("complete restored logical memory differs")
	}
	// Source.Close never owns the live VM's fd.
	if err := memory.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ReadAt(make([]byte, 1), 0); err != nil {
		t.Fatalf("capture closed live memory: %v", err)
	}
}
