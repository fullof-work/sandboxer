package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
)

func TestDiskMergePreservesZeroAndBoundsParent(t *testing.T) {
	ctx := context.Background()
	for _, checkpoint := range []bool{false, true} {
		for _, encrypted := range []bool{false, true} {
			t.Run(fmt.Sprintf("checkpoint=%t/encrypted=%t", checkpoint, encrypted), func(t *testing.T) {
				var options []diffstore.DiskOption
				if checkpoint {
					options = append(options, diffstore.WithCheckpoint("merge", ""))
				}
				if encrypted {
					options = append(options, diffstore.WithDiffEncryption([32]byte{7}, true))
				}
				top, err := diffstore.OpenDisk(filepath.Join(t.TempDir(), "top"), nil, diffstore.DiffInit{CreateSize: 4 * 4096}, options...)
				if err != nil {
					t.Fatal(err)
				}
				defer top.Close()
				if err := top.Discard(0, 4096); err != nil {
					t.Fatal(err)
				}
				if _, err := top.WriteAt(bytes.Repeat([]byte{0x91}, 4096), 8192); err != nil {
					t.Fatal(err)
				}
				parent, err := diffstore.OpenDisk(filepath.Join(t.TempDir(), "parent"), nil, diffstore.DiffInit{CreateSize: 5 * 4096})
				if err != nil {
					t.Fatal(err)
				}
				defer parent.Close()
				if _, err := parent.WriteAt(bytes.Repeat([]byte{0x67}, 5*4096), 0); err != nil {
					t.Fatal(err)
				}
				if err := parent.Discard(4096, 4096); err != nil {
					t.Fatal(err)
				}
				view, close, err := prepareDiskSource(ctx, DiskDiff{MergeBase: "parent", Export: func(ctx context.Context) (ExportView, error) {
					return top.Export(ctx, diffstore.ActiveDiff)
				}}, nil, false, func(ctx context.Context, _ string) (fetch.Stream, error) {
					return parent.Export(ctx, diffstore.ActiveDiff)
				})
				if err != nil {
					t.Fatal(err)
				}
				defer close()
				if view.Size() != 4*4096 {
					t.Fatal("parent tail entered disk", view.Size())
				}
				for i, want := range []sparse.RunKind{sparse.Zero, sparse.Zero, sparse.Data, sparse.Data} {
					r, err := view.RunAt(uint64(i*4096), 4096)
					if err != nil || r.Kind() != want {
						t.Fatal(i, r, err)
					}
				}
				var artifact bytes.Buffer
				_, digest, err := tarstream.WriteTo(ctx, &artifact, "disk", view)
				if err != nil {
					t.Fatal(err)
				}
				reopened, _, err := tarstream.SourceFrom(bytes.NewReader(artifact.Bytes()), "", tarstream.WithExpectedDigest(tarstream.DigestScheme, digest))
				if err != nil {
					t.Fatal(err)
				}
				want := make([]byte, 4*4096)
				copy(want[8192:], bytes.Repeat([]byte{0x91}, 4096))
				copy(want[12288:], bytes.Repeat([]byte{0x67}, 4096))
				got := make([]byte, len(want))
				if _, err := reopened.ReadAt(ctx, got, 0); err != nil || !bytes.Equal(got, want) {
					t.Fatal("merged carrier contents", err)
				}
				r, err := reopened.RunAt(0, 8192)
				// Storage supplies Zero above. A carrier can preserve it or store
				// explicit zero Data; either representation must mask the base.
				// Payload omission is verified in the carrier's own package.
				if err != nil || (r.Kind() != sparse.Zero && r.Kind() != sparse.Data) || r.End() != 8192 {
					t.Fatal("carrier lost the lower-layer zero mask", r, err)
				}
			})
		}
	}
}
