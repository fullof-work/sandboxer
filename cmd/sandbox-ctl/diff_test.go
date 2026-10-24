package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
)

func TestCreateProvisioningDiff(t *testing.T) {
	ctx := context.Background()
	for _, filesystem := range []string{"ext4", "none"} {
		t.Run(filesystem, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "new.diff")
			const size = 32 << 20
			if err := createProvisioningDiff(ctx, path, size, filesystem, "", false); err != nil {
				t.Fatal(err)
			}
			if capacity, err := diffstore.DiffSourceCapacity(path, true); err != nil || capacity != size {
				t.Fatal(capacity, err)
			}
			d, err := diffstore.OpenDisk(path, nil, diffstore.DiffInit{Existing: true})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if filesystem == "ext4" {
				if err := diffstore.ValidateExistingDiffExt4(ctx, path, nil); err != nil {
					t.Fatal(err)
				}
				if d.DirtyCount() == 0 || d.DirtyCount() >= size/4096 {
					t.Fatal("filesystem did not remain sparse", d.DirtyCount())
				}
			} else {
				v, err := d.Export(ctx, diffstore.ActiveDiff)
				if err != nil {
					t.Fatal(err)
				}
				defer v.Close()
				r, err := v.RunAt(0, size)
				if err != nil || r.Kind() != sparse.Hole || r.End() != size {
					t.Fatal(r, err)
				}
			}
		})
	}
}

func TestCreateProvisioningDiffFailuresPreserveOutput(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "existing")
	want := []byte("do not replace old or unknown files")
	if err := os.WriteFile(path, want, 0600); err != nil {
		t.Fatal(err)
	}
	if err := createProvisioningDiff(context.Background(), path, 32<<20, "ext4", "", false); err == nil {
		t.Fatal("replaced existing output")
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal(got, err)
	}
	for _, size := range []int64{0, 4097, diffstore.MaxWritableSize + 4096} {
		if err := createProvisioningDiff(context.Background(), filepath.Join(dir, "bad"), size, "none", "", false); err == nil {
			t.Fatal("invalid capacity accepted", size)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := createProvisioningDiff(ctx, filepath.Join(dir, "cancelled"), 4096, "none", "", false); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := createProvisioningDiff(context.Background(), filepath.Join(dir, "failed"), 32<<20, "ext4", filepath.Join(dir, "missing-seed"), false); err == nil {
		t.Fatal("missing seed accepted")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "existing" {
		t.Fatal("failed preparation leaked resources", entries, err)
	}
}
