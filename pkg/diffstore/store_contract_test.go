package diffstore_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
)

// These callers cannot import the private versioned engine. Every public entry
// point must preserve errors.Is identity for missing and incorrect v2 keys.
func TestV2PublicEncryptionErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "encrypted.diff")
	key, wrong := [32]byte{41}, [32]byte{42}
	store, err := diffstore.Create(path, diffstore.Options{
		DiffID: "auth-contract", LogicalSize: diffstore.PageSize, Key: &key,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		key  *[32]byte
		want error
	}{
		{"missing", nil, diffstore.ErrEncryptionRequired},
		{"incorrect", &wrong, diffstore.ErrAuthentication},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Run("Open", func(t *testing.T) {
				handle, err := diffstore.Open(path, diffstore.Options{Key: test.key})
				if handle != nil {
					defer handle.Close()
				}
				if !errors.Is(err, test.want) {
					t.Fatalf("got %v, want %v", err, test.want)
				}
			})
			t.Run("OpenExport", func(t *testing.T) {
				handle, err := diffstore.OpenExport(context.Background(), path, "", test.key)
				if handle != nil {
					defer handle.Close()
				}
				if !errors.Is(err, test.want) {
					t.Fatalf("got %v, want %v", err, test.want)
				}
			})
			t.Run("OpenDisk", func(t *testing.T) {
				options := []diffstore.DiskOption{diffstore.WithCheckpoint("auth-contract", "")}
				if test.key != nil {
					options = append(options, diffstore.WithDiffEncryption(*test.key, true))
				}
				handle, err := diffstore.OpenDisk(path, nil, diffstore.DiffInit{Existing: true}, options...)
				if handle != nil {
					defer handle.Close()
				}
				if !errors.Is(err, test.want) {
					t.Fatalf("got %v, want %v", err, test.want)
				}
			})
		})
	}
}
