package artifact

import (
	"context"
	"encoding/hex"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestProcessStorageFixesCustomerKey(t *testing.T) {
	first := [32]byte{1, 2, 3}
	second := [32]byte{9, 8, 7}
	t.Setenv("MANIFEST_KEY", hex.EncodeToString(first[:]))
	storage, err := NewProcessStorage(&config.ManifestConfig{Crypto: manifestcrypto.Config{Local: manifestcrypto.LocalAuto}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storage.Close() })
	if storage.LocalCodec() == nil || storage.LocalRequired() {
		t.Fatalf("codec=%T required=%v", storage.LocalCodec(), storage.LocalRequired())
	}
	t.Setenv("MANIFEST_KEY", hex.EncodeToString(second[:]))
	got, err := storage.CustomerKeyFunc()()
	if err != nil {
		t.Fatal(err)
	}
	if got != first {
		t.Fatal("customer key changed after process initialization")
	}
}

func TestProcessStorageUsesExplicitCustomerKeyResolver(t *testing.T) {
	t.Setenv("MANIFEST_KEY", strings.Repeat("ff", 32))
	want := [32]byte{1, 2, 3}
	calls := 0
	storage, err := NewProcessStorageWithCustomerKey(&config.ManifestConfig{}, func() ([32]byte, error) {
		calls++
		return want, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	for range 2 {
		got, err := storage.CustomerKeyFunc()()
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("customer key = %x, want %x", got, want)
		}
	}
	if calls != 1 {
		t.Fatalf("customer key resolver calls = %d, want 1", calls)
	}
}

func TestProcessStorageRequiresExplicitCustomerKeyResolver(t *testing.T) {
	if _, err := NewProcessStorageWithCustomerKey(&config.ManifestConfig{}, nil); err == nil {
		t.Fatal("NewProcessStorageWithCustomerKey accepted a nil resolver")
	}
}

func TestProcessStorageOffDefersAndFixesKeyResolution(t *testing.T) {
	t.Setenv("MANIFEST_KEY", "invalid")
	storage, err := NewProcessStorage(&config.ManifestConfig{Crypto: manifestcrypto.Config{Local: manifestcrypto.LocalOff}})
	if err != nil {
		t.Fatal(err)
	}
	if storage.LocalCodec() != nil || storage.LocalRequired() || storage.CustomerKeyFunc() == nil {
		t.Fatalf("keyFn=%v codec=%T required=%v", storage.CustomerKeyFunc() != nil, storage.LocalCodec(), storage.LocalRequired())
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.CustomerKeyFunc()(); err == nil {
		t.Fatal("actual manifest key use accepted invalid key")
	}
	valid := [32]byte{1}
	t.Setenv("MANIFEST_KEY", hex.EncodeToString(valid[:]))
	if _, err := storage.CustomerKeyFunc()(); err == nil {
		t.Fatal("customer key error was not fixed after first resolution")
	}
}

func TestProcessStoragePolicyValidation(t *testing.T) {
	for _, policy := range []manifestcrypto.LocalPolicy{manifestcrypto.LocalAuto, manifestcrypto.LocalRequired} {
		t.Run(string(policy), func(t *testing.T) {
			t.Setenv("MANIFEST_KEY", "")
			cfg := &config.ManifestConfig{Crypto: manifestcrypto.Config{Local: policy}}
			if _, err := NewProcessStorage(cfg); err == nil {
				t.Fatal("key-requiring policy accepted a missing customer key")
			}
		})
	}
	t.Run("invalid", func(t *testing.T) {
		cfg := &config.ManifestConfig{Crypto: manifestcrypto.Config{Local: "sometimes"}}
		if _, err := NewProcessStorage(cfg); err == nil {
			t.Fatal("invalid crypto.local policy was accepted")
		}
	})
	t.Run("default off", func(t *testing.T) {
		t.Setenv("MANIFEST_KEY", "invalid")
		storage, err := NewProcessStorage(&config.ManifestConfig{})
		if err != nil {
			t.Fatal(err)
		}
		if storage.LocalCodec() != nil || storage.LocalRequired() {
			t.Fatalf("codec=%T required=%v", storage.LocalCodec(), storage.LocalRequired())
		}
		if err := storage.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestProcessStorageLocalOnlyCloseDoesNotDialOrResolveKey(t *testing.T) {
	t.Setenv("MANIFEST_KEY", "invalid")
	storage, err := NewProcessStorage(&config.ManifestConfig{
		Store:  manifest.StoreConfig{Endpoint: "not-a-real-endpoint"},
		Crypto: manifestcrypto.Config{Local: manifestcrypto.LocalOff},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.Close(); err != nil {
		t.Fatalf("unused lazy storage close: %v", err)
	}
	if err := storage.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestProcessStorageExplicitLocalAccessWithoutManifest(t *testing.T) {
	key := [32]byte{0x31}
	codec, err := manifestcrypto.NewTarStreamCodec(key)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	storage, err := NewProcessStorageWithLocalAccess(nil, func() ([32]byte, error) { calls++; return key, nil }, codec, true)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	if storage.Fetcher() != nil || storage.LocalCodec() != codec || !storage.LocalRequired() {
		t.Fatal("explicit local access policy changed")
	}
	for range 2 {
		got, err := storage.CustomerKeyFunc()()
		if err != nil || got != key {
			t.Fatalf("customer key: %x %v", got, err)
		}
	}
	if calls != 1 {
		t.Fatalf("key resolved %d times", calls)
	}
	if _, err := NewProcessStorageWithLocalAccess(nil, nil, codec, true); err == nil {
		t.Fatal("codec accepted without key")
	}
	if _, err := NewProcessStorageWithLocalAccess(nil, nil, nil, true); err == nil {
		t.Fatal("required accepted without codec")
	}
}

type closeCountingFetcher struct {
	fetch.Fetcher
	closes int
}

func (f *closeCountingFetcher) Close() error { f.closes++; return nil }

func TestProcessStorageBorrowPreservesAccessWithoutTakingOwnership(t *testing.T) {
	cfg := &config.ManifestConfig{}
	customer := func() ([32]byte, error) { return [32]byte{1}, nil }
	external := &closeCountingFetcher{}
	storage, err := NewProcessStorageWithLocalAccess(cfg, customer, nil, false, external)
	if err != nil {
		t.Fatal(err)
	}
	borrowed := storage.Borrow()
	if borrowed.ManifestConfig() != cfg || borrowed.Fetcher() != external || borrowed.CustomerKeyFunc() == nil {
		t.Fatal("borrow lost an opaque access capability")
	}
	if err := borrowed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	if external.closes != 0 {
		t.Fatal("borrowed client ownership was transferred")
	}
	owned := &closeCountingFetcher{}
	original := &ProcessStorage{cfg: cfg, keyFn: customer, fetcher: &onDemandManifestFetcher{inner: owned, closer: owned}}
	if err := original.Borrow().Close(); err != nil {
		t.Fatal(err)
	}
	if owned.closes != 0 {
		t.Fatal("borrower closed original storage client")
	}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	if owned.closes != 1 {
		t.Fatal("original storage did not close its owned client")
	}
}

// Compile-time coverage of the borrowed manifest access capability.
var _ interface {
	OpenManifest(context.Context, store.ContentKey) (fetch.Stream, error)
} = (*closeCountingFetcher)(nil)
