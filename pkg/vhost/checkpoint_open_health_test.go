package vhost

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/internal/diffstore"
)

func TestCheckpointOpenRejectsCorruptCurrentBeforeCacheAttach(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%t", encrypted), func(t *testing.T) {
			const size = 520 * cowBlockSize
			path := filepath.Join(t.TempDir(), "active.diff")
			cow := checkpointCOW(t, path, nil, size, encrypted)
			// Multiple index leaves distinguish a root-only reopen from a scan.
			if _, err := cow.WriteAt(bytes.Repeat([]byte{0x5a}, 300*cowBlockSize), 0); err != nil {
				t.Fatal(err)
			}
			if err := cow.Checkpoint(context.Background(), "current"); err != nil {
				t.Fatal(err)
			}
			if err := cow.Close(); err != nil {
				t.Fatal(err)
			}
			f, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			control := make([]byte, diffstore.PageSize)
			if _, err := f.ReadAt(control, diffstore.PageSize); err != nil {
				t.Fatal(err)
			}
			length := binary.LittleEndian.Uint32(control[8:])
			if length > diffstore.PageSize-20 {
				t.Fatal("invalid fixture control length")
			}
			var state struct{ Current uint64 }
			if err := json.Unmarshal(control[12:12+length], &state); err != nil || state.Current == 0 {
				t.Fatal("missing fixture current checkpoint", err)
			}
			// v2 slots occupy 128 payload pages after each descriptor page,
			// starting at page 2048. Damage only the current checkpoint checksum.
			slot := state.Current - 1
			checkpointOffset := int64(2048+slot/128*129+1+slot%128) * diffstore.PageSize
			original := make([]byte, diffstore.PageSize)
			if _, err := f.ReadAt(original, checkpointOffset); err != nil {
				t.Fatal(err)
			}
			if !bytes.HasPrefix(original, []byte("KDCP2")) {
				t.Fatal("fixture current slot is not a checkpoint")
			}
			damaged := append([]byte(nil), original...)
			damaged[len(damaged)-1] ^= 1
			if _, err := f.WriteAt(damaged, checkpointOffset); err != nil {
				t.Fatal(err)
			}
			if err := f.Sync(); err != nil {
				t.Fatal(err)
			}
			cache, err := NewCOWCache(8*cowBlockSize, 4*cowBlockSize)
			if err != nil {
				t.Fatal(err)
			}
			defer cache.Close()
			options := []BlockCOWOption{WithCheckpoint("disk-under-test", "immutable-base"), WithCOWCache(cache)}
			if encrypted {
				options = append(options, WithDiffEncryption([32]byte{1, 2, 3}, true))
			}
			opened, err := OpenBlockCOW(path, nil, DiffInit{Existing: true, CreateSize: size}, options...)
			if opened != nil {
				health := opened.Err()
				_ = opened.Close()
				t.Fatalf("Open returned a COW with damaged Current: open=%v health=%v", err, health)
			}
			if err == nil || !strings.Contains(err.Error(), "checksum") {
				t.Fatalf("Open hid current-checkpoint corruption: %v", err)
			}
			cache.mu.Lock()
			clients := len(cache.clients)
			cache.mu.Unlock()
			if clients != 0 || cache.Err() != nil {
				t.Fatalf("failed open attached or poisoned shared cache: clients=%d err=%v", clients, cache.Err())
			}
			// Repair the fixture and reopen immediately: the failed attempt must
			// have closed its store and released the exclusive writer lock.
			if _, err := f.WriteAt(original, checkpointOffset); err != nil {
				t.Fatal(err)
			}
			if err := f.Sync(); err != nil {
				t.Fatal(err)
			}
			opened, err = OpenBlockCOW(path, nil, DiffInit{Existing: true, CreateSize: size}, options...)
			if err != nil {
				t.Fatal("failed open leaked its owner", err)
			}
			defer opened.Close()
			backend, err := opened.checkpointBackend()
			if err != nil {
				t.Fatal(err)
			}
			stats := backend.store.Stats()
			if stats.DataRead != 0 || stats.IndexRead != 2*diffstore.PageSize {
				t.Fatalf("reopen must read only root/current metadata, no payload/full scan: %+v", stats)
			}
			if stats := opened.BackendStats(); stats["diff_presence_known_blocks"] != 0 || stats["diff_presence_complete"] != false {
				t.Fatal("health check rebuilt presence", stats)
			}
		})
	}
}
