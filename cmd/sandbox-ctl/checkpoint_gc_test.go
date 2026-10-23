package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
)

func TestCheckpointGCOfflineEncryptedAndBlocked(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	owner, err := diffstore.LockDirectory(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	key := [32]byte{7, 9}
	store, err := diffstore.Create(filepath.Join(directory, "diffs", "encrypted.diff"), diffstore.Options{DiffID: "encrypted", LogicalSize: 128 * 4096, Key: &key})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = store.WriteAt(bytes.Repeat([]byte{0x32}, 128*4096), 0); err != nil {
		t.Fatal(err)
	}
	if err = store.Checkpoint(ctx, "old"); err != nil {
		t.Fatal(err)
	}
	view, err := store.Export(ctx, "old")
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	if _, err = store.WriteAt(bytes.Repeat([]byte{0x64}, 128*4096), 0); err != nil {
		t.Fatal(err)
	}
	if err = store.DeleteCheckpoint(ctx, "old"); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	command := func() (int, string, string) {
		return captureInfoOutput(t, func() int { return checkpointCmd([]string{"gc", "--checkpoint-dir", directory, "--json"}) })
	}
	rc, _, stderr := command()
	if rc == 0 {
		t.Fatal("online directory accepted", stderr)
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	// An unusable artifact config and no key must not affect metadata-only GC.
	t.Setenv(config.ManifestConfigEnv, filepath.Join(directory, "missing-artifacts.yaml"))
	rc, stdout, stderr := command()
	var rows []diffstore.DirectoryGCResult
	if rc != 0 || json.Unmarshal([]byte(stdout), &rows) != nil || len(rows) != 1 || rows[0].State != string(diffstore.BlockedByReaders) || rows[0].FreedSlots != 0 {
		t.Fatalf("blocked is a normal result: %d %s %s", rc, stdout, stderr)
	}
	if err = view.Close(); err != nil {
		t.Fatal(err)
	}
	rc, stdout, stderr = command()
	if rc != 0 || json.Unmarshal([]byte(stdout), &rows) != nil || rows[0].State != string(diffstore.Complete) || rows[0].FreedSlots == 0 {
		t.Fatalf("unpin reclaim: %d %s %s", rc, stdout, stderr)
	}
	// GC must keep the file and the consumed Checkpoint ID.
	s, err := diffstore.Open(filepath.Join(directory, "diffs", "encrypted.diff"), diffstore.Options{Key: &key})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Checkpoint(ctx, "old"); err != diffstore.ErrExists {
		t.Fatal("GC recycled Checkpoint ID", err)
	}
}

func TestCheckpointGCRejectsDeletionFiltersAndReportsFileErrors(t *testing.T) {
	for _, args := range [][]string{{"gc"}, {"gc", "--checkpoint-dir", "x", "--checkpoint", "old"}, {"gc", "--checkpoint-dir", "x", "--diff-id", "a"}, {"gc", "--sandbox-id", "s"}, {"gc", "--checkpoint-dir", "x", "old"}} {
		rc, _, _ := captureInfoOutput(t, func() int { return checkpointCmd(args) })
		if rc != 2 {
			t.Fatalf("accepted unsupported args %v: %d", args, rc)
		}
	}
	directory := t.TempDir()
	if err := os.MkdirAll(filepath.Join(directory, "diffs"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "diffs", "bad.diff"), []byte("bad"), 0600); err != nil {
		t.Fatal(err)
	}
	rc, stdout, stderr := captureInfoOutput(t, func() int { return checkpointCmd([]string{"gc", "--checkpoint-dir", directory, "--json"}) })
	if rc == 0 || !strings.Contains(stdout, `"diff_id":"bad"`) || !strings.Contains(stdout, `"state":"error"`) || stderr == "" {
		t.Fatalf("missing per-file failure: %d %s %s", rc, stdout, stderr)
	}
}
