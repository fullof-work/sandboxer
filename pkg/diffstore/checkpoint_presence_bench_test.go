package diffstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore/internal/versioned"
)

// TestCheckpointPresencePhases is an opt-in native-filesystem performance probe.
// Seed once with CHECKPOINT_PRESENCE_PHASE=seed, then measure the same files with
// each revision's test binary. CHECKPOINT_PRESENCE_DIR must be on an O_DIRECT
// capable filesystem; CASE is dense-plain, dense-xts, sparse-plain or sparse-xts.
// A sparse case has 30 GiB logical capacity but only 240 mapped payload pages.
// No timing threshold is a correctness assertion. Keep raw JSON and record the
// revision, filesystem and machine separately when comparing results.
func TestCheckpointPresencePhases(t *testing.T) {
	dir := os.Getenv("CHECKPOINT_PRESENCE_DIR")
	if dir == "" {
		t.Skip("requires explicit CHECKPOINT_PRESENCE_DIR performance fixture")
	}
	name := os.Getenv("CHECKPOINT_PRESENCE_CASE")
	if name != "dense-plain" && name != "dense-xts" && name != "sparse-plain" && name != "sparse-xts" {
		t.Fatal("invalid CHECKPOINT_PRESENCE_CASE")
	}
	sparse := strings.HasPrefix(name, "sparse-")
	encrypted := strings.HasSuffix(name, "-xts")
	size := int64(1 << 30)
	stride := int64(1)
	if sparse {
		size = 30 << 30
		stride = 32768
	}
	path := filepath.Join(dir, name+".diff")
	key := [32]byte{0x73, 0x51}
	storeOptions := versioned.Options{DiffID: name, BaseID: "presence-probe-base", LogicalSize: size}
	options := []DiskOption{WithCheckpoint(name, storeOptions.BaseID)}
	if encrypted {
		storeOptions.Key = &key
		storeOptions.RequireEncryption = true
		options = append(options, WithDiffEncryption(key, true))
	}
	if os.Getenv("CHECKPOINT_PRESENCE_PHASE") == "seed" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		store, err := versioned.Create(path, storeOptions)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		zero := make([]byte, cowBlockSize)
		data := bytes.Repeat([]byte{0x35}, cowBlockSize)
		pages := make([]versioned.Page, 0, versioned.MaxBatchPages)
		for block := int64(0); block < size/cowBlockSize; block += stride {
			payload := data
			if block == 0 {
				payload = zero // Allocated zero must mask the nonzero base.
			}
			pages = append(pages, versioned.Page{Block: uint64(block), Data: payload})
			if len(pages) == cap(pages) || block+stride >= size/cowBlockSize {
				if err := store.WritePages(context.Background(), pages); err != nil {
					t.Fatal(err)
				}
				pages = pages[:0]
			}
		}
		if err := store.Checkpoint(context.Background(), "seed"); err != nil {
			t.Fatal(err)
		}
		for {
			n, err := store.GC(context.Background(), 256)
			if err != nil {
				t.Fatal(err)
			}
			if n.State != More {
				break
			}
		}
		if err := store.Sync(); err != nil {
			t.Fatal(err)
		}
		return
	}
	if os.Getenv("CHECKPOINT_PRESENCE_PHASE") != "measure" {
		t.Fatal("CHECKPOINT_PRESENCE_PHASE must be seed or measure")
	}
	open := func() (*Disk, time.Duration, uint64) {
		t.Helper()
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		cow, err := OpenDisk(path, presenceProbeBase{size}, DiffInit{Existing: true, CreateSize: size}, options...)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&after)
		return cow, elapsed, after.TotalAlloc - before.TotalAlloc
	}
	cow, openTime, openAlloc := open()
	defer cow.Close()
	backend, err := cow.checkpointBackend()
	if err != nil {
		t.Fatal(err)
	}
	openStats := backend.store.Stats()
	openBackendStats := cow.BackendStats()
	// Include known upper data, allocated zero and base holes, then a reproducible
	// mixed startup working set. The repeated pass fits the 32 MiB COW cache.
	blocks := []int64{0, stride, size/cowBlockSize - 1}
	rng := rand.New(rand.NewSource(334))
	for len(blocks) < 1024 {
		block := rng.Int63n(size / cowBlockSize)
		if sparse && len(blocks)%2 == 0 {
			block = (block / stride) * stride
		}
		blocks = append(blocks, block)
	}
	read := func() (time.Duration, []int64) {
		t.Helper()
		latencies := make([]int64, 0, len(blocks))
		page := make([]byte, cowBlockSize)
		start := time.Now()
		for _, block := range blocks {
			before := time.Now()
			if n, err := cow.ReadAt(page, block*cowBlockSize); err != nil || n != len(page) {
				t.Fatalf("read block %d: %d %v", block, n, err)
			}
			latencies = append(latencies, time.Since(before).Nanoseconds())
			want := byte(0x35)
			if block == 0 {
				want = 0
			} else if sparse && block%stride != 0 {
				want = 0xa7
			}
			for _, b := range page {
				if b != want {
					t.Fatalf("block %d returned %02x, want %02x", block, b, want)
				}
			}
		}
		return time.Since(start), latencies
	}
	startupTime, startupLatencies := read()
	startupStats := backend.store.Stats()
	warmTime, warmLatencies := read()
	warmStats := backend.store.Stats()
	if err := cow.Close(); err != nil {
		t.Fatal(err)
	}
	// Ordinary exports still need the complete hole map. Measure this on a fresh
	// reopen rather than giving the export credit for the preceding workload.
	cow, exportOpenTime, _ := open()
	defer cow.Close()
	backend, err = cow.checkpointBackend()
	if err != nil {
		t.Fatal(err)
	}
	exportBefore := backend.store.Stats()
	start := time.Now()
	view, holes, err := cow.SnapshotView()
	exportTime := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	exportAfter := backend.store.Stats()
	for _, stats := range []versioned.Stats{warmStats, exportAfter} {
		if stats.DataWritten != 0 || stats.IndexWritten != 0 || stats.RedoWritten != 0 || stats.GCWritten != 0 || stats.GCPunched != 0 {
			t.Fatalf("read-only measurement contained background writes: %+v", stats)
		}
	}
	if offset, err := view.Seek(0, io.SeekEnd); err != nil || offset != size {
		t.Fatal("snapshot size", offset, err)
	}
	if got, want := cow.DirtyCount(), int(size/cowBlockSize/stride); got != want {
		t.Fatalf("snapshot presence count %d, want %d", got, want)
	}
	wantHoles := 0
	if sparse {
		wantHoles = int(size / cowBlockSize / stride)
	}
	if len(holes) != wantHoles {
		t.Fatalf("snapshot hole count %d, want %d", len(holes), wantHoles)
	}
	result := map[string]any{
		"case": name, "label": os.Getenv("CHECKPOINT_PRESENCE_LABEL"), "logical_bytes": size,
		"mapped_pages": size / cowBlockSize / stride, "requests": len(blocks),
		"open_ns": openTime.Nanoseconds(), "open_total_alloc_bytes": openAlloc,
		"open_index_read_bytes": openStats.IndexRead, "open_data_read_bytes": openStats.DataRead,
		"open_backend_stats": openBackendStats,
		"first_read_ns":      startupLatencies[0], "startup_reads_ns": startupTime.Nanoseconds(),
		"open_plus_startup_ns":     (openTime + startupTime).Nanoseconds(),
		"startup_index_read_bytes": startupStats.IndexRead - openStats.IndexRead,
		"startup_data_read_bytes":  startupStats.DataRead - openStats.DataRead,
		"warm_reads_ns":            warmTime.Nanoseconds(),
		"warm_index_read_bytes":    warmStats.IndexRead - startupStats.IndexRead,
		"warm_data_read_bytes":     warmStats.DataRead - startupStats.DataRead,
		"export_open_ns":           exportOpenTime.Nanoseconds(), "snapshot_view_ns": exportTime.Nanoseconds(),
		"open_plus_snapshot_view_ns":     (exportOpenTime + exportTime).Nanoseconds(),
		"snapshot_view_index_read_bytes": exportAfter.IndexRead - exportBefore.IndexRead,
		"snapshot_holes":                 len(holes), "snapshot_dirty_blocks": cow.DirtyCount(),
		"after_workload_store_stats": warmStats, "after_export_store_stats": exportAfter,
	}
	for name, values := range map[string][]int64{"startup": startupLatencies, "warm": warmLatencies} {
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		for _, percentile := range []int{50, 95, 99} {
			result[fmt.Sprintf("%s_p%d_ns", name, percentile)] = values[(len(values)-1)*percentile/100]
		}
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("PRESENCE_RESULT %s\n", encoded)
}

type presenceProbeBase struct{ size int64 }

func (b presenceProbeBase) Size() int64 { return b.size }
func (presenceProbeBase) Close() error  { return nil }
func (presenceProbeBase) ReadAt(p []byte, off int64) (int, error) {
	for i := range p {
		p[i] = 0xa7
	}
	return len(p), nil
}
