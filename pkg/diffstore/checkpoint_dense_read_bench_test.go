package diffstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore/internal/versioned"
	"golang.org/x/sys/unix"
)

// TestCheckpointDenseReadPhases uses the same initialized O_DIRECT files for
// baseline and candidate binaries. It is opt-in, has no timing assertions and
// never substitutes a retained View for the writable owner's read path.
//
// CHECKPOINT_READ_DIR selects an ext4/native filesystem fixture directory.
// CHECKPOINT_READ_CASE is plain-shared, plain-private, xts-shared or xts-private.
// CHECKPOINT_READ_PHASE is seed (once per case) or measure (fresh process/run).
// CHECKPOINT_READ_MIB defaults to 128, strictly larger than the 32 MiB cache.
// CHECKPOINT_READ_RANDOM_REQUESTS defaults to 4096 distinct 4 KiB pages (16 MiB,
// which fits the cache for the hot repeat). CHECKPOINT_READ_LABEL labels JSON.
// Existing BenchmarkCheckpointCOWWriteback supplies drained write workloads;
// set CHECKPOINT_BENCH_MIB=128 to compare the same working-set size.
func TestCheckpointDenseReadPhases(t *testing.T) {
	dir := os.Getenv("CHECKPOINT_READ_DIR")
	if dir == "" {
		t.Skip("requires explicit CHECKPOINT_READ_DIR performance fixture")
	}
	name := os.Getenv("CHECKPOINT_READ_CASE")
	if name != "plain-shared" && name != "plain-private" && name != "xts-shared" && name != "xts-private" {
		t.Fatal("invalid CHECKPOINT_READ_CASE")
	}
	mib := checkpointReadEnvInt(t, "CHECKPOINT_READ_MIB", 128, 64, 4096)
	randomRequests := checkpointReadEnvInt(t, "CHECKPOINT_READ_RANDOM_REQUESTS", 4096, 1, 4096)
	size := int64(mib) << 20
	path := filepath.Join(dir, name+".diff")
	key := [32]byte{0x35, 0x37}
	storeOptions := versioned.Options{DiffID: name, BaseID: "dense-read-probe", LogicalSize: size}
	options := []DiskOption{WithCheckpoint(name, storeOptions.BaseID)}
	if strings.HasPrefix(name, "xts-") {
		storeOptions.Key, storeOptions.RequireEncryption = &key, true
		options = append(options, WithDiffEncryption(key, true))
	}
	switch os.Getenv("CHECKPOINT_READ_PHASE") {
	case "seed":
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		store, err := versioned.Create(path, storeOptions)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		write := func() {
			payload := make([]byte, versioned.MaxBatchPages*cowBlockSize)
			pages := make([]versioned.Page, versioned.MaxBatchPages)
			for off := int64(0); off < size; off += int64(len(payload)) {
				checkpointReadPattern(payload, off)
				for i := range pages {
					pages[i] = versioned.Page{Block: uint64(off/cowBlockSize) + uint64(i), Data: payload[i*cowBlockSize : (i+1)*cowBlockSize]}
				}
				if err := store.WritePages(context.Background(), pages); err != nil {
					t.Fatal(err)
				}
			}
		}
		write()
		if err := store.Checkpoint(context.Background(), "retained"); err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(name, "-private") {
			write() // Every active payload slot is now private; the old root remains retained.
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
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	case "measure":
		measureCheckpointDenseReads(t, name, path, size, randomRequests, options)
	default:
		t.Fatal("CHECKPOINT_READ_PHASE must be seed or measure")
	}
}

func checkpointReadEnvInt(t *testing.T, name string, fallback, minimum, maximum int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < minimum || n > maximum {
		t.Fatalf("%s must be an integer from %d to %d", name, minimum, maximum)
	}
	return n
}

func checkpointReadPattern(p []byte, off int64) {
	for i := 0; i < len(p); i += cowBlockSize {
		block := uint64((off + int64(i)) / cowBlockSize)
		page := p[i : i+cowBlockSize]
		for j := range page {
			page[j] = byte(block*17 + uint64(j)*31)
		}
		binary.LittleEndian.PutUint64(page, block)
	}
}

func measureCheckpointDenseReads(t *testing.T, name, path string, size int64, randomRequests int, options []DiskOption) {
	t.Helper()
	open := func() (*Disk, *checkpointDiff, time.Duration) {
		start := time.Now()
		cow, err := OpenDisk(path, nil, DiffInit{Existing: true, CreateSize: size}, options...)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cow.Close() })
		backend, err := cow.checkpointBackend()
		if err != nil {
			t.Fatal(err)
		}
		if cow.cache.Stats().Capacity != 32<<20 {
			t.Fatal("probe requires a 32 MiB COW cache")
		}
		return cow, backend, elapsed
	}
	measure := func(cow *Disk, backend *checkpointDiff, openTime time.Duration, phase string, offsets []int64, request int, hot bool) {
		payload, want := make([]byte, request), make([]byte, request)
		latencies := make([]int64, len(offsets))
		runtime.GC()
		var memoryBefore, memoryAfter runtime.MemStats
		runtime.ReadMemStats(&memoryBefore)
		before := backend.store.Stats()
		var cpuBefore, cpuAfter unix.Rusage
		if err := unix.Getrusage(unix.RUSAGE_SELF, &cpuBefore); err != nil {
			t.Fatal(err)
		}
		var apiTime time.Duration
		start := time.Now()
		for i, off := range offsets {
			callStart := time.Now()
			n, err := cow.ReadAt(payload, off)
			elapsed := time.Since(callStart)
			latencies[i] = elapsed.Nanoseconds()
			apiTime += elapsed
			if err != nil || n != len(payload) {
				t.Fatalf("%s read at %d: %d %v", phase, off, n, err)
			}
			// Full-byte verification is excluded from per-call API latencies.
			checkpointReadPattern(want, off)
			if !bytes.Equal(payload, want) {
				t.Fatalf("%s data mismatch at %d", phase, off)
			}
		}
		wall := time.Since(start)
		if err := unix.Getrusage(unix.RUSAGE_SELF, &cpuAfter); err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&memoryAfter)
		after := backend.store.Stats()
		if hot && after.DataRead != before.DataRead {
			t.Fatalf("%s should fit in COW cache: payload bytes=%d", phase, after.DataRead-before.DataRead)
		}
		if after.DataWritten != before.DataWritten || after.IndexWritten != before.IndexWritten || after.RedoWritten != before.RedoWritten || after.GCWritten != before.GCWritten || after.GCPunched != before.GCPunched {
			t.Fatal("read-only measurement mutated its fixture", before, after)
		}
		logical := int64(len(offsets) * request)
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		row := map[string]any{
			"case": name, "label": os.Getenv("CHECKPOINT_READ_LABEL"), "phase": phase,
			"logical_size_bytes": size, "cache_bytes": cow.cache.Stats().Capacity,
			"request_bytes": request, "requests": len(offsets), "logical_bytes": logical,
			"open_ns": openTime.Nanoseconds(), "api_ns": apiTime.Nanoseconds(),
			"api_MiB_s":                         float64(logical) / (1 << 20) / apiTime.Seconds(),
			"wall_with_validation_ns":           wall.Nanoseconds(),
			"process_user_with_validation_ns":   cpuAfter.Utime.Nano() - cpuBefore.Utime.Nano(),
			"process_system_with_validation_ns": cpuAfter.Stime.Nano() - cpuBefore.Stime.Nano(),
			"process_rss_end_bytes":             checkpointReadRSS(t), "process_lifetime_peak_rss_kib": cpuAfter.Maxrss,
			"go_heap_alloc_end_bytes": memoryAfter.HeapAlloc, "go_heap_inuse_end_bytes": memoryAfter.HeapInuse,
			"go_total_alloc_delta_bytes": memoryAfter.TotalAlloc - memoryBefore.TotalAlloc,
			"go_allocations_delta":       memoryAfter.Mallocs - memoryBefore.Mallocs,
			"gc_cycles":                  memoryAfter.NumGC - memoryBefore.NumGC,
			"payload_read_bytes":         after.DataRead - before.DataRead, "index_read_bytes": after.IndexRead - before.IndexRead,
			"store_before": before, "store_after": after,
		}
		for _, percentile := range []int{50, 95, 99} {
			row[fmt.Sprintf("api_p%d_ns", percentile)] = latencies[(len(latencies)-1)*percentile/100]
		}
		encoded, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("CHECKPOINT_READ_RESULT %s\n", encoded)
	}
	sequential := make([]int64, size/(1<<20))
	for i := range sequential {
		sequential[i] = int64(i) << 20
	}
	cow, backend, openTime := open()
	measure(cow, backend, openTime, "sequential-cold", sequential, 1<<20, false)
	measure(cow, backend, 0, "sequential-repeat", sequential, 1<<20, false)
	measure(cow, backend, 0, "sequential-hot-tail", sequential[len(sequential)-16:], 1<<20, true)
	if err := cow.Close(); err != nil {
		t.Fatal(err)
	}
	// A fresh COW cache isolates random misses from the sequential tail.
	cow, backend, openTime = open()
	permutation := rand.New(rand.NewSource(337)).Perm(int(size / cowBlockSize))
	randomOffsets := make([]int64, randomRequests)
	for i := range randomOffsets {
		randomOffsets[i] = int64(permutation[i] * cowBlockSize)
	}
	measure(cow, backend, openTime, "random-cold", randomOffsets, cowBlockSize, false)
	measure(cow, backend, 0, "random-hot", randomOffsets, cowBlockSize, true)
	if err := cow.Close(); err != nil {
		t.Fatal(err)
	}
}

func checkpointReadRSS(t *testing.T) uint64 {
	t.Helper()
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		t.Fatal("missing resident page count")
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return pages * uint64(os.Getpagesize())
}
