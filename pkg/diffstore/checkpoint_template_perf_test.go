package diffstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// TestCheckpointTemplatePhases is the opt-in entry point used by
// test/perf/checkpoint_template.py. Each invocation initializes and verifies a
// complete 64 MiB template on the actual filesystem, then times fresh target
// Open, Ready, materialization, and final publication separately.
func TestCheckpointTemplatePhases(t *testing.T) {
	mode := os.Getenv("VHOST_TEMPLATE_PERF_MODE")
	if mode == "" {
		t.Skip("requires explicit test/perf/checkpoint_template.py harness")
	}
	if mode != "raw" && mode != "v1" && mode != "v2" && mode != "v2-xts" {
		t.Fatal("invalid VHOST_TEMPLATE_PERF_MODE")
	}
	round, err := strconv.Atoi(os.Getenv("VHOST_TEMPLATE_PERF_ROUND"))
	if err != nil || round < 1 {
		t.Fatal("invalid VHOST_TEMPLATE_PERF_ROUND")
	}
	const size = int64(64 << 20)
	ctx := context.Background()
	dir := t.TempDir()
	var fs unix.Statfs_t
	if err := unix.Statfs(dir, &fs); err != nil {
		t.Fatal(err)
	}
	if fs.Type == unix.TMPFS_MAGIC {
		t.Fatal("template throughput requires a real disk filesystem")
	}
	var options []DiskOption
	if mode == "v1" || mode == "v2-xts" {
		options = append(options, WithDiffEncryption([32]byte{1}, true))
	}
	templatePath := filepath.Join(dir, "template.diff")
	template, err := OpenDisk(templatePath, nil, DiffInit{CreateSize: size}, options...)
	if err != nil {
		t.Fatal(err)
	}
	defer template.Close()
	chunk := make([]byte, 1<<20)
	if _, err := rand.New(rand.NewSource(327)).Read(chunk); err != nil {
		t.Fatal(err)
	}
	want := sha256.New()
	for off := int64(0); off < size; off += int64(len(chunk)) {
		if n, err := template.WriteAt(chunk, off); err != nil || n != len(chunk) {
			t.Fatal(n, err)
		}
		_, _ = want.Write(chunk)
	}
	if err := template.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := template.Close(); err != nil {
		t.Fatal(err)
	}
	if mode == "v2" || mode == "v2-xts" {
		options = append(options, WithCheckpoint("template-perf", ""))
	}
	path := filepath.Join(dir, "target.diff")
	var cpuBefore unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &cpuBefore); err != nil {
		t.Fatal(err)
	}
	totalStart := time.Now()
	start := time.Now()
	cow, err := OpenDisk(path, nil, DiffInit{CreateSize: size, TemplatePath: templatePath}, options...)
	openNS := time.Since(start).Nanoseconds()
	if err != nil {
		t.Fatal(err)
	}
	defer cow.Close()
	start = time.Now()
	if err := cow.WaitMaterialReady(ctx); err != nil {
		t.Fatal(err)
	}
	readyNS := time.Since(start).Nanoseconds()
	start = time.Now()
	if err := cow.WaitMaterialized(ctx); err != nil {
		t.Fatal(err)
	}
	materializedNS := time.Since(start).Nanoseconds()
	totalNS := time.Since(totalStart).Nanoseconds()
	var cpuAfter unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &cpuAfter); err != nil {
		t.Fatal(err)
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	var physical unix.Stat_t
	if err := unix.Stat(path, &physical); err != nil {
		t.Fatal(err)
	}
	row := map[string]any{
		"phase": "vhost-template", "mode": mode, "round": round, "template_payload_bytes": size,
		"open_ns": openNS, "wait_ready_ns": readyNS, "materialized_ns": materializedNS, "total_ns": totalNS,
		"sustained_MB_per_second": float64(size) * 1e3 / float64(totalNS),
		"cpu_user_ns":             cpuAfter.Utime.Nano() - cpuBefore.Utime.Nano(),
		"cpu_system_ns":           cpuAfter.Stime.Nano() - cpuBefore.Stime.Nano(),
		"process_peak_rss_kib":    cpuAfter.Maxrss, "go_heap_bytes": memory.HeapAlloc,
		"allocated_file_bytes": physical.Blocks * 512, "file_bytes": physical.Size,
		"filesystem_type": fmt.Sprintf("0x%x", fs.Type),
		"note":            "64 MiB fully populated raw/v1 source; v1 and v2-xts share key/required policy; total includes final materialization publication and initial synchronization; source setup and final full SHA256 verification are outside timing; process peak RSS includes setup",
	}
	if backend, err := cow.checkpointBackend(); err == nil {
		stats := backend.store.Stats()
		if stats.DataWritten != uint64(size) {
			t.Fatalf("materializer committed %d data bytes, want %d", stats.DataWritten, size)
		}
		row["stats"] = stats
	}
	got := sha256.New()
	for off := int64(0); off < size; off += int64(len(chunk)) {
		if n, err := cow.ReadAt(chunk, off); err != nil || n != len(chunk) {
			t.Fatal(n, err)
		}
		_, _ = got.Write(chunk)
	}
	if !bytes.Equal(want.Sum(nil), got.Sum(nil)) {
		t.Fatal("materialized template SHA256 mismatch")
	}
	row["verified_sha256"] = hex.EncodeToString(got.Sum(nil))
	if err := cow.Close(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("vhost-template %s\n", encoded)
}
