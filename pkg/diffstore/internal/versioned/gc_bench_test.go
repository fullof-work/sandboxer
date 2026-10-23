package versioned

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// BenchmarkCheckpointReclamation is also source-compatible with the pre-#344
// engine. It observes persisted backlog/free states rather than guessing that
// a zero legacy GC return means completion. The explicit worker is stopped so
// baseline and candidate perform identical, reproducible phases and retention.
func BenchmarkCheckpointReclamation(b *testing.B) {
	for _, encrypted := range []bool{false, true} {
		name := "plain"
		if encrypted {
			name = "xts"
		}
		b.Run(name, func(b *testing.B) {
			mib := 64
			if value := os.Getenv("CHECKPOINT_BENCH_MIB"); value != "" {
				var err error
				mib, err = strconv.Atoi(value)
				if err != nil || mib < 64 || mib > 4096 {
					b.Fatal("invalid CHECKPOINT_BENCH_MIB")
				}
			}
			size := int64(mib) << 20
			opts := Options{DiffID: "reclamation", BaseID: "fixed-base", LogicalSize: size}
			if encrypted {
				opts.Key = &[32]byte{7}
			}
			path := b.TempDir() + "/disk.diff"
			s, err := Create(path, opts)
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			stopTestGC(s)
			chunk := bytes.Repeat([]byte{0x11}, MaxBatchPages*PageSize)
			writeAll := func(latencies *[]int64) {
				for off := int64(0); off < size; off += int64(len(chunk)) {
					start := time.Now()
					if _, err := s.WriteAt(chunk, off); err != nil {
						b.Fatal(err)
					}
					if latencies != nil {
						*latencies = append(*latencies, time.Since(start).Nanoseconds())
					}
				}
			}
			writeAll(nil)
			if err = s.Checkpoint(ctx, "keep"); err != nil {
				b.Fatal(err)
			}
			for i := range chunk {
				chunk[i] = 0x20
			}
			writeAll(nil)
			var punchCalls, gcCalls, freed, blockedWrites uint64
			var gcTime, backlogTime time.Duration
			s.beforeIO = func(stage string) error {
				if stage == "gc-punch" {
					punchCalls++
				}
				return nil
			}
			before := s.Stats()
			latencies := make([]int64, 0, b.N*int(size)/len(chunk))
			b.SetBytes(size)
			b.ResetTimer()
			for cycle := 0; cycle < b.N; cycle++ {
				id := fmt.Sprintf("temporary-%d", cycle)
				if err = s.Checkpoint(ctx, id); err != nil {
					b.Fatal(err)
				}
				v, err := s.Export(ctx, id)
				if err != nil {
					b.Fatal(err)
				}
				for i := range chunk {
					chunk[i] = byte(0x21 + cycle%100)
				}
				writeAll(&latencies)
				if err = s.DeleteCheckpoint(ctx, id); err != nil {
					b.Fatal(err)
				}
				blockedBefore := s.Stats()
				// Same number of attempts while a retained reader is deliberately held.
				for range 16 {
					if _, err = s.GC(ctx, 64); err != nil {
						b.Fatal(err)
					}
					gcCalls++
				}
				blockedAfter := s.Stats()
				blockedWrites += blockedAfter.IndexWritten - blockedBefore.IndexWritten + blockedAfter.RedoWritten - blockedBefore.RedoWritten
				if err = v.Close(); err != nil {
					b.Fatal(err)
				}
				start := time.Now()
				for steps := 0; s.ctrl.Pending != 0 || s.ctrl.Garbage != 0; steps++ {
					if steps > int(size/PageSize)*4+10000 {
						b.Fatal("GC failed to drain backlog")
					}
					oldHead := s.ctrl.Free
					callStart := time.Now()
					if _, err = s.GC(ctx, 64); err != nil {
						b.Fatal(err)
					}
					gcTime += time.Since(callStart)
					gcCalls++
					// Count only free-list publication, including legacy implementations.
					tx := &transaction{store: s, ctrl: s.ctrl, pages: map[uint64][]byte{}}
					for slot := s.ctrl.Free; slot != oldHead; {
						if slot == 0 {
							b.Fatal("GC lost old free tail")
						}
						d, e := tx.desc(slot)
						if e != nil || d.Kind != kindFree {
							b.Fatal("invalid committed free slot", e)
						}
						freed++
						slot = d.Next
					}
				}
				backlogTime += time.Since(start)
			}
			b.StopTimer()
			after := s.Stats()
			for name, value := range map[string]uint64{
				"commits/cycle": after.Commits - before.Commits, "index-write-B/cycle": after.IndexWritten - before.IndexWritten,
				"redo-B/cycle": after.RedoWritten - before.RedoWritten, "reused-slots/cycle": after.Reused - before.Reused,
				"allocated-slots/cycle": after.Allocated - before.Allocated, "punch-calls/cycle": punchCalls, "gc-calls/cycle": gcCalls,
				"freed-slots/cycle": freed, "blocked-metadata-B/cycle": blockedWrites,
			} {
				b.ReportMetric(float64(value)/float64(b.N), name)
			}
			b.ReportMetric(float64(gcTime.Nanoseconds())/float64(b.N), "gc-ns/cycle")
			b.ReportMetric(float64(backlogTime.Nanoseconds())/float64(b.N), "backlog-wall-ns/cycle")
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			for _, p := range []int{95, 99} {
				b.ReportMetric(float64(latencies[(len(latencies)-1)*p/100]), fmt.Sprintf("store-write-p%d-ns", p))
			}
			var stat unix.Stat_t
			if err = unix.Stat(path, &stat); err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(stat.Blocks*512), "allocated-B")
			b.ReportMetric(float64(stat.Size), "file-length-B")
			// Observe locality after repeated reuse. Check full content, including the
			// retained root, outside the write/GC timer with O_DIRECT payload reads.
			buffer := make([]byte, 1<<20)
			start := time.Now()
			expected := byte(0x21 + (b.N-1)%100)
			for off := int64(0); off < size; off += int64(len(buffer)) {
				if _, err = s.ReadAt(buffer, off); err != nil {
					b.Fatal(err)
				}
				if !bytes.Equal(buffer, bytes.Repeat([]byte{expected}, len(buffer))) {
					b.Fatal("active data differs after reuse")
				}
			}
			b.ReportMetric(float64(size)/(1<<20)/time.Since(start).Seconds(), "sequential-read-MiB/s")
			v, err := s.Export(ctx, "keep")
			if err != nil {
				b.Fatal(err)
			}
			defer v.Close()
			start = time.Now()
			for off := int64(0); off < size; off += int64(len(buffer)) {
				if _, err = v.ReadAt(ctx, buffer, uint64(off)); err != nil {
					b.Fatal(err)
				}
				if !bytes.Equal(buffer, bytes.Repeat([]byte{0x11}, len(buffer))) {
					b.Fatal("retained data differs after reuse")
				}
			}
			b.ReportMetric(float64(size)/(1<<20)/time.Since(start).Seconds(), "retained-export-read-MiB/s")
			if err = s.Checkpoint(ctx, "current"); err != nil {
				b.Fatal(err)
			}
			current, err := s.Export(ctx, "current")
			if err != nil {
				b.Fatal(err)
			}
			defer current.Close()
			start = time.Now()
			for off := int64(0); off < size; off += int64(len(buffer)) {
				if _, err = current.ReadAt(ctx, buffer, uint64(off)); err != nil {
					b.Fatal(err)
				}
				if !bytes.Equal(buffer, bytes.Repeat([]byte{expected}, len(buffer))) {
					b.Fatal("current export differs after reuse")
				}
			}
			b.ReportMetric(float64(size)/(1<<20)/time.Since(start).Seconds(), "current-export-read-MiB/s")
		})
	}
}

// Measure the real worker separately from explicit GC calls. A 4 MiB backlog
// keeps the legacy one-batch-per-tick comparison short enough to repeat.
func BenchmarkCheckpointBackgroundGC(b *testing.B) {
	for _, encrypted := range []bool{false, true} {
		name := "plain"
		if encrypted {
			name = "xts"
		}
		b.Run(name, func(b *testing.B) {
			const size = 4 << 20
			var freed, metadata uint64
			b.SetBytes(size)
			b.ResetTimer()
			b.StopTimer()
			for cycle := range b.N {
				opts := Options{DiffID: "background", LogicalSize: size}
				if encrypted {
					opts.Key = &[32]byte{7}
				}
				s, err := Create(fmt.Sprintf("%s/%d.diff", b.TempDir(), cycle), opts)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = s.Close() })
				stopTestGC(s)
				payload := bytes.Repeat([]byte{0x41}, size)
				for _, id := range []string{"keep", "delete"} {
					if _, err = s.WriteAt(payload, 0); err != nil {
						b.Fatal(err)
					}
					if err = s.Checkpoint(ctx, id); err != nil {
						b.Fatal(err)
					}
				}
				if _, err = s.WriteAt(payload, 0); err != nil {
					b.Fatal(err)
				}
				if err = s.DeleteCheckpoint(ctx, "delete"); err != nil {
					b.Fatal(err)
				}
				before := s.Stats()
				deadline := time.Now().Add(time.Minute)
				b.StartTimer()
				s.startGC()
				for {
					s.mu.Lock()
					complete, fatal := s.ctrl.Pending == 0 && s.ctrl.Garbage == 0, s.fatal
					s.mu.Unlock()
					if fatal != nil {
						b.Fatal(fatal)
					}
					if complete {
						break
					}
					if time.Now().After(deadline) {
						b.Fatal("background reclamation did not complete")
					}
					time.Sleep(time.Millisecond)
				}
				b.StopTimer()
				stopTestGC(s)
				after := s.Stats()
				metadata += after.IndexWritten - before.IndexWritten + after.RedoWritten - before.RedoWritten
				tx := &transaction{store: s, ctrl: s.ctrl, pages: map[uint64][]byte{}}
				for slot := s.ctrl.Free; slot != 0; {
					d, err := tx.desc(slot)
					if err != nil || d.Kind != kindFree {
						b.Fatal("invalid committed free slot", err)
					}
					freed++
					slot = d.Next
				}
				if err = s.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(size), "backlog-payload-B")
			b.ReportMetric(float64(freed)/float64(b.N), "freed-slots/cycle")
			b.ReportMetric(float64(metadata)/float64(b.N), "metadata-B/cycle")
		})
	}
}
