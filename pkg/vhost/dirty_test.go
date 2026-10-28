package vhost

import (
	"context"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/sandboxer/pkg/memory"
)

func TestWritableTranslationMarksBeforePartialFailure(t *testing.T) {
	tracker, _ := memory.NewDirty(5 * 4096)
	table := MemTable{regions: []MemRegion{{GuestPhysAddr: 1 << 32, UserspaceAddr: 1 << 40, MemorySize: 8192, MmapOffset: 4096, mmapBytes: make([]byte, 8192)}}, EnsureLoaded: func(context.Context, uint64, uint64) error { return nil }, MarkDirty: tracker.Mark}
	if _, err := table.TranslateGPA(context.Background(), 1<<32, 8192); err != nil {
		t.Fatal(err)
	}
	if tracker.Snapshot()[0] != 0 {
		t.Fatal("read-only translation became dirty")
	}
	b, err := table.WritableGPA(context.Background(), (1<<32)+4095, 2)
	if err != nil {
		t.Fatal(err)
	}
	if tracker.Snapshot()[0] != 6 {
		t.Fatalf("mark must precede slice exposure: %08b", tracker.Snapshot()[0])
	}
	b[0] = 7
	// A simulated I/O failure after the first byte cannot retract either page.
	failure := errors.New("partial read failure")
	_ = failure
	if tracker.Snapshot()[0] != 6 {
		t.Fatal("partial write disappeared")
	}
	if _, err := table.WritableUVA(context.Background(), 1<<40, 1); err != nil {
		t.Fatal(err)
	}
	table.MarkDirty = func(uint64, uint64) error { return failure }
	if b, err := table.WritableGPA(context.Background(), 1<<32, 1); b != nil || !errors.Is(err, failure) {
		t.Fatalf("failed tracking exposed buffer: %v %v", b, err)
	}
}
func TestGoBackendDirtyBeforePartialFailureAndCompletion(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry-completion", true: "terminal"}[terminal], func(t *testing.T) {
			failure := errors.New("storage failed after changing guest buffer")
			var ranges [][2]uint64
			covers := func(off uint64) bool {
				for _, r := range ranges {
					if off >= r[0] && off < r[0]+r[1] {
						return true
					}
				}
				return false
			}
			calls := 0
			s, q, mem, _ := recoveryQueue(t, recoveryStream{read: func(_ context.Context, p []byte) (int, error) {
				if !covers(512) {
					t.Fatal("guest buffer exposed before dirty mark")
				}
				calls++
				p[0] = 0x79
				if terminal {
					return 1, readerr.Mark(failure, false)
				}
				if calls == 1 {
					return 1, failure
				}
				return len(p), nil
			}}, false)
			s.SetDirtyTracker(func(off, n uint64) error { ranges = append(ranges, [2]uint64{off, n}); return nil })
			err := s.processQueue(q)
			if terminal {
				if !errors.Is(err, failure) {
					t.Fatal(err)
				}
				assertPending(t, q, mem)
			} else {
				if err != nil {
					t.Fatal(err)
				}
				for _, off := range []uint64{1536, 194, 196} {
					if !covers(off) {
						t.Fatalf("write %d not covered: %v", off, ranges)
					}
				}
			}
			if !covers(512) || mem[512] != 0x79 {
				t.Fatal("partial write or its dirty mark lost")
			}
		})
	}
}
