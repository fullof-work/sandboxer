package versioned

import (
	"context"
	"sync"
	"testing"

	"golang.org/x/sys/unix"
)

type firstErrContext struct {
	context.Context
	once sync.Once
	fn   func()
}

func (c *firstErrContext) Err() error { c.once.Do(c.fn); return nil }

func TestDiscardFencesCheckpointAfterInitialMark(t *testing.T) {
	s, _ := newTestStore(t, 64, nil)
	putPages(t, s, []uint64{1, 2}, 1)
	gate := &firstErrContext{Context: ctx, fn: func() {
		// MarkModified's zero-current fast path has observed zero, but has not
		// returned to Discard's mutation lock yet. This reproduces that race
		// deterministically without a production hook.
		if err := s.Checkpoint(ctx, "intervening"); err != nil {
			t.Fatal(err)
		}
	}}
	if err := s.Discard(gate, PageSize, PageSize); err != nil {
		t.Fatal(err)
	}
	if s.CanResume("intervening") {
		t.Fatal("discard left a stale continuation marker")
	}
	checkActive(t, s, map[uint64][]byte{1: nil, 2: pageFor(2, 1)})
	v, err := s.Export(ctx, "intervening")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	checkView(t, v, map[uint64][]byte{1: pageFor(1, 1), 2: pageFor(2, 1)})
}

func TestDiscardReplaysRangeAfterReservationRetry(t *testing.T) {
	s, _ := newTestStore(t, 64, nil)
	stopTestGC(s)
	putPages(t, s, []uint64{1, 2, 3, 50}, 1)
	if err := s.Discard(ctx, 50*PageSize, PageSize); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(ctx, "before-discard"); err != nil {
		t.Fatal(err)
	}
	fired := false
	s.beforeIO = func(stage string) error {
		if stage == "reserve" && !fired {
			fired = true
			return unix.ENOSPC
		}
		return nil
	}
	if err := s.Discard(ctx, PageSize, 2*PageSize); err != nil {
		t.Fatal(err)
	}
	if !fired || s.Stats().GCPunched == 0 {
		t.Fatal("fixture did not exercise foreground GC and mutation retry")
	}
	checkActive(t, s, map[uint64][]byte{1: nil, 2: nil, 3: pageFor(3, 1), 50: nil})
	v, err := s.Export(ctx, "before-discard")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	checkView(t, v, map[uint64][]byte{1: pageFor(1, 1), 2: pageFor(2, 1), 3: pageFor(3, 1), 50: nil})
}
