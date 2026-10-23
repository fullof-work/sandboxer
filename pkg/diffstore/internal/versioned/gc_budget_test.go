package versioned

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"
)

func TestGCPredecessorDiscoveryIsBoundedAndResumes(t *testing.T) {
	s, path := newTestStore(t, 64, nil)
	stopTestGC(s)
	putPages(t, s, []uint64{1}, 1)
	if err := s.Checkpoint(ctx, "survivor"); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 80)
	for i := range ids {
		ids[i] = fmt.Sprintf("middle-%d", i)
		if err := s.Checkpoint(ctx, ids[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Checkpoint(ctx, "tail"); err != nil {
		t.Fatal(err)
	}
	v, err := s.Export(ctx, "tail")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteCheckpoints(ctx, append(ids, "tail")); err != nil {
		t.Fatal(err)
	}
	putPages(t, s, []uint64{1}, 2)
	v.Close()
	s.mu.Lock()
	tail := s.ctrl.Tail
	s.gc = gcScan{cursor: tail, tail: tail, generation: s.gcDeletion, limit: s.ctrl.Next}
	cp, readErr := s.readCheckpoint(tail, nil)
	s.mu.Unlock()
	if err != nil || readErr != nil {
		t.Fatal(err, readErr)
	}
	previous := cp.Previous
	if _, err = s.GC(ctx, 1); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	cp, err = s.readCheckpoint(tail, nil)
	protected := s.ctrl.Protected
	s.mu.Unlock()
	if err != nil || cp.Previous != previous || protected != tail || s.gc.predecessor == previous {
		t.Fatalf("bounded traversal lost volatile progress/conservative protection: %+v %d %v", cp, protected, err)
	}
	before := s.Stats()
	if result, err := s.GC(ctx, 1); err != nil || result.State != More || result.FreedSlots != 0 {
		t.Fatalf("pure predecessor scan: %+v %v", result, err)
	}
	after := s.Stats()
	if after.Commits != before.Commits || after.RedoWritten != before.RedoWritten || after.IndexWritten != before.IndexWritten {
		t.Fatal("pure traversal committed metadata")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	stopTestGC(s)
	for i := 0; i < 200; i++ {
		if _, err = s.GC(ctx, 1); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.Lock()
	_, cp, err = s.findCheckpoint("tail", nil)
	pending := s.ctrl.Pending
	s.mu.Unlock()
	if err != nil || cp.Pending || pending != 0 {
		t.Fatalf("restarted traversal failed to finish: %+v pending=%d err=%v", cp, pending, err)
	}
	v, err = s.Export(ctx, "survivor")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	checkView(t, v, map[uint64][]byte{1: pageFor(1, 1)})
	checkActive(t, s, map[uint64][]byte{1: pageFor(1, 2)})
}

func finishTestGC(t *testing.T, s *Store, budget int) GCResult {
	t.Helper()
	var total uint64
	for range 2000 {
		r, err := s.GC(ctx, budget)
		if err != nil {
			t.Fatal(err)
		}
		total += r.FreedSlots
		if r.State != More {
			r.FreedSlots = total
			return r
		}
	}
	t.Fatal("GC did not finish its bounded round")
	return GCResult{}
}

func TestGCBlockedRoundHasNoWritesAndReaderReleaseNeedsNoDelete(t *testing.T) {
	s, _ := newTestStore(t, 16, nil)
	stopTestGC(s)
	putPages(t, s, []uint64{1, 2}, 1)
	if err := s.Checkpoint(ctx, "pinned"); err != nil {
		t.Fatal(err)
	}
	v, err := s.Export(ctx, "pinned")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	putPages(t, s, []uint64{1, 2}, 2)
	if err = s.DeleteCheckpoint(ctx, "pinned"); err != nil {
		t.Fatal(err)
	}
	before := s.Stats()
	for range 3 {
		r := finishTestGC(t, s, 1)
		if r.State != BlockedByReaders || r.FreedSlots != 0 {
			t.Fatalf("blocked result: %+v", r)
		}
	}
	after := s.Stats()
	if after.Commits != before.Commits || after.GCPunchCalls != before.GCPunchCalls || after.RedoWritten != before.RedoWritten {
		t.Fatal("blocked scan wrote metadata or punched")
	}
	checkView(t, v, map[uint64][]byte{1: pageFor(1, 1), 2: pageFor(2, 1)})
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	r := finishTestGC(t, s, 1)
	if r.State != Complete || r.FreedSlots == 0 {
		t.Fatalf("reader release did not progress without another deletion: %+v", r)
	}
	checkActive(t, s, map[uint64][]byte{1: pageFor(1, 2), 2: pageFor(2, 2)})
}

func TestGCRevalidatesPredecessorDeletedBetweenBatches(t *testing.T) {
	s, _ := newTestStore(t, 16, nil)
	stopTestGC(s)
	putPages(t, s, []uint64{1, 2, 3}, 1)
	if err := s.Checkpoint(ctx, "old"); err != nil {
		t.Fatal(err)
	}
	v, err := s.Export(ctx, "old")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if err = s.Checkpoint(ctx, "middle"); err != nil {
		t.Fatal(err)
	}
	if err = s.Checkpoint(ctx, "tail"); err != nil {
		t.Fatal(err)
	}
	putPages(t, s, []uint64{1, 2, 3}, 2)
	if err = s.DeleteCheckpoint(ctx, "tail"); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.gc = gcScan{cursor: s.ctrl.Tail, tail: s.ctrl.Tail, generation: s.gcDeletion, limit: s.ctrl.Next}
	s.mu.Unlock()
	if r, err := s.GC(ctx, 1); err != nil || r.State != More {
		t.Fatalf("first transfer: %+v %v", r, err)
	}
	if !s.gc.predecessorReady {
		t.Fatal("fixture has no remembered predecessor")
	}
	if err = s.DeleteCheckpoint(ctx, "middle"); err != nil {
		t.Fatal(err)
	}
	if r := finishTestGC(t, s, 1); r.State != Complete {
		t.Fatal(r)
	}
	checkView(t, v, map[uint64][]byte{1: pageFor(1, 1), 2: pageFor(2, 1), 3: pageFor(3, 1)})
	checkActive(t, s, map[uint64][]byte{1: pageFor(1, 2), 2: pageFor(2, 2), 3: pageFor(3, 2)})
}

func TestGCRoundTailAndDeletionGeneration(t *testing.T) {
	s, _ := newTestStore(t, 16, nil)
	stopTestGC(s)
	for _, id := range []string{"early", "pinned", "tail"} {
		if err := s.Checkpoint(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	v, err := s.Export(ctx, "pinned")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if err = s.DeleteCheckpoint(ctx, "pinned"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GC(ctx, 1); err != nil {
		t.Fatal(err)
	} // early already scanned
	oldTail := s.gc.tail
	if err = s.Checkpoint(ctx, "appended"); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteCheckpoint(ctx, "early"); err != nil {
		t.Fatal(err)
	}
	if s.gc.tail != oldTail {
		t.Fatal("append extended current round")
	}
	for i := 0; i < 2; i++ {
		r, err := s.GC(ctx, 1)
		if err != nil || r.State != More {
			t.Fatalf("stale blocked conclusion: %+v %v", r, err)
		}
	}
	if s.gc.cursor != 0 {
		t.Fatal("round continued beyond captured tail")
	}
	if r := finishTestGC(t, s, 1); r.State != BlockedByReaders {
		t.Fatal(r)
	}
}

func TestMetadataManagementNeverSchedulesGC(t *testing.T) {
	s, path := newTestStore(t, 16, nil)
	stopTestGC(s)
	putPages(t, s, []uint64{1}, 1)
	if err := s.Checkpoint(ctx, "delete"); err != nil {
		t.Fatal(err)
	}
	putPages(t, s, []uint64{1}, 2)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	m, err := OpenMetadata(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, err = m.ListCheckpoints(ctx); err != nil {
		t.Fatal(err)
	}
	if err = m.DeleteCheckpoint(ctx, "delete"); err != nil {
		t.Fatal(err)
	}
	before := m.Stats()
	time.Sleep(350 * time.Millisecond) // exceeds the active store startup interval
	after := m.Stats()
	if after.Commits != before.Commits || after.GCPunchCalls != 0 || after.GCProcessed != 0 {
		t.Fatal("metadata management started GC")
	}
	if m.ctrl.Protected != 0 {
		t.Fatal("deletion did not promptly withdraw unpinned protection")
	}
	if r := finishTestGC(t, m, 1); r.State != Complete || r.FreedSlots == 0 {
		t.Fatal(r)
	}
}

func TestGCBudgetOneFairWithContinuouslyAddedGarbage(t *testing.T) {
	s, _ := newTestStore(t, 16, nil)
	stopTestGC(s)
	putPages(t, s, []uint64{1, 2, 3}, 1)
	if err := s.Checkpoint(ctx, "keep"); err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		if err := s.Checkpoint(ctx, fmt.Sprintf("deleted-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	putPages(t, s, []uint64{1, 2, 3}, 2)
	for i := range 20 {
		if err := s.DeleteCheckpoint(ctx, fmt.Sprintf("deleted-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	var freed uint64
	for step := 0; s.ctrl.Pending != 0 && step < 200; step++ {
		progress := s.gc
		// This new page was born after every retained version. Its withdrawal
		// continually replenishes the global chain while version work is pending.
		putPages(t, s, []uint64{8}, byte(step))
		if err := s.Discard(ctx, 8*PageSize, PageSize); err != nil {
			t.Fatal(err)
		}
		if s.gc != progress {
			t.Fatal("ordinary I/O reset the in-memory scan")
		}
		if s.ctrl.Garbage == 0 {
			t.Fatal("fixture did not replenish global garbage")
		}
		r, err := s.GC(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		freed += r.FreedSlots
	}
	if s.ctrl.Pending != 0 || freed == 0 {
		t.Fatalf("budget-one classes starved: pending=%d freed=%d", s.ctrl.Pending, freed)
	}
	if result := finishTestGC(t, s, 1); result.State != Complete {
		t.Fatal(result)
	}
	v, err := s.Export(ctx, "keep")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	checkView(t, v, map[uint64][]byte{1: pageFor(1, 1), 2: pageFor(2, 1), 3: pageFor(3, 1)})
	checkActive(t, s, map[uint64][]byte{1: pageFor(1, 2), 2: pageFor(2, 2), 3: pageFor(3, 2)})
}

func TestBackgroundGCDrainsMoreAfterReaderCloses(t *testing.T) {
	s, _ := newTestStore(t, 256, nil)
	stopTestGC(s)
	pages := make([]uint64, 256)
	for i := range pages {
		pages[i] = uint64(i)
	}
	putPages(t, s, pages, 1)
	if err := s.Checkpoint(ctx, "reader"); err != nil {
		t.Fatal(err)
	}
	v, err := s.Export(ctx, "reader")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	putPages(t, s, pages, 2)
	if err = s.DeleteCheckpoint(ctx, "reader"); err != nil {
		t.Fatal(err)
	}
	if r := finishTestGC(t, s, 64); r.State != BlockedByReaders {
		t.Fatal(r)
	}
	s.startGC()
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		complete, fatal := s.ctrl.Pending == 0 && s.ctrl.Garbage == 0, s.fatal
		s.mu.Unlock()
		if fatal != nil {
			t.Fatal(fatal)
		}
		if complete {
			if s.Stats().GCFreed <= MaxBatchPages {
				t.Fatal("fixture did not require multiple progressing batches")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not continue without another deletion or explicit GC")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Exercise physical address ordering independently of logical B+ tree order.
// Positions around 128 cross a descriptor page; 130 is deliberately live.
func gcPhysicalFixture(t *testing.T) (*Store, string, []uint64) {
	t.Helper()
	s, path := newTestStore(t, 16, nil)
	stopTestGC(s)
	err := s.mutate(ctx, func(tx *transaction) error {
		for range 132 {
			if _, err := tx.allocate(kindData); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slots := []uint64{132, 128, 127, 131, 129}
	err = s.mutate(ctx, func(tx *transaction) error {
		for i, slot := range slots {
			next := uint64(0)
			if i+1 < len(slots) {
				next = slots[i+1]
			}
			if err := tx.setDesc(slot, descriptor{Kind: kindGarbage, Next: next}); err != nil {
				return err
			}
		}
		tx.ctrl.Garbage = slots[0]
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return s, path, slots
}

func TestGCPhysicalCoalescingAndAscendingReuse(t *testing.T) {
	s, _, slots := gcPhysicalFixture(t)
	before := s.Stats()
	r, err := s.GC(ctx, 64)
	if err != nil || r.State != Complete || r.FreedSlots != uint64(len(slots)) {
		t.Fatal(r, err)
	}
	after := s.Stats()
	if after.GCPunchCalls-before.GCPunchCalls != 3 {
		t.Fatalf("must split descriptor and live page: %+v", after)
	}
	sort.Slice(slots, func(i, j int) bool { return slotPage(slots[i]) < slotPage(slots[j]) })
	err = s.mutate(ctx, func(tx *transaction) error {
		for _, want := range slots {
			got, e := tx.allocate(kindData)
			if e != nil {
				return e
			}
			if got != want {
				return fmt.Errorf("reuse %d, want %d", got, want)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGCPunchFailureDoesNotPublishFreeSlots(t *testing.T) {
	for _, stage := range []string{"second-punch", "gc-punched"} {
		t.Run(stage, func(t *testing.T) {
			s, path, slots := gcPhysicalFixture(t)
			fired := 0
			s.beforeIO = func(actual string) error {
				if actual == "gc-punch" {
					fired++
				}
				if stage == "second-punch" && actual == "gc-punch" && fired == 2 || actual == stage {
					return os.ErrPermission
				}
				return nil
			}
			r, err := s.GC(ctx, 64)
			if !errors.Is(err, os.ErrPermission) || r.FreedSlots != 0 || s.ctrl.Free != 0 || s.ctrl.Garbage == 0 {
				t.Fatalf("published incomplete reclaim: %+v %v", r, err)
			}
			_ = s.Close()
			m, err := OpenMetadata(path)
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			if r := finishTestGC(t, m, 64); r.State != Complete || r.FreedSlots != uint64(len(slots)) {
				t.Fatal(r)
			}
		})
	}
}

func TestGCRejectsDuplicateCandidateBeforePunch(t *testing.T) {
	s, _, slots := gcPhysicalFixture(t)
	err := s.mutate(ctx, func(tx *transaction) error {
		d, e := tx.desc(slots[1])
		if e != nil {
			return e
		}
		d.Next = slots[0]
		return tx.setDesc(slots[1], d)
	})
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.GC(ctx, 64)
	if err == nil || r.FreedSlots != 0 || s.Stats().GCPunchCalls != 0 {
		t.Fatalf("invalid candidates punched: %+v %v", r, err)
	}
}
