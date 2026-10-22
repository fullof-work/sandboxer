package diffstore

import (
	"fmt"
	"testing"
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
	err = s.mutate(ctx, func(tx *transaction) error { tx.ctrl.GCScan = tx.ctrl.Tail; return nil })
	tail := s.ctrl.Tail
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
	if err != nil || cp.Previous == previous || protected != tail {
		t.Fatalf("bounded traversal did not persist progress/conservative protection: %+v %d %v", cp, protected, err)
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
