package versioned

import (
	"context"
	"errors"
	"testing"
)

// Add a valid intermediate level without allocating hundreds of MiB of data.
// The existing leaves and mappings are unchanged; only the root fanout differs.
func deepenPresenceTree(t *testing.T, s *Store) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	n, err := readNode(storeReader{s}, s.ctrl.Root)
	if err != nil || n.leaf || len(n.entries) < 4 {
		t.Fatal("expected a split index root", err)
	}
	err = s.mutate(ctx, func(tx *transaction) error {
		left, err := tx.allocate(kindIndex)
		if err != nil {
			return err
		}
		right, err := tx.allocate(kindIndex)
		if err != nil {
			return err
		}
		mid := len(n.entries) / 2
		tx.nodes[left] = node{entries: append([]entry(nil), n.entries[:mid]...)}
		tx.nodes[right] = node{entries: append([]entry(nil), n.entries[mid:]...)}
		tx.nodes[tx.ctrl.Root] = node{entries: []entry{{n.entries[mid-1].key, left}, {n.entries[len(n.entries)-1].key, right}}}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPresenceRejectsHiddenMappingsAcrossSkippedBoundaries(t *testing.T) {
	for _, mode := range []string{"predecessor", "last-child", "nested-predecessor", "recursive-predecessor", "recursive-last-child", "successor-low", "recursive-successor-low"} {
		t.Run(mode, func(t *testing.T) {
			s, path := newTestStore(t, 2048, nil)
			stopTestGC(s)
			low := mode == "successor-low" || mode == "recursive-successor-low"
			if low {
				// Leave holes from creation: deleting an entry after a split can
				// merge the neighboring leaves and invalidate the corruption fixture.
				pages := make([]Page, 1024)
				for i := range pages {
					pages[i] = Page{Block: uint64(2 * i), Data: pageFor(uint64(2*i), 1)}
				}
				if err := s.WritePages(ctx, pages); err != nil {
					t.Fatal(err)
				}
			} else {
				fillPresenceTree(t, s, 1024)
			}
			deep := mode == "nested-predecessor" || mode == "recursive-predecessor" || mode == "recursive-last-child" || mode == "recursive-successor-low"
			if deep {
				deepenPresenceTree(t, s)
			}
			root := s.ctrl.Root
			n, err := readNode(storeReader{s}, root)
			if err != nil {
				t.Fatal(err)
			}
			hidden := n.entries[0].key
			target := root
			index := 0
			var also uint64
			if mode == "last-child" || mode == "recursive-last-child" {
				index = len(n.entries) - 1
				hidden = n.entries[index].key
			}
			if mode == "nested-predecessor" {
				target = n.entries[0].slot
				child, err := readNode(storeReader{s}, target)
				if err != nil {
					t.Fatal(err)
				}
				hidden = child.entries[0].key
			}
			if mode == "recursive-predecessor" || mode == "recursive-last-child" {
				// Parent and child agree on the wrong separator; only following
				// the skipped subtree's rightmost path reaches the hidden tail.
				also = n.entries[index].slot
			}
			if low {
				hidden = n.entries[0].key - 1
				target = n.entries[1].slot
				if deep {
					child, err := readNode(storeReader{s}, target)
					if err != nil {
						t.Fatal(err)
					}
					target = child.entries[0].slot
				}
			}
			before := make([]bool, 1)
			if err := s.Presence(ctx, hidden, before); err != nil || before[0] == low {
				t.Fatal("unexpected healthy fixture presence", hidden, before, err)
			}
			if err := s.Scan(ctx, func(uint64) error { return nil }); err != nil {
				t.Fatal("fixture is not a valid complete tree before corruption", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			corruptPresencePage(t, path, target, func(b []byte) []byte {
				n, err := decodeNode(b)
				if err != nil {
					t.Fatal(err)
				}
				if low {
					n.entries[0].key = hidden
				} else {
					n.entries[index].key--
				}
				return encodeNode(n) // Valid checksum and sorted entries.
			})
			if also != 0 {
				corruptPresencePage(t, path, also, func(b []byte) []byte {
					n, err := decodeNode(b)
					if err != nil {
						t.Fatal(err)
					}
					n.entries[len(n.entries)-1].key--
					return encodeNode(n)
				})
			}
			s, err = Open(path, Options{})
			if err != nil {
				t.Fatal("root-only open unexpectedly scanned children", err)
			}
			defer s.Close()
			found := false
			err = walkTree(ctx, storeReader{s}, s.ctrl.Root, func(e entry) error {
				found = found || e.key == hidden
				return nil
			}, 0)
			if err != nil || !found {
				t.Fatal("fixture has no hidden mapping", hidden, err)
			}
			present := make([]bool, 1)
			err = s.Presence(ctx, hidden, present)
			if err == nil {
				t.Fatalf("hidden mapped block %d returned present=%v without reporting corrupt boundary", hidden, present[0])
			}
			if !errors.Is(s.Err(), err) {
				t.Fatal("boundary error was not sticky", err, s.Err())
			}
			if again := s.Presence(ctx, 0, make([]bool, 1)); again != err {
				t.Fatal("fatal owner continued serving", again)
			}
		})
	}
}

func TestPresenceBoundaryValidationRemainsBounded(t *testing.T) {
	s, path := newTestStore(t, 8*1024*1024, nil)
	stopTestGC(s)
	fillPresenceTree(t, s, 1024)
	deepenPresenceTree(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	before := s.Stats()
	if before.IndexRead != PageSize {
		t.Fatal("open scanned past root", before)
	}
	present := make([]bool, 1)
	if err := s.Presence(ctx, 8*1024*1024-1, present); err != nil || present[0] {
		t.Fatal(present, err)
	}
	after := s.Stats()
	if got := after.IndexRead - before.IndexRead; got != 2*PageSize {
		t.Fatalf("tail absence must validate exactly one right boundary path, read=%d", got)
	}
	if after.DataRead != 0 {
		t.Fatal("boundary validation read payload")
	}
}

func TestPresenceCancellationDuringBoundaryValidation(t *testing.T) {
	s, path := newTestStore(t, 2048, nil)
	stopTestGC(s)
	fillPresenceTree(t, s, 1024)
	deepenPresenceTree(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cancelled, cancel := context.WithCancel(ctx)
	defer cancel()
	// healthy, query root, intermediate boundary, then cancellation before
	// the boundary leaf: interrupt the additional validation path itself.
	query := &cancelPresenceContext{Context: cancelled, cancel: cancel, checks: 4}
	before := s.Stats()
	present := make([]bool, 1)
	if err := s.Presence(query, 2047, present); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s.Err() != nil {
		t.Fatal("boundary cancellation poisoned owner", s.Err())
	}
	if got := s.Stats().IndexRead - before.IndexRead; got != PageSize {
		t.Fatalf("fixture must cancel between boundary nodes, read=%d", got)
	}
	if err := s.Presence(ctx, 2047, present); err != nil || present[0] {
		t.Fatal("boundary retry failed", present, err)
	}
}
