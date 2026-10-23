package versioned

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"

	"golang.org/x/sys/unix"
)

func TestPresenceSparseZeroAndCurrentRoot(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%v", encrypted), func(t *testing.T) {
			var key *[32]byte
			if encrypted {
				key = &[32]byte{1, 3, 7}
			}
			s, path := newTestStore(t, 1024, key)
			stopTestGC(s)
			putPages(t, s, []uint64{2, 64, 255, 256, 1023}, 1)
			if err := s.WritePages(ctx, []Page{{Block: 3, Data: make([]byte, PageSize)}}); err != nil {
				t.Fatal(err)
			}
			if err := s.Checkpoint(ctx, "before"); err != nil {
				t.Fatal(err)
			}
			if err := s.Discard(ctx, 64*PageSize, PageSize); err != nil {
				t.Fatal(err)
			}
			putPages(t, s, []uint64{65}, 2)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err := Open(path, Options{Key: key})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			stopTestGC(s)
			before := s.Stats()
			want := map[uint64]bool{2: true, 3: true, 65: true, 255: true, 256: true, 1023: true}
			for start := uint64(0); start < 1024; start += MaxPresencePages {
				present := make([]bool, MaxPresencePages)
				// Reused destination storage must also clear holes.
				for i := range present {
					present[i] = true
				}
				if err := s.Presence(ctx, start, present); err != nil {
					t.Fatal(err)
				}
				for i, got := range present {
					block := start + uint64(i)
					if got != want[block] {
						t.Fatalf("block %d presence=%v want=%v", block, got, want[block])
					}
				}
			}
			after := s.Stats()
			if after.DataRead != before.DataRead || after.DataWritten != before.DataWritten || after.IndexWritten != before.IndexWritten || after.RedoWritten != before.RedoWritten {
				t.Fatalf("presence changed payload or metadata: before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestPresenceBoundsCancellationAndClose(t *testing.T) {
	s, _ := newTestStore(t, 1024, nil)
	for _, tc := range []struct {
		start uint64
		count int
	}{{0, MaxPresencePages + 1}, {1024, 1}, {1025, 0}, {1023, 2}, {math.MaxUint64, 1}} {
		if err := s.Presence(ctx, tc.start, make([]bool, tc.count)); err == nil {
			t.Fatalf("accepted presence range %d+%d", tc.start, tc.count)
		}
		if err := s.Err(); err != nil {
			t.Fatal("invalid range poisoned owner", err)
		}
	}
	if err := s.Presence(ctx, 1024, nil); err != nil {
		t.Fatal("empty end range", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	before := s.Stats()
	if err := s.Presence(cancelled, 0, make([]bool, 1)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s.Stats() != before || s.Err() != nil {
		t.Fatal("cancelled query performed I/O or poisoned owner")
	}
	present := []bool{true}
	if err := s.Presence(ctx, 0, present); err != nil || present[0] {
		t.Fatal("empty store", present, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Presence(ctx, 0, present); !errors.Is(err, os.ErrClosed) {
		t.Fatal("closed owner", err)
	}
}

func fillPresenceTree(t *testing.T, s *Store, blocks int) {
	t.Helper()
	pages := make([]Page, blocks)
	for i := range pages {
		pages[i] = Page{Block: uint64(i), Data: pageFor(uint64(i), 1)}
	}
	if err := s.WritePages(ctx, pages); err != nil {
		t.Fatal(err)
	}
}

func TestPresenceReadsOnlyRequestedIndexRange(t *testing.T) {
	s, path := newTestStore(t, 4096, nil)
	stopTestGC(s)
	fillPresenceTree(t, s, 4096)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	stopTestGC(s)
	if got := s.Stats().IndexRead; got != PageSize {
		t.Fatalf("open read %d index bytes, want one root page", got)
	}
	before := s.Stats()
	present := make([]bool, MaxPresencePages)
	if err := s.Presence(ctx, 117, present); err != nil {
		t.Fatal(err)
	}
	for i, exists := range present {
		if !exists {
			t.Fatalf("missing block %d", 117+i)
		}
	}
	after := s.Stats()
	// Three intersecting leaves plus the immediate successor boundary.
	if read := after.IndexRead - before.IndexRead; read > 4*PageSize {
		t.Fatalf("bounded range read %d index bytes", read)
	}
	if after.DataRead != 0 {
		t.Fatal("presence read payload")
	}
	if err := s.Scan(ctx, func(uint64) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if s.Stats().IndexRead <= after.IndexRead {
		t.Fatal("range query had eagerly visited the complete index")
	}
}

func TestPresenceDefersUnrelatedCorruptionAndFailsSticky(t *testing.T) {
	s, path := newTestStore(t, 1024, nil)
	stopTestGC(s)
	fillPresenceTree(t, s, 1024)
	s.mu.Lock()
	n, err := readNode(storeReader{s}, s.ctrl.Root)
	s.mu.Unlock()
	if err != nil || n.leaf {
		t.Fatal("fixture must have child nodes", n.leaf, err)
	}
	last := n.entries[len(n.entries)-1].slot
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	corruptPresencePage(t, path, last, func(b []byte) []byte { b[0] ^= 1; return b })
	s, err = Open(path, Options{})
	if err != nil {
		t.Fatal("open scanned an unrelated child", err)
	}
	defer s.Close()
	if err := s.Presence(ctx, 0, make([]bool, 8)); err != nil {
		t.Fatal("query scanned an unrelated child", err)
	}
	err = s.Presence(ctx, 1023, make([]bool, 1))
	if err == nil || !errors.Is(s.Err(), err) {
		t.Fatal("corrupt requested child must fail sticky", err, s.Err())
	}
	if got := s.Presence(ctx, 0, make([]bool, 1)); got != err {
		t.Fatal("fatal owner continued serving", got)
	}
}

func corruptPresencePage(t *testing.T, path string, slot uint64, mutate func([]byte) []byte) {
	t.Helper()
	f, err := openData(path, unix.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b, err := readFull(f, slotPage(slot))
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFull(f, mutate(b), slotPage(slot)); err != nil {
		t.Fatal(err)
	}
}

func TestOpenValidatesRootBeforeLazyPresence(t *testing.T) {
	for _, corruption := range []string{"checksum", "capacity", "allocation"} {
		t.Run(corruption, func(t *testing.T) {
			s, path := newTestStore(t, 64, nil)
			stopTestGC(s)
			putPages(t, s, []uint64{1}, 1)
			root, next := s.ctrl.Root, s.ctrl.Next
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			var saved []byte
			corruptPresencePage(t, path, root, func(b []byte) []byte {
				saved = append([]byte(nil), b...)
				if corruption == "checksum" {
					b[0] ^= 1
					return b
				}
				n, err := decodeNode(b)
				if err != nil {
					t.Fatal(err)
				}
				if corruption == "capacity" {
					n.entries[0].key = 64
				} else {
					n.entries[0].slot = next
				}
				return encodeNode(n)
			})
			if broken, err := Open(path, Options{}); err == nil {
				broken.Close()
				t.Fatal("accepted corrupt root")
			}
			metadata, err := OpenMetadata(path)
			if err != nil {
				t.Fatal("corrupt active root prevented metadata-only cleanup", err)
			}
			if err := metadata.Close(); err != nil {
				t.Fatal(err)
			}
			corruptPresencePage(t, path, root, func([]byte) []byte { return saved })
			repaired, err := Open(path, Options{})
			if err != nil {
				t.Fatal("failed open leaked owner lock", err)
			}
			defer repaired.Close()
			present := make([]bool, 2)
			if err := repaired.Presence(ctx, 0, present); err != nil || !reflect.DeepEqual(present, []bool{false, true}) {
				t.Fatal(present, err)
			}
		})
	}
}

// Cancel deterministically between child-node visits, after the query has
// already found mappings. Cancellation must not become a sticky I/O failure.
type cancelPresenceContext struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *cancelPresenceContext) Err() error {
	c.checks--
	if c.checks == 0 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestPresenceCancellationDuringRangeWalk(t *testing.T) {
	s, _ := newTestStore(t, 1024, nil)
	stopTestGC(s)
	fillPresenceTree(t, s, 1024)
	cancelled, cancel := context.WithCancel(ctx)
	defer cancel()
	partial := make([]bool, MaxPresencePages)
	query := &cancelPresenceContext{Context: cancelled, cancel: cancel, checks: 4}
	if err := s.Presence(query, 117, partial); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s.Err() != nil {
		t.Fatal("in-progress cancellation poisoned owner", s.Err())
	}
	if !partial[0] || partial[len(partial)-1] {
		t.Fatal("fixture did not cancel between child-node visits")
	}
	if err := s.Presence(ctx, 117, partial); err != nil {
		t.Fatal("query did not recover from cancellation", err)
	}
	for i, exists := range partial {
		if !exists {
			t.Fatalf("successful retry missed block %d", 117+i)
		}
	}
}

func TestPresenceRejectsMismatchedChildRange(t *testing.T) {
	s, path := newTestStore(t, 1024, nil)
	stopTestGC(s)
	fillPresenceTree(t, s, 1024)
	s.mu.Lock()
	n, err := readNode(storeReader{s}, s.ctrl.Root)
	s.mu.Unlock()
	if err != nil || n.leaf {
		t.Fatal("fixture must have child nodes", err)
	}
	last := n.entries[len(n.entries)-1].slot
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	corruptPresencePage(t, path, last, func(b []byte) []byte {
		n, err := decodeNode(b)
		if err != nil {
			t.Fatal(err)
		}
		// Keep a valid checksum and ordering, but make the child disagree with
		// the parent's maximum key. Otherwise absence could be silently trusted.
		n.entries = n.entries[:len(n.entries)-1]
		return encodeNode(n)
	})
	s, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Presence(ctx, 1023, make([]bool, 1)); err == nil || s.Err() == nil {
		t.Fatal("accepted an absence answer from mismatched child metadata", err)
	}
}

func TestScanSparseLargeCapacity(t *testing.T) {
	const blocks = 8 * 1024 * 1024 // 32 GiB logical capacity; four allocated pages.
	s, path := newTestStore(t, blocks, nil)
	stopTestGC(s)
	want := []uint64{1, 256, blocks / 2, blocks - 1}
	putPages(t, s, want, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	stopTestGC(s)
	before := s.Stats()
	var got []uint64
	if err := s.Scan(ctx, func(block uint64) error { got = append(got, block); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("sparse mapping enumeration", got, want)
	}
	after := s.Stats()
	if after.IndexRead != before.IndexRead || after.DataRead != before.DataRead || after.DataWritten != before.DataWritten || after.IndexWritten != before.IndexWritten || after.RedoWritten != before.RedoWritten {
		t.Fatalf("scan of the cached single-leaf tree performed I/O: before=%+v after=%+v", before, after)
	}
}

func TestScanCancellationAndVisitorErrorsAreNotSticky(t *testing.T) {
	s, _ := newTestStore(t, 64, nil)
	putPages(t, s, []uint64{1, 2, 3}, 1)
	cancelled, cancel := context.WithCancel(ctx)
	defer cancel()
	seen := 0
	if err := s.Scan(cancelled, func(uint64) error {
		seen++
		cancel()
		return nil
	}); !errors.Is(err, context.Canceled) || seen != 1 {
		t.Fatal("scan ignored cancellation between visits", seen, err)
	}
	stop := errors.New("visitor stopped scan")
	if err := s.Scan(ctx, func(uint64) error { return stop }); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	if err := s.Err(); err != nil {
		t.Fatal("caller interruption poisoned owner", err)
	}
	var got []uint64
	if err := s.Scan(ctx, func(block uint64) error { got = append(got, block); return nil }); err != nil || !reflect.DeepEqual(got, []uint64{1, 2, 3}) {
		t.Fatal("healthy scan after interruption", got, err)
	}
}

func TestScanValidatesChildrenAndFailsSticky(t *testing.T) {
	for _, corruption := range []string{"checksum", "capacity", "allocation", "range"} {
		t.Run(corruption, func(t *testing.T) {
			s, path := newTestStore(t, 1024, nil)
			stopTestGC(s)
			fillPresenceTree(t, s, 1024)
			n, err := readNode(storeReader{s}, s.ctrl.Root)
			if err != nil || n.leaf {
				t.Fatal("fixture must have child nodes", err)
			}
			last, next := n.entries[len(n.entries)-1].slot, s.ctrl.Next
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			corruptPresencePage(t, path, last, func(b []byte) []byte {
				if corruption == "checksum" {
					b[0] ^= 1
					return b
				}
				n, err := decodeNode(b)
				if err != nil {
					t.Fatal(err)
				}
				switch corruption {
				case "capacity":
					n.entries[len(n.entries)-1].key = 1024
				case "allocation":
					n.entries[len(n.entries)-1].slot = next
				case "range":
					n.entries = n.entries[:len(n.entries)-1]
				}
				return encodeNode(n)
			})
			s, err = Open(path, Options{})
			if err != nil {
				t.Fatal("root-only open scanned child corruption", err)
			}
			defer s.Close()
			err = s.Scan(ctx, func(uint64) error { return nil })
			if err == nil || !errors.Is(s.Err(), err) {
				t.Fatal("scan accepted corruption or did not fail sticky", err, s.Err())
			}
			if got := s.Presence(ctx, 0, make([]bool, 1)); got != err {
				t.Fatal("fatal owner continued serving", got)
			}
		})
	}
}
