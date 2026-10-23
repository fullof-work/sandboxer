package versioned

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"golang.org/x/sys/unix"
)

var ctx = context.Background()

func newTestStore(t *testing.T, blocks int, key *[32]byte) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.diff")
	s, err := Create(path, Options{DiffID: "test", BaseID: "immutable-base", LogicalSize: int64(blocks) * PageSize, Key: key})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}
func pageFor(block uint64, generation byte) []byte {
	b := bytes.Repeat([]byte{generation}, PageSize)
	le.PutUint64(b, block)
	return b
}
func putPages(t *testing.T, s *Store, blocks []uint64, generation byte) {
	t.Helper()
	pages := make([]Page, len(blocks))
	for i, b := range blocks {
		pages[i] = Page{b, pageFor(b, generation)}
	}
	if err := s.WritePages(ctx, pages); err != nil {
		t.Fatal(err)
	}
}
func checkView(t *testing.T, v *View, want map[uint64][]byte) {
	t.Helper()
	if err := compareView(v, want); err != nil {
		t.Fatal(err)
	}
}

// The same full-content oracle is used by ordinary correctness tests and the
// deliberately broken retained-root fixture below. This guards against a test
// suite that checks only metadata/counts and misses historical payload damage.
func compareView(v *View, want map[uint64][]byte) error {
	seen := map[uint64]bool{}
	b := make([]byte, PageSize)
	for off := uint64(0); off < v.Size(); {
		r, err := v.RunAt(off, v.Size()-off)
		if err != nil {
			return err
		}
		if r.Offset() != off || r.End() <= off || r.End() > v.Size() {
			return fmt.Errorf("invalid run: %d..%d", r.Offset(), r.End())
		}
		for pos := off; pos < r.End(); pos += PageSize {
			block := pos / PageSize
			expected, ok := want[block]
			if ok != (r.Kind() == sparse.Data) {
				return fmt.Errorf("block %d presence: %v != %v", block, r.Kind(), ok)
			}
			n, err := r.ReadAt(ctx, b, pos-off)
			if err != nil || n != PageSize {
				return fmt.Errorf("block %d read: %d %v", block, n, err)
			}
			if !ok {
				expected = make([]byte, PageSize)
			} else {
				seen[block] = true
			}
			if !bytes.Equal(b, expected) {
				return fmt.Errorf("block %d content mismatch: got %x want %x", block, b[:16], expected[:16])
			}
		}
		off = r.End()
	}
	if len(seen) != len(want) {
		return fmt.Errorf("export mapping count %d want %d", len(seen), len(want))
	}
	return nil
}
func snapshotModel(m map[uint64][]byte) map[uint64][]byte {
	n := make(map[uint64][]byte, len(m))
	for k, v := range m {
		n[k] = append([]byte(nil), v...)
	}
	return n
}
func checkActive(t *testing.T, s *Store, want map[uint64][]byte) {
	t.Helper()
	v, err := s.Export(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	checkView(t, v, want)
}

func TestVersionsAndRandomOperations(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%v", encrypted), func(t *testing.T) {
			var key *[32]byte
			if encrypted {
				key = &[32]byte{1, 3, 7}
			}
			s, path := newTestStore(t, 2048, key)
			r := rand.New(rand.NewSource(327))
			active := map[uint64][]byte{}
			versions := map[string]map[uint64][]byte{}
			for round := 0; round < 120; round++ {
				switch {
				case round%7 == 0:
					id := fmt.Sprintf("cp-%d", round)
					before := s.Stats()
					if err := s.Checkpoint(ctx, id); err != nil {
						t.Fatal(err)
					}
					after := s.Stats()
					if before.DataRead != after.DataRead || before.DataWritten != after.DataWritten {
						t.Fatal("checkpoint touched payload")
					}
					versions[id] = snapshotModel(active)
				case round%11 == 0 && len(versions) > 2:
					var ids []string
					for id := range versions {
						ids = append(ids, id)
					}
					sort.Strings(ids)
					id := ids[r.Intn(len(ids))]
					if err := s.DeleteCheckpoints(ctx, []string{id}); err != nil {
						t.Fatal(err)
					}
					delete(versions, id)
					if err := s.DeleteCheckpoints(ctx, []string{id}); err != nil {
						t.Fatal("retry", err)
					}
					if err := s.Checkpoint(ctx, id); !errors.Is(err, ErrExists) {
						t.Fatal("ID reuse", err)
					}
				case round%5 == 0:
					block := uint64(r.Intn(2048))
					count := min(24, 2048-int(block))
					if err := s.Discard(ctx, int64(block)*PageSize, int64(count)*PageSize); err != nil {
						t.Fatal(err)
					}
					for i := 0; i < count; i++ {
						delete(active, block+uint64(i))
					}
				default:
					pages := make([]Page, 64)
					for i := range pages {
						block := uint64(r.Intn(2048))
						data := pageFor(block, byte(round))
						if i%9 == 0 {
							clear(data)
						}
						pages[i] = Page{block, data}
						active[block] = append([]byte(nil), data...)
					}
					if err := s.WritePages(ctx, pages); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := s.GC(ctx, 64); err != nil {
					t.Fatal(err)
				}
				if round%20 == 19 {
					checkActive(t, s, active)
					for id, want := range versions {
						v, err := s.Export(ctx, id)
						if err != nil {
							t.Fatal(err)
						}
						checkView(t, v, want)
						v.Close()
					}
				}
			}
			checkActive(t, s, active)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err := Open(path, Options{Key: key})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			checkActive(t, s, active)
			for id, want := range versions {
				v, err := OpenExport(ctx, path, id, key)
				if err != nil {
					t.Fatal(err)
				}
				checkView(t, v, want)
				v.Close()
			}
		})
	}
}

func TestPrivateOverwriteAndPhysicalAggregation(t *testing.T) {
	s, _ := newTestStore(t, 4096, &[32]byte{9})
	blocks := []uint64{900, 1, 3000, 4, 1023, 99, 1000, 2048}
	putPages(t, s, blocks, 1)
	if err := s.Checkpoint(ctx, "one"); err != nil {
		t.Fatal(err)
	}
	putPages(t, s, blocks, 2)
	before := s.Stats()
	putPages(t, s, blocks, 3)
	after := s.Stats()
	if after.Allocated != before.Allocated || after.IndexWritten != before.IndexWritten || after.RedoWritten != before.RedoWritten {
		t.Fatalf("private overwrite wrote metadata: before=%+v after=%+v", before, after)
	}
	v, err := s.Export(ctx, "one")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	expected := map[uint64][]byte{}
	for _, b := range blocks {
		expected[b] = pageFor(b, 1)
	}
	checkView(t, v, expected)
	expected = map[uint64][]byte{}
	for _, b := range blocks {
		expected[b] = pageFor(b, 3)
	}
	checkActive(t, s, expected)
	s.mu.Lock()
	defer s.mu.Unlock()
	var slots []uint64
	for _, b := range blocks {
		slot, err := lookup(storeReader{s}, s.ctrl.Root, b)
		if err != nil {
			t.Fatal(err)
		}
		slots = append(slots, slot)
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	for i := 1; i < len(slots); i++ {
		if slots[i] != slots[i-1]+1 {
			t.Fatalf("data slots not grouped: %v", slots)
		}
	}
}

func TestDeletedReaderRetainsCoWAndSurvivesOwner(t *testing.T) {
	s, path := newTestStore(t, 128, nil)
	putPages(t, s, []uint64{1, 19, 80}, 1)
	if err := s.Checkpoint(ctx, "pinned"); err != nil {
		t.Fatal(err)
	}
	a, err := OpenExport(ctx, path, "pinned", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := s.Export(ctx, "pinned")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err = s.DeleteCheckpoints(ctx, []string{"pinned"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Export(ctx, "pinned"); !errors.Is(err, ErrNotExist) {
		t.Fatalf("new deleted reader: %v", err)
	}
	for i := 0; i < 4; i++ {
		if _, err = s.GC(ctx, 64); err != nil {
			t.Fatal(err)
		}
	}
	putPages(t, s, []uint64{1, 19, 80}, 2)
	want := map[uint64][]byte{}
	for _, block := range []uint64{1, 19, 80} {
		want[block] = pageFor(block, 1)
	}
	checkView(t, a, want)
	checkView(t, b, want)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	putPages(t, s, []uint64{1, 19, 80}, 3)
	checkView(t, a, want)
	checkView(t, b, want)
	a.Close()
	for i := 0; i < 3; i++ {
		s.GC(ctx, 64)
	}
	checkView(t, b, want)
	b.Close()
	for i := 0; i < 16; i++ {
		if _, err = s.GC(ctx, 64); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.Lock()
	_, cp, err := s.findCheckpoint("pinned", nil)
	retained := cp.Retained
	pending := s.ctrl.Pending
	s.mu.Unlock()
	if err != nil || retained || pending != 0 {
		t.Fatalf("reader release not reclaimed: %v %+v pending=%d", err, cp, pending)
	}
}

func TestBatchWithdrawalSkipsDeletedPredecessors(t *testing.T) {
	s, _ := newTestStore(t, 128, nil)
	putPages(t, s, []uint64{1, 4, 8}, 1)
	var ids []string
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("v%d", i)
		ids = append(ids, id)
		if err := s.Checkpoint(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	putPages(t, s, []uint64{1, 4, 8}, 2)
	// Persist a non-head cursor to reproduce reverse deletion's worst ordering.
	s.mu.Lock()
	err := s.mutate(ctx, func(tx *transaction) error { tx.ctrl.GCScan = tx.ctrl.Tail; return nil })
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	for i, j := 0, len(ids)-1; i < j; i, j = i+1, j-1 {
		ids[i], ids[j] = ids[j], ids[i]
	}
	if err = s.DeleteCheckpoints(ctx, ids); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	if s.ctrl.Protected != 0 {
		t.Fatal("batch retained an unpinned member")
	}
	s.mu.Unlock()
	before := s.Stats()
	for i := 0; i < 32; i++ {
		if _, err = s.GC(ctx, 64); err != nil {
			t.Fatal(err)
		}
	}
	after := s.Stats()
	if after.GCProcessed-before.GCProcessed > 16 {
		t.Fatalf("retirement repeatedly forwarded: %d", after.GCProcessed-before.GCProcessed)
	}
	before = after
	putPages(t, s, []uint64{40, 41, 42}, 3)
	after = s.Stats()
	if after.Reused == before.Reused {
		t.Fatal("freed slots were not reused")
	}
}

func TestBPlusSplitMergeAndHoles(t *testing.T) {
	s, _ := newTestStore(t, 4096, nil)
	blocks := make([]uint64, 1600)
	for i := range blocks {
		blocks[i] = uint64(i)
	}
	putPages(t, s, blocks, 8)
	if err := s.Checkpoint(ctx, "split"); err != nil {
		t.Fatal(err)
	}
	if err := s.Discard(ctx, 100*PageSize, 1300*PageSize); err != nil {
		t.Fatal(err)
	}
	want := map[uint64][]byte{}
	for _, b := range blocks {
		if b < 100 || b >= 1400 {
			want[b] = pageFor(b, 8)
		}
	}
	checkActive(t, s, want)
	v, err := s.Export(ctx, "split")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	want = map[uint64][]byte{}
	for _, b := range blocks {
		want[b] = pageFor(b, 8)
	}
	checkView(t, v, want)
	var seen []uint64
	if err = s.Scan(ctx, func(b uint64) error { seen = append(seen, b); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 300 {
		t.Fatalf("scan count %d", len(seen))
	}
}

func TestHeaderCapacityAuthenticationAndOwner(t *testing.T) {
	key := &[32]byte{7}
	s, path := newTestStore(t, 1024, key)
	info, err := Probe(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.LogicalSize != 1024*PageSize || !info.Encrypted {
		t.Fatalf("info %+v", info)
	}
	if _, err = Open(path, Options{Key: key}); !errors.Is(err, ErrBusy) {
		t.Fatalf("second writer %v", err)
	}
	if _, err = OpenExport(ctx, path, "", nil); !errors.Is(err, ErrEncryptionRequired) {
		t.Fatal(err)
	}
	if _, err = OpenExport(ctx, path, "", &[32]byte{8}); !errors.Is(err, ErrAuthentication) {
		t.Fatal(err)
	}
	if err = s.Checkpoint(ctx, "current"); err != nil {
		t.Fatal(err)
	}
	if !s.CanResume("current") {
		t.Fatal("missing current")
	}
	if err = s.MarkModified(ctx); err != nil {
		t.Fatal(err)
	}
	if s.CanResume("current") {
		t.Fatal("stale current")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(path, Options{Key: key, BaseID: "wrong"}); err == nil {
		t.Fatal("base mismatch accepted")
	}
	m, err := OpenMetadata(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ids, err := m.ListCheckpoints(ctx)
	if err != nil || !reflect.DeepEqual(ids, []string{"current"}) {
		t.Fatalf("metadata list %v %v", ids, err)
	}
}

func TestRedoFailureBoundariesPreserveVersionContents(t *testing.T) {
	stages := []string{"data", "new-index", "redo-reset", "redo-index", "redo-images", "redo-commit", "apply", "redo-clear"}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			s, path := newTestStore(t, 512, nil)
			blocks := make([]uint64, 300)
			want := map[uint64][]byte{}
			for i := range blocks {
				blocks[i] = uint64(i)
				want[uint64(i)] = pageFor(uint64(i), 1)
			}
			putPages(t, s, blocks, 1)
			if err := s.Checkpoint(ctx, "stable"); err != nil {
				t.Fatal(err)
			}
			if err := s.MarkModified(ctx); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected I/O failure")
			s.mu.Lock()
			s.beforeIO = func(actual string) error {
				if actual == stage {
					return injected
				}
				return nil
			}
			s.mu.Unlock()
			err := s.WritePages(ctx, []Page{{1, pageFor(1, 2)}, {200, pageFor(200, 2)}})
			if !errors.Is(err, injected) {
				t.Fatalf("did not inject %s: %v", stage, err)
			}
			s.Close()
			s, err = Open(path, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			v, err := s.Export(ctx, "stable")
			if err != nil {
				t.Fatal(err)
			}
			checkView(t, v, want)
			v.Close()
			active := snapshotModel(want)
			if stage == "apply" || stage == "redo-clear" {
				active[1] = pageFor(1, 2)
				active[200] = pageFor(200, 2)
			}
			checkActive(t, s, active)
		})
	}
}

func TestGCInterruptionAndRetirement(t *testing.T) {
	s, path := newTestStore(t, 64, nil)
	putPages(t, s, []uint64{0, 1, 2}, 1)
	s.Checkpoint(ctx, "old")
	putPages(t, s, []uint64{0, 1, 2}, 2)
	s.DeleteCheckpoints(ctx, []string{"old"})
	s.GC(ctx, 64)
	s.mu.Lock()
	s.beforeIO = func(stage string) error {
		if stage == "gc-punch" {
			return unix.ENOSPC
		}
		return nil
	}
	s.mu.Unlock()
	if _, err := s.GC(ctx, 64); !errors.Is(err, unix.ENOSPC) {
		t.Fatal(err)
	}
	s.Close()
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 8; i++ {
		if _, err = s.GC(ctx, 64); err != nil {
			t.Fatal(err)
		}
	}
	v, err := s.Export(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Retire(ctx); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	if _, err = OpenExport(ctx, path, "", nil); !errors.Is(err, ErrRetired) {
		t.Fatal(err)
	}
	v.Close()
	if err = s.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
}

func TestDirectIOAndDirectoryLocks(t *testing.T) {
	s, _ := newTestStore(t, 32, nil)
	flags, err := unix.FcntlInt(s.file.Fd(), unix.F_GETFL, 0)
	if err != nil || flags&unix.O_DIRECT == 0 {
		t.Fatalf("not direct: %x %v", flags, err)
	}
	var fs unix.Statfs_t
	if err = unix.Fstatfs(int(s.file.Fd()), &fs); err != nil {
		t.Fatal(err)
	}
	t.Logf("filesystem type=%x; O_DIRECT=true", fs.Type)
	if fs.Type == unix.TMPFS_MAGIC {
		t.Fatal("direct-I/O acceptance must use a real Linux filesystem")
	}
	dir := t.TempDir()
	a, err := LockDirectory(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err = LockDirectory(dir); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	a.Close()
	b, err := LockDirectory(dir)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
}

func TestViewBounds(t *testing.T) {
	s, _ := newTestStore(t, 8, nil)
	putPages(t, s, []uint64{1}, 1)
	v, err := s.Export(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if _, err = v.RunAt(0, 0); err == nil {
		t.Fatal("zero limit")
	}
	b := make([]byte, PageSize)
	n, err := v.ReadAt(ctx, b, v.Size()-8)
	if n != 8 || !errors.Is(err, io.EOF) {
		t.Fatalf("clip %d %v", n, err)
	}
}
