package versioned

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
)

const maxEntries = 255
const maxDepth = 8

type entry struct{ key, slot uint64 }
type node struct {
	leaf    bool
	entries []entry
}

func encodeNode(n node) []byte {
	b := make([]byte, PageSize)
	copy(b, "KDBTREE2")
	le.PutUint16(b[8:], uint16(len(n.entries)))
	if n.leaf {
		b[10] = 1
	}
	for i, e := range n.entries {
		off := 16 + i*16
		le.PutUint64(b[off:], e.key)
		le.PutUint64(b[off+8:], e.slot)
	}
	h := crc32.NewIEEE()
	h.Write(b[:12])
	h.Write(b[16:])
	le.PutUint32(b[12:], h.Sum32())
	return b
}
func decodeNode(b []byte) (node, error) {
	if len(b) != PageSize || string(b[:8]) != "KDBTREE2" {
		return node{}, fmt.Errorf("diffstore: invalid B+ page")
	}
	h := crc32.NewIEEE()
	h.Write(b[:12])
	h.Write(b[16:])
	if h.Sum32() != le.Uint32(b[12:]) {
		return node{}, fmt.Errorf("diffstore: B+ checksum mismatch")
	}
	count := int(le.Uint16(b[8:]))
	if count == 0 || count > maxEntries || b[10] > 1 {
		return node{}, fmt.Errorf("diffstore: invalid B+ occupancy")
	}
	n := node{leaf: b[10] != 0, entries: make([]entry, count)}
	for i := range n.entries {
		off := 16 + i*16
		n.entries[i] = entry{le.Uint64(b[off:]), le.Uint64(b[off+8:])}
		if n.entries[i].slot == 0 || i > 0 && n.entries[i-1].key >= n.entries[i].key {
			return node{}, fmt.Errorf("diffstore: invalid B+ ordering")
		}
	}
	return n, nil
}

type pageReader interface{ read(uint64) ([]byte, error) }
type storeReader struct{ s *Store }

func (r storeReader) read(p uint64) ([]byte, error) { return r.s.readPage(p) }
func readNode(r pageReader, slot uint64) (node, error) {
	n, err := inspectNode(r, slot)
	if err == nil {
		n.entries = append([]entry(nil), n.entries...)
	}
	return n, err
}
func inspectNode(r pageReader, slot uint64) (node, error) {
	var cache *pageCache
	var stats *Stats
	switch source := r.(type) {
	case storeReader:
		cache = source.s.cache
		stats = &source.s.stats
	case *View:
		cache = source.cache
	case *transaction:
		if n, ok := source.nodes[slot]; ok {
			return n, nil
		}
		cache = source.store.cache
		stats = &source.store.stats
	}
	if cache != nil {
		if n, ok := cache.getNode(slotPage(slot)); ok {
			if stats != nil {
				stats.CacheHits++
			}
			return n, nil
		}
	}
	b, err := r.read(slotPage(slot))
	if err != nil {
		return node{}, err
	}
	n, err := decodeNode(b)
	if err == nil && cache != nil {
		cache.putNode(slotPage(slot), n)
	}
	return n, err
}
func lookup(r pageReader, root, key uint64) (uint64, error) {
	for depth := 0; root != 0; depth++ {
		if depth >= maxDepth {
			return 0, fmt.Errorf("diffstore: B+ depth exceeded")
		}
		n, err := inspectNode(r, root)
		if err != nil {
			return 0, err
		}
		i := sort.Search(len(n.entries), func(i int) bool { return n.entries[i].key >= key })
		if i == len(n.entries) {
			return 0, nil
		}
		e := n.entries[i]
		if n.leaf {
			if e.key == key {
				return e.slot, nil
			}
			return 0, nil
		}
		root = e.slot
	}
	return 0, nil
}

// successor traverses with a bounded recursion stack; there are no leaf links
// requiring additional CoW when adjacent leaves change.
func successor(r pageReader, root, key uint64, depth int) (entry, bool, error) {
	if root == 0 {
		return entry{}, false, nil
	}
	if depth >= maxDepth {
		return entry{}, false, fmt.Errorf("diffstore: B+ depth exceeded")
	}
	n, err := inspectNode(r, root)
	if err != nil {
		return entry{}, false, err
	}
	i := sort.Search(len(n.entries), func(i int) bool { return n.entries[i].key >= key })
	if n.leaf {
		if i == len(n.entries) {
			return entry{}, false, nil
		}
		return n.entries[i], true, nil
	}
	for ; i < len(n.entries); i++ {
		e, ok, err := successor(r, n.entries[i].slot, key, depth+1)
		if err != nil || ok {
			return e, ok, err
		}
	}
	return entry{}, false, nil
}
func walkTree(ctx context.Context, r pageReader, root uint64, visit func(entry) error, depth int) error {
	if root == 0 {
		return nil
	}
	if depth >= maxDepth {
		return fmt.Errorf("diffstore: B+ depth exceeded")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	n, err := inspectNode(r, root)
	if err != nil {
		return err
	}
	for _, e := range n.entries {
		if n.leaf {
			if err = visit(e); err != nil {
				return err
			}
		} else {
			if err = walkTree(ctx, r, e.slot, visit, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}
func (t *transaction) writable(slot uint64, n node) (uint64, error) {
	private, err := t.private(slot)
	if err != nil {
		return 0, err
	}
	if private {
		return slot, nil
	}
	fresh, err := t.allocate(kindIndex)
	if err != nil {
		return 0, err
	}
	if err = t.retire(slot); err != nil {
		return 0, err
	}
	t.nodes[fresh] = n
	return fresh, nil
}
func (t *transaction) put(root, key, value uint64, depth int) ([]entry, error) {
	if depth >= maxDepth {
		return nil, fmt.Errorf("diffstore: B+ depth exceeded")
	}
	if root == 0 {
		slot, err := t.allocate(kindIndex)
		if err != nil {
			return nil, err
		}
		t.nodes[slot] = node{leaf: true, entries: []entry{{key, value}}}
		return []entry{{key, slot}}, nil
	}
	n, err := readNode(t, root)
	if err != nil {
		return nil, err
	}
	slot, err := t.writable(root, n)
	if err != nil {
		return nil, err
	}
	i := sort.Search(len(n.entries), func(i int) bool { return n.entries[i].key >= key })
	if n.leaf {
		if i < len(n.entries) && n.entries[i].key == key {
			n.entries[i].slot = value
		} else {
			n.entries = append(n.entries, entry{})
			copy(n.entries[i+1:], n.entries[i:])
			n.entries[i] = entry{key, value}
		}
	} else {
		if i == len(n.entries) {
			i--
		}
		children, err := t.put(n.entries[i].slot, key, value, depth+1)
		if err != nil {
			return nil, err
		}
		replacement := make([]entry, 0, len(n.entries)+1)
		replacement = append(replacement, n.entries[:i]...)
		replacement = append(replacement, children...)
		replacement = append(replacement, n.entries[i+1:]...)
		n.entries = replacement
	}
	if len(n.entries) <= maxEntries {
		t.nodes[slot] = n
		return []entry{{n.entries[len(n.entries)-1].key, slot}}, nil
	}
	mid := len(n.entries) / 2
	right, err := t.allocate(kindIndex)
	if err != nil {
		return nil, err
	}
	t.nodes[slot] = node{n.leaf, n.entries[:mid]}
	t.nodes[right] = node{n.leaf, n.entries[mid:]}
	return []entry{{n.entries[mid-1].key, slot}, {n.entries[len(n.entries)-1].key, right}}, nil
}
func (t *transaction) set(key, value uint64) error {
	children, err := t.put(t.ctrl.Root, key, value, 0)
	if err != nil {
		return err
	}
	if len(children) == 1 {
		t.ctrl.Root = children[0].slot
		return nil
	}
	root, err := t.allocate(kindIndex)
	if err != nil {
		return err
	}
	t.nodes[root] = node{entries: children}
	t.ctrl.Root = root
	return nil
}
func (t *transaction) remove(root, key uint64, depth int) ([]entry, error) {
	if root == 0 {
		return nil, nil
	}
	if depth >= maxDepth {
		return nil, fmt.Errorf("diffstore: B+ depth exceeded")
	}
	n, err := readNode(t, root)
	if err != nil {
		return nil, err
	}
	i := sort.Search(len(n.entries), func(i int) bool { return n.entries[i].key >= key })
	if i == len(n.entries) || n.leaf && n.entries[i].key != key {
		return []entry{{n.entries[len(n.entries)-1].key, root}}, nil
	}
	slot, err := t.writable(root, n)
	if err != nil {
		return nil, err
	}
	if n.leaf {
		n.entries = append(n.entries[:i], n.entries[i+1:]...)
	} else {
		children, err := t.remove(n.entries[i].slot, key, depth+1)
		if err != nil {
			return nil, err
		}
		repl := append([]entry(nil), n.entries[:i]...)
		repl = append(repl, children...)
		n.entries = append(repl, n.entries[i+1:]...)
		// Merge only siblings that fit. Moving child pointers does not retire those
		// child objects: only the disappearing sibling page itself is retired.
		for j := max(0, i-1); j+1 < len(n.entries) && j <= i; j++ {
			left, err := readNode(t, n.entries[j].slot)
			if err != nil {
				return nil, err
			}
			right, err := readNode(t, n.entries[j+1].slot)
			if err != nil {
				return nil, err
			}
			if left.leaf != right.leaf {
				return nil, fmt.Errorf("diffstore: inconsistent B+ levels")
			}
			if len(left.entries)+len(right.entries) > maxEntries {
				continue
			}
			lslot, err := t.writable(n.entries[j].slot, left)
			if err != nil {
				return nil, err
			}
			if err = t.retire(n.entries[j+1].slot); err != nil {
				return nil, err
			}
			left.entries = append(left.entries, right.entries...)
			t.nodes[lslot] = left
			n.entries[j] = entry{left.entries[len(left.entries)-1].key, lslot}
			n.entries = append(n.entries[:j+1], n.entries[j+2:]...)
			break
		}
	}
	if len(n.entries) == 0 {
		delete(t.pages, slotPage(slot))
		delete(t.nodes, slot)
		if err = t.retire(slot); err != nil {
			return nil, err
		}
		return nil, nil
	}
	t.nodes[slot] = n
	return []entry{{n.entries[len(n.entries)-1].key, slot}}, nil
}
func (t *transaction) unset(key uint64) error {
	children, err := t.remove(t.ctrl.Root, key, 0)
	if err != nil {
		return err
	}
	if len(children) == 0 {
		t.ctrl.Root = 0
		return nil
	}
	t.ctrl.Root = children[0].slot
	n, err := readNode(t, t.ctrl.Root)
	if err != nil {
		return err
	}
	if !n.leaf && len(n.entries) == 1 {
		old := t.ctrl.Root
		t.ctrl.Root = n.entries[0].slot
		return t.retire(old)
	}
	return nil
}

func (s *Store) WritePages(ctx context.Context, pages []Page) error {
	if len(pages) == 0 {
		return ctx.Err()
	}
	for _, p := range pages {
		if len(p.Data) != PageSize || p.Block >= uint64(s.Size()/PageSize) {
			return fmt.Errorf("diffstore: invalid full page write")
		}
	}
	if err := s.MarkModified(ctx); err != nil {
		return err
	}
	for start := 0; start < len(pages); start += MaxBatchPages {
		part := pages[start:min(start+MaxBatchPages, len(pages))]
		s.mu.Lock()
		if err := s.healthy(ctx); err != nil {
			s.mu.Unlock()
			return err
		}
		if s.metadataOnly {
			s.mu.Unlock()
			return fmt.Errorf("diffstore: metadata-only handle cannot write payload")
		}
		// A direct Store caller may race Checkpoint between the optimistic
		// MarkModified and this write lock. Fence again before any data I/O.
		if s.ctrl.Current != 0 {
			if err := s.mutate(ctx, func(t *transaction) error { t.ctrl.Current = 0; return nil }); err != nil {
				s.mu.Unlock()
				return err
			}
		}
		err := s.mutate(ctx, func(t *transaction) error {
			// Last write to a repeated block wins within this bounded batch.
			unique := make(map[uint64][]byte, len(part))
			order := make([]uint64, 0, len(part))
			for _, p := range part {
				if _, ok := unique[p.Block]; !ok {
					order = append(order, p.Block)
				}
				unique[p.Block] = p.Data
			}
			// Reserve the batch's data slots before allocating any B+ pages. This
			// keeps unrelated logical pages physically adjacent whenever possible.
			var mappings []entry
			for _, block := range order {
				slot, err := lookup(t, t.ctrl.Root, block)
				if err != nil {
					return err
				}
				if slot != 0 {
					private, err := t.private(slot)
					if err != nil {
						return err
					}
					if private {
						t.data = append(t.data, dataWrite{slot, block, unique[block]})
						continue
					}
				}
				newSlot, err := t.allocate(kindData)
				if err != nil {
					return err
				}
				t.data = append(t.data, dataWrite{newSlot, block, unique[block]})
				if slot != 0 {
					if err = t.retire(slot); err != nil {
						return err
					}
				}
				mappings = append(mappings, entry{block, newSlot})
			}
			for _, mapping := range mappings {
				if err := t.set(mapping.key, mapping.slot); err != nil {
					return err
				}
			}
			return nil
		})
		s.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthy(context.Background()); err != nil {
		return 0, err
	}
	if s.metadataOnly {
		return 0, fmt.Errorf("diffstore: metadata-only handle cannot read payload")
	}
	if off < 0 || off > s.Size() {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off == s.Size() {
		return 0, io.EOF
	}
	count := min(len(p), int(s.Size()-off))
	done := 0
	for done < count {
		pos := off + int64(done)
		block := uint64(pos / PageSize)
		inner := int(pos % PageSize)
		n := min(count-done, PageSize-inner)
		slot, err := lookup(storeReader{s}, s.ctrl.Root, block)
		if err != nil {
			return done, s.fail(err)
		}
		if slot == 0 {
			clear(p[done : done+n])
			done += n
			continue
		}
		// Keep the active mapping and slot lifetimes protected through the
		// read and copy. Slots that differ by one need not be physical neighbors:
		// each allocator group has an intervening descriptor page.
		pages := 1
		var lookupErr error
		for pages < 256 && pages*PageSize-inner < count-done {
			var next uint64
			next, lookupErr = lookup(storeReader{s}, s.ctrl.Root, block+uint64(pages))
			if lookupErr != nil || next == 0 || slotPage(next) != slotPage(slot)+uint64(pages) {
				break
			}
			pages++
		}
		n, err = s.readData(p[done:count], slotPage(slot), block, pages, inner)
		done += n
		if err != nil {
			return done, s.fail(err)
		}
		// A lookahead failure must not discard the readable prefix collected
		// before it. No mapping beyond that prefix is consumed.
		if lookupErr != nil {
			return done, s.fail(lookupErr)
		}
	}
	if done < len(p) {
		return done, io.EOF
	}
	return done, nil
}
func (s *Store) Scan(ctx context.Context, visit func(uint64) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthy(ctx); err != nil {
		return err
	}
	if s.ctrl.Root == 0 {
		return nil
	}
	return s.scanActive(ctx, s.ctrl.Root, 0, uint64(s.Size()/PageSize)-1, 0, visit)
}
func (s *Store) Discard(ctx context.Context, off, length int64) error {
	if off < 0 || length < 0 || off%PageSize != 0 || length%PageSize != 0 || off > s.Size() || length > s.Size()-off {
		return fmt.Errorf("diffstore: invalid discard range")
	}
	if length == 0 {
		return ctx.Err()
	}
	if err := s.MarkModified(ctx); err != nil {
		return err
	}
	for pos := off; pos < off+length; {
		end := min(off+length, pos+MaxBatchPages*PageSize)
		s.mu.Lock()
		if err := s.healthy(ctx); err != nil {
			s.mu.Unlock()
			return err
		}
		if s.metadataOnly {
			s.mu.Unlock()
			return fmt.Errorf("diffstore: metadata-only handle cannot discard payload")
		}
		err := s.mutate(ctx, func(t *transaction) error {
			// A concurrent direct caller may checkpoint after the optimistic
			// MarkModified call. Publish invalidation with this mapping change.
			t.ctrl.Current = 0
			// mutate may retry after reservation pressure and foreground GC. Its
			// closure must not advance the caller's range before a commit succeeds.
			for current := pos; current < end; current += PageSize {
				block := uint64(current / PageSize)
				slot, err := lookup(t, t.ctrl.Root, block)
				if err != nil {
					return err
				}
				if slot != 0 {
					if err = t.retire(slot); err != nil {
						return err
					}
					if err = t.unset(block); err != nil {
						return err
					}
				}
			}
			return nil
		})
		s.mu.Unlock()
		if err != nil {
			return err
		}
		pos = end
	}
	return nil
}
