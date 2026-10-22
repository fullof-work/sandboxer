package diffstore

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// MaxPresencePages limits one presence query to 1 MiB of logical address space.
// Callers completing a larger map must release their own range locks between
// calls, so foreground I/O is not held behind a whole-device index walk.
const MaxPresencePages = 256

// Presence resolves active-tree mappings for [start, start+len(present)) without
// reading payload. A mapped all-zero page is present. Each call observes one
// current root under the owner lock; callers must synchronize their own pending
// writes and discards before publishing these answers in an external cache.
// The result is usable only on success. An index or I/O failure is sticky, while
// invalid ranges and context cancellation do not poison the store.
func (s *Store) Presence(ctx context.Context, start uint64, present []bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthy(ctx); err != nil {
		return err
	}
	total := uint64(s.Size() / PageSize)
	if len(present) > MaxPresencePages || start > total || uint64(len(present)) > total-start {
		return fmt.Errorf("diffstore: invalid presence range")
	}
	clear(present)
	if len(present) == 0 || s.ctrl.Root == 0 {
		return nil
	}
	err := s.presenceRange(ctx, s.ctrl.Root, start, start+uint64(len(present)), present, 0, total-1, 0)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return s.fail(err)
}

// activeNode checks the metadata page and the immediate bounds needed to trust
// its absence answers. Descendants are validated only when their range is used.
func (s *Store) activeNode(slot uint64) (node, error) {
	if slot == 0 || slot >= s.ctrl.Next {
		return node{}, fmt.Errorf("diffstore: B+ slot outside allocation")
	}
	n, err := inspectNode(storeReader{s}, slot)
	if err != nil {
		return node{}, err
	}
	for _, e := range n.entries {
		if e.key >= uint64(s.Size()/PageSize) {
			return node{}, fmt.Errorf("diffstore: mapping outside capacity")
		}
		if e.slot >= s.ctrl.Next {
			return node{}, fmt.Errorf("diffstore: B+ entry outside allocation")
		}
	}
	return n, nil
}

func (s *Store) presenceRange(ctx context.Context, slot, start, end uint64, present []bool, lower, upper uint64, depth int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n, err := s.activeRangeNode(slot, lower, upper, depth)
	if err != nil {
		return err
	}
	i := sort.Search(len(n.entries), func(i int) bool { return n.entries[i].key >= start })
	for ; i < len(n.entries); i++ {
		e := n.entries[i]
		if n.leaf {
			if e.key >= end {
				break
			}
			present[e.key-start] = true
			continue
		}
		childLower := lower
		if i > 0 {
			childLower = n.entries[i-1].key + 1
		}
		if childLower >= end {
			break
		}
		if err := s.presenceRange(ctx, e.slot, start, end, present, childLower, e.key, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) activeRangeNode(slot, lower, upper uint64, depth int) (node, error) {
	if depth >= maxDepth {
		return node{}, fmt.Errorf("diffstore: B+ depth exceeded")
	}
	n, err := s.activeNode(slot)
	if err != nil {
		return node{}, err
	}
	last := n.entries[len(n.entries)-1].key
	if n.entries[0].key < lower || last > upper || depth > 0 && last != upper {
		return node{}, fmt.Errorf("diffstore: invalid B+ child range")
	}
	return n, nil
}

// scanActive visits allocated index pages, independent of the logical capacity.
// Only metadata failures are sticky; caller cancellation and visitor errors may
// stop a scan without preventing subsequent I/O on an otherwise healthy owner.
func (s *Store) scanActive(ctx context.Context, slot, lower, upper uint64, depth int, visit func(uint64) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n, err := s.activeRangeNode(slot, lower, upper, depth)
	if err != nil {
		return s.fail(err)
	}
	for _, e := range n.entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if n.leaf {
			if err := visit(e.key); err != nil {
				return err
			}
		} else {
			if err := s.scanActive(ctx, e.slot, lower, e.key, depth+1, visit); err != nil {
				return err
			}
			lower = e.key + 1
		}
	}
	return nil
}
