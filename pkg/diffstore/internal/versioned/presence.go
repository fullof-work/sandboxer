package versioned

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

// validatePayloadRange preserves the B+ boundary checks when logical presence
// comes from P rather than an index walk. Callers hold the Store lock. Failure
// is reported before payload reads/writes or metadata publication.
func (s *Store) validatePayloadRange(ctx context.Context, start, end uint64) error {
	if s.ctrl.Root == 0 {
		return nil
	}
	var scratch [MaxPresencePages]bool
	for start < end {
		next := min(end, start+MaxPresencePages)
		if err := payloadRange(ctx, storeReader{s}, s.ctrl.Root, start, next, scratch[:next-start], 0, uint64(s.Size()/PageSize)-1, 0); err != nil {
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				return err
			}
			return s.fail(err)
		}
		start = next
	}
	return nil
}

// PayloadPresence resolves active-tree mappings for [start, start+len(present)) without
// reading payload. A mapped all-zero page is present. Each call observes one
// current root under the owner lock; callers must synchronize their own pending
// writes and discards before publishing these answers in an external cache.
// The result is usable only on success. An index or I/O failure is sticky, while
// invalid ranges and context cancellation do not poison the store.
func (s *Store) PayloadPresence(ctx context.Context, start uint64, present []bool) error {
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
	err := payloadRange(ctx, storeReader{s}, s.ctrl.Root, start, start+uint64(len(present)), present, 0, total-1, 0)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return s.fail(err)
}

// activeNode checks the metadata page and the immediate bounds needed to trust
// its absence answers. Descendants are validated only when their range is used.
func (s *Store) activeNode(slot uint64) (node, error) {
	return inspectNode(storeReader{s}, slot)
}

func payloadRange(ctx context.Context, r pageReader, slot, start, end uint64, present []bool, lower, upper uint64, depth int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n, err := payloadRangeNode(r, slot, lower, upper, depth)
	if err != nil {
		return err
	}
	i := sort.Search(len(n.entries), func(i int) bool { return n.entries[i].key >= start })
	if !n.leaf && i > 0 {
		// A checksummed separator can still understate its child's maximum.
		// Validate the skipped predecessor down to its rightmost leaf before
		// trusting an absence, including searches beyond the final separator.
		if err := presenceBoundary(ctx, r, n, i-1, lower, depth+1, true); err != nil {
			return err
		}
	}
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
		if err := payloadRange(ctx, r, e.slot, start, end, present, childLower, e.key, depth+1); err != nil {
			return err
		}
	}
	if !n.leaf && i < len(n.entries) {
		// Likewise a successor's leftmost leaf must not hide mappings below
		// its declared lower bound, inside the range just reported as holes.
		if err := presenceBoundary(ctx, r, n, i, lower, depth+1, false); err != nil {
			return err
		}
	}
	return nil
}

// presenceBoundary checks only an adjacent subtree's extreme path. Each visited
// internal node checks at most two such paths, each capped by maxDepth;
// this never expands into a whole-tree scan or reads payload pages.
func presenceBoundary(ctx context.Context, r pageReader, parent node, index int, lower uint64, depth int, right bool) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if index > 0 {
			lower = parent.entries[index-1].key + 1
		}
		e := parent.entries[index]
		n, err := payloadRangeNode(r, e.slot, lower, e.key, depth)
		if err != nil {
			return err
		}
		if n.leaf {
			return nil
		}
		parent, index = n, 0
		if right {
			index = len(n.entries) - 1
		}
		depth++
	}
}

func payloadRangeNode(r pageReader, slot, lower, upper uint64, depth int) (node, error) {
	if depth >= maxDepth {
		return node{}, fmt.Errorf("diffstore: B+ depth exceeded")
	}
	n, err := inspectNode(r, slot)
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
	n, err := payloadRangeNode(storeReader{s}, slot, lower, upper, depth)
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
