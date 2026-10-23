package versioned

import (
	"context"
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func (s *Store) findCheckpoint(id string, t *transaction) (uint64, checkpoint, error) {
	head := s.ctrl.Head
	next := s.ctrl.Next
	if t != nil {
		head = t.ctrl.Head
		next = t.ctrl.Next
	}
	for slot, count := head, uint64(0); slot != 0; count++ {
		if count >= next || slot >= next {
			return 0, checkpoint{}, fmt.Errorf("diffstore: checkpoint chain cycle")
		}
		cp, err := s.readCheckpoint(slot, t)
		if err != nil {
			return 0, checkpoint{}, err
		}
		if cp.ID == id {
			return slot, cp, nil
		}
		slot = cp.Next
	}
	return 0, checkpoint{}, ErrNotExist
}
func (s *Store) Checkpoint(ctx context.Context, id string) error {
	if err := ValidID(id); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthy(ctx); err != nil {
		return err
	}
	if _, _, err := s.findCheckpoint(id, nil); err == nil {
		return ErrExists
	} else if !errors.Is(err, ErrNotExist) {
		return s.fail(err)
	}
	return s.mutate(ctx, func(t *transaction) error {
		slot, err := t.allocate(kindCheckpoint)
		if err != nil {
			return err
		}
		cp := checkpoint{ID: id, Epoch: t.ctrl.Epoch, Root: t.ctrl.Root, Previous: t.ctrl.Tail, Visible: true, Retained: true}
		if t.ctrl.Tail != 0 {
			last, err := s.readCheckpoint(t.ctrl.Tail, t)
			if err != nil {
				return err
			}
			last.Next = slot
			if err = t.putCheckpoint(t.ctrl.Tail, last); err != nil {
				return err
			}
		} else {
			t.ctrl.Head = slot
		}
		if err = t.putCheckpoint(slot, cp); err != nil {
			return err
		}
		t.ctrl.Tail = slot
		t.ctrl.Current = slot
		t.ctrl.Protected = slot
		t.ctrl.Epoch++
		if t.ctrl.Epoch == 0 {
			return fmt.Errorf("diffstore: generation exhausted")
		}
		return nil
	})
}
func (s *Store) ListCheckpoints(ctx context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthy(ctx); err != nil {
		return nil, err
	}
	out := []string{}
	for slot, count := s.ctrl.Head, uint64(0); slot != 0; count++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if count >= s.ctrl.Next || slot >= s.ctrl.Next {
			return nil, s.fail(fmt.Errorf("diffstore: checkpoint chain cycle"))
		}
		cp, err := s.readCheckpoint(slot, nil)
		if err != nil {
			return nil, s.fail(err)
		}
		if cp.Visible {
			out = append(out, cp.ID)
		}
		slot = cp.Next
	}
	return out, nil
}
func (s *Store) DeleteCheckpoint(ctx context.Context, id string) error {
	return s.DeleteCheckpoints(ctx, []string{id})
}
func (s *Store) DeleteCheckpoints(ctx context.Context, ids []string) error {
	for _, id := range ids {
		if err := ValidID(id); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthy(ctx); err != nil {
		return err
	}
	// Hold the in-process gate across withdrawal of the complete chosen set.
	// Background GC cannot repeatedly transfer a chain through members of it.
	for start := 0; start < len(ids); start += MaxBatchPages {
		part := ids[start:min(start+MaxBatchPages, len(ids))]
		err := s.mutate(ctx, func(t *transaction) error {
			for _, id := range part {
				slot, cp, err := s.findCheckpoint(id, t)
				if err != nil {
					return err
				}
				if !cp.Visible {
					continue
				}
				cp.Visible = false
				cp.Pending = true
				t.ctrl.Pending++
				// Withdraw all unpinned members before any GC, including when the
				// persisted cursor starts in the middle of this batch. Pinned deleted
				// versions remain retained and continue to force CoW.
				pinErr := ofd(s.lock, unix.F_WRLCK, checkpointLockBase+int64(slot), false)
				if pinErr == nil {
					_ = ofd(s.lock, unix.F_UNLCK, checkpointLockBase+int64(slot), false)
					cp.Retained = false
				} else if !errors.Is(pinErr, ErrBusy) {
					return pinErr
				}
				if t.ctrl.Current == slot {
					t.ctrl.Current = 0
				}
				if err = t.putCheckpoint(slot, cp); err != nil {
					return err
				}
				if t.ctrl.Protected == slot && !cp.Retained {
					prev, _, err := t.predecessor(cp.Previous)
					if err != nil {
						return err
					}
					t.ctrl.Protected = prev
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}
func (t *transaction) predecessor(slot uint64) (uint64, checkpoint, error) {
	for count := uint64(0); slot != 0; count++ {
		if count >= t.ctrl.Next {
			return 0, checkpoint{}, fmt.Errorf("diffstore: checkpoint predecessor cycle")
		}
		cp, err := t.store.readCheckpoint(slot, t)
		if err != nil {
			return 0, checkpoint{}, err
		}
		if cp.Retained {
			return slot, cp, nil
		}
		slot = cp.Previous
	}
	return 0, checkpoint{}, nil
}

// GC advances at most budget reclamation steps and separately bounds version
// visits and predecessor discovery by the same budget. Its cursors and chain
// changes commit together, so an interrupted owner resumes without a tree walk.
// A slot is punched before it becomes reachable from the free list.
func (s *Store) GC(ctx context.Context, budget int) (int, error) {
	if budget <= 0 {
		return 0, nil
	}
	budget = min(budget, MaxBatchPages)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gcLocked(ctx, budget)
}
func (s *Store) gcLocked(ctx context.Context, budget int) (int, error) {
	if s.closed {
		return 0, os.ErrClosed
	}
	if s.fatal != nil {
		return 0, s.fatal
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	before := s.stats
	defer func() {
		// These are completed physical bytes attributable to GC, not logical
		// record visits or cache hits. They overlap the index/redo totals.
		s.stats.GCRead += s.stats.IndexRead - before.IndexRead
		s.stats.GCWritten += s.stats.IndexWritten - before.IndexWritten + s.stats.RedoWritten - before.RedoWritten
	}()
	processed := 0
	err := s.mutate(ctx, func(t *transaction) error {
		// Garbage has no retained-version owner, so it is immediately recyclable.
		for t.ctrl.Garbage != 0 && processed < budget {
			slot := t.ctrl.Garbage
			d, err := t.desc(slot)
			if err != nil {
				return err
			}
			if d.Kind != kindGarbage {
				return fmt.Errorf("diffstore: invalid garbage chain")
			}
			if err = s.hook("gc-punch"); err != nil {
				return err
			}
			if err = punch(s.file, slot); err != nil {
				return err
			}
			s.stats.GCPunched += PageSize
			t.ctrl.Garbage = d.Next
			d.Next = t.ctrl.Free
			d.Kind = kindFree
			if err = t.setDesc(slot, d); err != nil {
				return err
			}
			t.ctrl.Free = slot
			s.cache.remove(slotPage(slot))
			processed++
			s.stats.GCProcessed++
		}
		if t.ctrl.Pending == 0 || processed >= budget {
			return nil
		}
		cursor := t.ctrl.GCScan
		if cursor == 0 {
			cursor = t.ctrl.Head
		}
		visited := 0
		predecessorVisits := 0
		for cursor != 0 && processed < budget && visited < budget {
			cp, err := s.readCheckpoint(cursor, t)
			if err != nil {
				return err
			}
			visited++
			if cp.Pending {
				if cp.Retained {
					err = ofd(s.lock, unix.F_WRLCK, checkpointLockBase+int64(cursor), false)
					if err == nil {
						_ = ofd(s.lock, unix.F_UNLCK, checkpointLockBase+int64(cursor), false)
						cp.Retained = false
					} else if !errors.Is(err, ErrBusy) {
						return err
					}
				}
				if !cp.Retained {
					var prevSlot uint64
					var prev checkpoint
					if cp.Garbage != 0 || t.ctrl.Protected == cursor {
						prevSlot = cp.Previous
						found := false
						for prevSlot != 0 && predecessorVisits < budget {
							prev, err = s.readCheckpoint(prevSlot, t)
							if err != nil {
								return err
							}
							predecessorVisits++
							if prev.Epoch >= cp.Epoch {
								return fmt.Errorf("diffstore: invalid predecessor generation")
							}
							if prev.Retained {
								found = true
								break
							}
							// New checkpoints only append. A withdrawn predecessor can
							// never become retained again, so this persisted shortcut is
							// safe and makes a long tombstone span resumable.
							cp.Previous = prev.Previous
							prevSlot = cp.Previous
						}
						if prevSlot != 0 && !found {
							if err = t.putCheckpoint(cursor, cp); err != nil {
								return err
							}
							t.ctrl.GCScan = cursor
							return nil
						}
						if t.ctrl.Protected == cursor {
							// Until discovery finishes, retaining this conservative epoch
							// merely causes extra CoW; it cannot expose a reader to reuse.
							t.ctrl.Protected = prevSlot
						}
					}
					for cp.Garbage != 0 && processed < budget {
						slot := cp.Garbage
						d, err := t.desc(slot)
						if err != nil {
							return err
						}
						if d.Kind != kindGarbage {
							return fmt.Errorf("diffstore: invalid retired chain")
						}
						cp.Garbage = d.Next
						if prevSlot != 0 && prev.Epoch >= d.Birth {
							d.Next = prev.Garbage
							prev.Garbage = slot
						} else {
							d.Next = t.ctrl.Garbage
							t.ctrl.Garbage = slot
						}
						if err = t.setDesc(slot, d); err != nil {
							return err
						}
						processed++
						s.stats.GCProcessed++
					}
					if prevSlot != 0 {
						if err = t.putCheckpoint(prevSlot, prev); err != nil {
							return err
						}
					}
					if cp.Garbage == 0 {
						cp.Pending = false
						t.ctrl.Pending--
					}
				}
				if err = t.putCheckpoint(cursor, cp); err != nil {
					return err
				}
			}
			if cp.Pending && !cp.Retained && cp.Garbage != 0 {
				break
			}
			cursor = cp.Next
		}
		t.ctrl.GCScan = cursor
		return nil
	})
	return processed, err
}

// Retire marks the whole unmounted DIFF unavailable before trying to unlink it.
// Existing fixed views retain their inode and return ErrBusy here until closed.
// OpenMetadata can reopen a retired DIFF to retry this exact operation.
func (s *Store) Retire(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	if s.fatal != nil {
		return s.fatal
	}
	if !s.ctrl.Retired {
		if err := s.mutate(ctx, func(t *transaction) error { t.ctrl.Retired = true; t.ctrl.Current = 0; return nil }); err != nil {
			return err
		}
	}
	if err := ofd(s.lock, unix.F_WRLCK, readerByte, false); err != nil {
		return err
	}
	defer ofd(s.lock, unix.F_UNLCK, readerByte, false)
	// A template owner can retain its original descriptor across publication by
	// rename. Resolve that inode's current name, then check it before unlinking.
	path, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", s.file.Fd()))
	if err != nil {
		return err
	}
	current, err := os.Lstat(path)
	if err != nil {
		return err
	}
	owned, err := s.file.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(current, owned) {
		return fmt.Errorf("diffstore: retired path no longer identifies the owned inode")
	}
	return os.Remove(path)
}
