package vhost

import (
	"context"
	"errors"
	"fmt"

	"github.com/kuasar-sandbox/sandboxer/internal/diffstore"
)

// resolvePresenceLocked resolves an inclusive, bounded logical range. Callers
// hold its frontend stripes for reading or writing, so discard and accepted
// writes cannot race publication. The writeback worker can change physical
// mappings, but its logical presence has already been published in the cache.
func (c *BlockCOW) resolvePresenceLocked(ctx context.Context, first, last int64) error {
	if c.known == nil {
		return nil
	}
	if last < first || last-first+1 > diffstore.MaxPresencePages {
		return fmt.Errorf("vhost: invalid bounded presence range")
	}
	c.bitmapMu.RLock()
	for first <= last && bitmapBlockDirty(c.known, first) {
		first++
	}
	for last >= first && bitmapBlockDirty(c.known, last) {
		last--
	}
	c.bitmapMu.RUnlock()
	if first > last {
		return nil
	}
	var present [diffstore.MaxPresencePages]bool
	if err := c.diff.backend.presence(ctx, uint64(first), present[:last-first+1]); err != nil {
		return c.presenceError(ctx, err)
	}
	c.bitmapMu.Lock()
	defer c.bitmapMu.Unlock()
	for block := first; block <= last; block++ {
		// A known block may contain an accepted write still awaiting writeback.
		// Its cache state takes precedence over the store's older mapping.
		if bitmapBlockDirty(c.known, block) {
			continue
		}
		mask := uint64(1) << (uint64(block) % 64)
		if present[block-first] {
			c.bitmap[block/64] |= mask
		}
		c.known[block/64] |= mask
	}
	return nil
}

func (c *BlockCOW) resolveAllPresence(ctx context.Context) error {
	if c.known == nil {
		return nil
	}
	_, known := c.presenceCounts()
	blocks := c.size / c.blockSize
	if int64(known) == blocks {
		return nil
	}
	// Ordinary active export already requires a quiescent frontend. Enumerate
	// mapped keys once instead of querying every logical range of a sparse disk.
	// Keep partial/canceled results private; no unknown block becomes a hole
	// unless the complete traversal succeeds. Named checkpoints never call this.
	bitmap := make([]uint64, len(c.bitmap))
	if err := c.diff.backend.scan(ctx, func(block uint64) error {
		bitmap[block/64] |= uint64(1) << (block % 64)
		return nil
	}); err != nil {
		return c.presenceError(ctx, err)
	}
	c.bitmapMu.Lock()
	defer c.bitmapMu.Unlock()
	for i := range bitmap {
		// Accepted cache writes/discards remain authoritative even if they have
		// not reached the index, or raced the traversal's publication boundary.
		c.bitmap[i] = c.bitmap[i]&c.known[i] | bitmap[i]&^c.known[i]
		c.known[i] = ^uint64(0)
	}
	if tail := uint64(blocks) % 64; tail != 0 {
		c.known[len(c.known)-1] = (uint64(1) << tail) - 1
	}
	return nil
}

func (c *BlockCOW) presenceError(ctx context.Context, err error) error {
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return err
	}
	// Never turn an index failure into a lower-layer read. Report it through
	// the same sandbox-wide sticky failure path as a failed cache writeback.
	c.cache.mu.Lock()
	report := c.cache.failLocked(fmt.Errorf("vhost: resolve checkpoint presence: %w", err))
	fatal := c.cache.err
	c.cache.mu.Unlock()
	if report != nil {
		report(fatal)
	}
	return fatal
}
