package chapi

import (
	"context"
	"errors"
	"sync"
)

// DirtyMemoryCollector remembers capability for one owned CH process. A failed
// first collection leaves it unknown; only a complete 404 selects the legacy
// working set. Once native collection succeeds, no later error can downgrade it.
// It does not enable or reset logging: CH records from initialization onward.
type DirtyMemoryCollector struct {
	client Client
	size   uint64
	mu     sync.Mutex
	mode   uint8
}

const (
	dirtyUnknown uint8 = iota
	dirtyNative
	dirtyLegacy
)

func NewDirtyMemoryCollector(client Client, size uint64) *DirtyMemoryCollector {
	return &DirtyMemoryCollector{client: client, size: size}
}

func (c *DirtyMemoryCollector) Collect(ctx context.Context) (*DirtyMemory, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if c.mode == dirtyLegacy {
		return nil, false, nil
	}
	report, err := c.client.DirtyMemory(ctx, c.size)
	if err != nil {
		if c.mode == dirtyUnknown && errors.Is(err, ErrDirtyMemoryUnsupported) {
			c.mode = dirtyLegacy
			return nil, false, nil
		}
		return nil, false, err
	}
	c.mode = dirtyNative
	return report, true, nil
}
