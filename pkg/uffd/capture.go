package uffd

import (
	"fmt"

	"github.com/kuasar-sandbox/sandboxer/pkg/memory"
)

// CaptureView fixes residency and release state under the same gate that
// acknowledges REMOVE and installs folios. Call only after CH pause, backend
// quiesce, CH snapshot and final CPU/software-log collection. No source I/O or
// worker join occurs under this lock; subsequent tails only fill B or zeros.
func (h *Handler) CaptureView() (*memory.CaptureView, error) {
	h.uffdsMu.Lock()
	defer h.uffdsMu.Unlock()
	h.installMu.Lock()
	defer h.installMu.Unlock()
	if h.captureError != nil {
		return nil, h.captureError
	}
	if h.closing.Load() || h.ctx.Err() != nil || !h.started {
		return nil, fmt.Errorf("uffd: capture on stopped handler")
	}
	for off := uint64(0); off < uint64(h.cfg.Size); {
		_, fd, n, err := h.addrMap.registration(off, uint64(h.cfg.Size)-off)
		if err != nil {
			return nil, err
		}
		if fd <= 0 || n == 0 {
			return nil, fmt.Errorf("uffd: capture registration gap at 0x%x", off)
		}
		off += n
	}
	holes, err := memory.WalkHoles(h.cfg.MemfdFD, int64(h.cfg.Size))
	if err != nil {
		return nil, err
	}
	resident, _ := memory.NewDirty(uint64(h.cfg.Size))
	off := uint64(0)
	for _, hole := range holes {
		if hole.Offset%PageSize != 0 || hole.Size%PageSize != 0 {
			return nil, fmt.Errorf("uffd: capture residency is not page aligned")
		}
		if err := resident.Mark(off, hole.Offset-off); err != nil {
			return nil, err
		}
		off = hole.Offset + hole.Size
	}
	if err := resident.Mark(off, uint64(h.cfg.Size)-off); err != nil {
		return nil, err
	}
	v := &memory.CaptureView{Size: uint64(h.cfg.Size), Materialized: resident.Snapshot(), Dirty: h.cfg.Dirty.Snapshot(), Released: h.released.Snapshot()}
	h.state.mu.RLock()
	defer h.state.mu.RUnlock()
	for i, state := range h.state.pages {
		if PageState(state) == StateLoaded && v.Materialized[i/8]&(1<<uint(i%8)) == 0 && v.Released[i/8]&(1<<uint(i%8)) == 0 {
			return nil, fmt.Errorf("uffd: Loaded page %d became Hole without release", i)
		}
	}
	return v, nil
}
