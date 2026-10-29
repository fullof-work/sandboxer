package uffd

import (
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// CaptureMemoryLayout must be called after CH (including balloon/reporting
// workers) and all backend writers are paused. The scan runs under installMu,
// jointly sampling physical holes and their existing page states. Source I/O is
// never performed here. Later speculative fills cannot change the logical view.
//
// Data always wins. Only an Absent hole inherits the original source. A Released
// hole is zero; a Loaded hole is also zero, just as handleFault treats a MISSING
// Loaded page after a zero fill followed by CH's final punch. No state is reset.
func (h *Handler) CaptureMemoryLayout(scan func() ([]sparse.Extent, error)) (holes, zeros []sparse.Extent, err error) {
	h.installMu.Lock()
	defer h.installMu.Unlock()
	if h.closing.Load() {
		return nil, nil, fmt.Errorf("memory capture: handler is closing")
	}
	physical, err := scan()
	if err != nil {
		return nil, nil, err
	}
	h.state.mu.RLock()
	defer h.state.mu.RUnlock()
	appendRun := func(dst *[]sparse.Extent, start, end uint64) {
		if n := len(*dst); n > 0 && (*dst)[n-1].Offset+(*dst)[n-1].Size == start {
			(*dst)[n-1].Size += end - start
		} else {
			*dst = append(*dst, sparse.Extent{Offset: start, Size: end - start})
		}
	}
	var previous uint64
	for _, run := range physical {
		if run.Size == 0 || run.Offset < previous || run.Offset > uint64(h.cfg.Size) || run.Size > uint64(h.cfg.Size)-run.Offset {
			return nil, nil, fmt.Errorf("memory capture: invalid physical hole")
		}
		end := run.Offset + run.Size
		for off := run.Offset; off < end; {
			page := off / PageSize
			if page >= uint64(len(h.state.pages)) {
				return nil, nil, fmt.Errorf("memory capture: state map is too short")
			}
			next := min(end, (page+1)*PageSize)
			switch PageState(h.state.pages[page]) {
			case StateAbsent:
				appendRun(&holes, off, next)
			case StateReleased, StateLoaded:
				appendRun(&zeros, off, next)
			default:
				return nil, nil, fmt.Errorf("memory capture: invalid page state")
			}
			off = next
		}
		previous = end
	}
	return holes, zeros, nil
}
