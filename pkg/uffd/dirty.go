package uffd

import (
	"errors"
	"fmt"
	"runtime"

	"golang.org/x/sys/unix"
)

// seedRegion runs before the CH va_report ACK. Pages that already exist can
// have been installed outside our COPY protocol; include them in D_seed.
func (h *Handler) seedRegion(fd int) error {
	if h.cfg.Dirty == nil {
		return nil
	}
	h.addrMap.mu.RLock()
	defer h.addrMap.mu.RUnlock()
	for _, v := range h.addrMap.vmas {
		if v.process != ProcessCH || v.uffdFD != fd || v.invalid {
			continue
		}
		end := v.memfdOffset + (v.end - v.start)
		for off := v.memfdOffset; off < end; {
			data, err := unix.Seek(h.cfg.MemfdFD, int64(off), unix.SEEK_DATA)
			if errors.Is(err, unix.ENXIO) {
				break
			}
			if err != nil {
				return err
			}
			if uint64(data) >= end {
				break
			}
			hole, err := unix.Seek(h.cfg.MemfdFD, data, unix.SEEK_HOLE)
			if err != nil {
				return err
			}
			if hole <= data {
				return fmt.Errorf("uffd: invalid seed residency")
			}
			if err := h.cfg.Dirty.Mark(uint64(data), min(uint64(hole), end)-uint64(data)); err != nil {
				return err
			}
			off = uint64(hole)
		}
	}
	if runtime.GOARCH != "arm64" && !h.writeProtect[fd] {
		h.logf("dirty memory fallback: region fd=%d has no atomic shmem COPY_MODE_WP; materialized pages are conservatively dirty", fd)
	}
	return nil
}

// All callers hold installMu. An unknown existing folio is conservative dirty;
// it cannot be assumed protected just because COPY reports EEXIST.
func (h *Handler) markInstall(fd int, dst, length uint64) error {
	if h.cfg.Dirty == nil {
		return nil
	}
	off, ok := h.addrMap.Locate(dst)
	if !ok || !h.addrMap.owns(fd, dst, off, length) {
		return fmt.Errorf("uffd: dirty installation registration missing")
	}
	return h.cfg.Dirty.Mark(off, length)
}
func (h *Handler) copyCleanLocked(fd int, dst uint64, data []byte) (int64, error) {
	copyOp := h.ops.copy
	if h.writeProtect[fd] {
		copyOp = h.ops.copyWP
		if copyOp == nil {
			return 0, fmt.Errorf("uffd: protected COPY unavailable")
		}
	} else if runtime.GOARCH != "arm64" {
		if err := h.markInstall(fd, dst, uint64(len(data))); err != nil {
			return 0, err
		}
	}
	n, err := copyOp(fd, dst, data)
	if errors.Is(err, unix.EEXIST) {
		if markErr := h.markInstall(fd, dst, uint64(len(data))); markErr != nil {
			return n, markErr
		}
	}
	return n, err
}
func (h *Handler) zeroCleanLocked(fd int, dst, length uint64) (int64, error, bool) {
	if h.writeProtect[fd] {
		n, err := h.copyCleanLocked(fd, dst, zeroScratch[:length])
		h.stats.copies.Add(1)
		return n, err, false
	}
	if runtime.GOARCH != "arm64" {
		if err := h.markInstall(fd, dst, length); err != nil {
			return 0, err, true
		}
	}
	n, err := h.ops.zeropage(fd, dst, length)
	if n == 0 && (errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP)) {
		n, err = h.copyCleanLocked(fd, dst, zeroScratch[:length])
		h.stats.copies.Add(1)
		return n, err, false
	}
	if errors.Is(err, unix.EEXIST) {
		if markErr := h.markInstall(fd, dst, length); markErr != nil {
			return n, markErr, true
		}
	}
	return n, err, true
}

// handleWP makes one attempt. EAGAIN must return to the worker: retaining a
// worker here can fill its queue and stop the reader before a pending REMOVE.
func (h *Handler) handleWP(fd int, va, off uint64) error {
	h.installMu.Lock()
	defer h.installMu.Unlock()
	if err := h.ctx.Err(); err != nil {
		return err
	}
	if !h.writeProtect[fd] || !h.addrMap.owns(fd, va, off, PageSize) {
		return fmt.Errorf("uffd: WP fault outside protected registration")
	}
	if h.cfg.Dirty == nil || h.ops.writeProtect == nil {
		return fmt.Errorf("uffd: WP tracker unavailable")
	}
	if err := h.cfg.Dirty.Mark(off, PageSize); err != nil {
		return err
	}
	// D precedes every unprotect attempt, including transient/permanent failure.
	if err := h.ops.writeProtect(fd, va, PageSize, false); err != nil {
		return fmt.Errorf("uffd: unprotect: %w", err)
	}
	return nil
}

// A queued MISSING event can be stale after another installer populated the
// page. A genuinely absent Loaded page needs a published release; otherwise
// zero filling would hide lost data. The check shares the installation gate.
func (h *Handler) checkLoadedPage(offset uint64) error {
	if h.cfg.Dirty == nil {
		return nil
	} // isolated legacy handler test fixtures
	h.installMu.Lock()
	defer h.installMu.Unlock()
	data, err := unix.Seek(h.cfg.MemfdFD, int64(offset), unix.SEEK_DATA)
	if err != nil && !errors.Is(err, unix.ENXIO) {
		return err
	}
	if (errors.Is(err, unix.ENXIO) || uint64(data) > offset) && !h.released.Contains(offset) {
		h.captureError = fmt.Errorf("uffd: Loaded page at 0x%x became Hole without release", offset)
		return h.captureError
	}
	return nil
}

func (h *Handler) invalidateCapture(err error) {
	h.installMu.Lock()
	if h.captureError == nil {
		h.captureError = err
	}
	h.installMu.Unlock()
}
