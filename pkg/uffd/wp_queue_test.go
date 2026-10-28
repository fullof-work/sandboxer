package uffd

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Exercise the production reader, dispatch and workers with an event pipe.
// The injected ioctl models mmap_changing until the reader publishes REMOVE;
// this is a deterministic queue regression, not a kernel-pressure simulation.
func TestWPRetryDrainsQueueBeforeRelease(t *testing.T) {
	for _, mode := range []string{"remove", "unmap", "cancel", "close"} {
		t.Run(mode, func(t *testing.T) {
			var fds [2]int
			if err := unix.Pipe2(fds[:], unix.O_NONBLOCK|unix.O_CLOEXEC); err != nil {
				t.Fatal(err)
			}
			defer unix.Close(fds[1])
			fd, err := unix.MemfdCreate("wp-queue", unix.MFD_CLOEXEC)
			if err != nil {
				t.Fatal(err)
			}
			defer unix.Close(fd)
			const size = 1024 * PageSize
			if err = unix.Ftruncate(fd, size); err != nil {
				t.Fatal(err)
			}
			m := NewAddressMap(size)
			if err = m.RegisterVMA(ProcessCH, unitCHVA, size, 0); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h, err := NewWithBackendUffd(fds[0], m, Config{Context: ctx, MemfdFD: fd, Size: size, Source: ZeroSource{}, NumWorkers: MinWorkers, WriteProtect: true})
			if err != nil {
				t.Fatal(err)
			}
			var attempts, successes atomic.Int64
			h.ops.writeProtect = func(_ int, va, _ uint64, _ bool) error {
				attempts.Add(1)
				if !h.cfg.Dirty.Contains(va - unitCHVA) {
					t.Error("unprotect before dirty")
				}
				if !h.released.Contains(0) {
					return unix.EAGAIN
				}
				successes.Add(1)
				return nil
			}
			h.Start()
			closed := false
			defer func() {
				if !closed {
					h.Close()
				}
			}()
			written := make(chan error, 1)
			go func() {
				send := func(event byte, first, second uint64) error {
					var raw [32]byte
					raw[0] = event
					binary.LittleEndian.PutUint64(raw[8:], first)
					binary.LittleEndian.PutUint64(raw[16:], second)
					for {
						_, err := unix.Write(fds[1], raw[:])
						if err == nil {
							return nil
						}
						if err != unix.EAGAIN {
							return err
						}
						select {
						case <-h.ctx.Done():
							return h.ctx.Err()
						case <-time.After(time.Millisecond):
						}
					}
				}
				count := 0
				for page := uint64(0); count < 300; page++ {
					if pageIdxHash(page)%MinWorkers != 0 {
						continue
					}
					if err := send(uffdEventPagefault, uffdPagefaultFlagWP, unitCHVA+page*PageSize); err != nil {
						written <- err
						return
					}
					count++
				}
				// Duplicate requests must not create an unbounded retry backlog.
				for i := 0; i < 300; i++ {
					if err := send(uffdEventPagefault, uffdPagefaultFlagWP, unitCHVA); err != nil {
						written <- err
						return
					}
				}
				if mode == "remove" || mode == "unmap" {
					event := byte(uffdEventRemove)
					if mode == "unmap" {
						event = uffdEventUnmap
					}
					written <- send(event, unitCHVA, unitCHVA+size)
				} else {
					written <- nil
				}
			}()
			select {
			case err := <-written:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("reader blocked behind WP queue")
			}
			deadline := time.After(3 * time.Second)
			switch mode {
			case "remove":
				for successes.Load() < 300 {
					select {
					case <-deadline:
						t.Fatalf("REMOVE/retry stalled: queue=%d attempts=%d successes=%d", h.QueueDepth(), attempts.Load(), successes.Load())
					case <-time.After(time.Millisecond):
					}
				}
				if !h.released.Contains(0) {
					t.Fatal("REMOVE not published")
				}
			case "unmap":
				select {
				case <-h.ctx.Done():
				case <-deadline:
					t.Fatal("UNMAP did not invalidate retry")
				}
				h.installMu.Lock()
				failure := h.captureError
				h.installMu.Unlock()
				if failure == nil {
					t.Fatal("UNMAP did not invalidate capture")
				}
			case "cancel":
				cancel()
			}
			done := make(chan struct{})
			go func() { h.Close(); close(done) }()
			select {
			case <-done:
				closed = true
			case <-deadline:
				t.Fatal("Close blocked on WP retry")
			}
			before := attempts.Load()
			time.Sleep(3 * time.Millisecond)
			if attempts.Load() != before {
				t.Fatal("ioctl after Close")
			}
		})
	}
}
