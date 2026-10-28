package uffd

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/memory"

	"golang.org/x/sys/unix"
)

// Exercise the production reader, dispatch and workers with an event pipe.
// The injected ioctl models mmap_changing until the reader publishes REMOVE;
// this is a deterministic queue regression, not a kernel-pressure simulation.
func TestWPRetryDrainsQueueBeforeRelease(t *testing.T) {
	for _, mode := range []string{"remove", "remove-missing", "unmap", "cancel", "close"} {
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
			successSeen := make(map[uint64]bool) // protected by installMu around the injected ioctl
			copied := make(chan struct{}, 1)
			h.ops.copyWP = func(int, uint64, []byte) (int64, error) {
				select {
				case copied <- struct{}{}:
				default:
				}
				return 0, unix.EEXIST
			}
			h.ops.wake = func(int, uint64, uint64) error { return nil }
			h.ops.writeProtect = func(_ int, va, _ uint64, _ bool) error {
				attempts.Add(1)
				if !h.cfg.Dirty.Contains(va - unitCHVA) {
					t.Error("unprotect before dirty")
				}
				if !h.released.Contains(0) {
					return unix.EAGAIN
				}
				if !successSeen[va] {
					successSeen[va] = true
					successes.Add(1)
				}
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
					if pageIdxHash(page)%MinWorkers != pageIdxHash(0)%MinWorkers {
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
				if mode == "remove-missing" {
					if err := send(uffdEventPagefault, 0, unitCHVA); err != nil {
						written <- err
						return
					}
					select {
					case <-copied:
					case <-h.ctx.Done():
						written <- h.ctx.Err()
						return
					}
				}
				if mode == "remove" || mode == "remove-missing" || mode == "unmap" {
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
			case "remove", "remove-missing":
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

// A first write to a protected resident page must not wait for another worker
// to be scheduled. Only mmap_changing/EAGAIN needs the asynchronous retry path.
func TestWPDispatchAttemptsOnceBeforeQueue(t *testing.T) {
	for _, result := range []struct {
		name   string
		err    error
		queued int
	}{{"complete", nil, 0}, {"retry", unix.EAGAIN, 1}, {"fatal", unix.EIO, 0}} {
		t.Run(result.name, func(t *testing.T) {
			h := newUnitHandler(t, 2*PageSize, ZeroSource{})
			defer h.cancel()
			var err error
			h.cfg.Dirty, err = memory.NewDirty(2 * PageSize)
			if err != nil {
				t.Fatal(err)
			}
			h.writeProtect = map[int]bool{unitFD: true}
			h.queue = []chan faultEvent{make(chan faultEvent, 1)}
			h.state.Set(0, StateLoaded)
			calls := 0
			h.ops.writeProtect = func(fd int, va, length uint64, protect bool) error {
				calls++
				if fd != unitFD || va != unitCHVA || length != PageSize || protect || !h.cfg.Dirty.Contains(0) {
					t.Fatal("unprotect without exact ownership and prior dirty publication")
				}
				return result.err
			}
			var msg uffdMsg
			msg.Event = uffdEventPagefault
			binary.LittleEndian.PutUint64(msg.Arg[0:8], uffdPagefaultFlagWP)
			binary.LittleEndian.PutUint64(msg.Arg[8:16], unitCHVA)
			h.dispatch(&msg, unitFD)
			if calls != 1 || len(h.queue[0]) != result.queued {
				t.Fatalf("attempts=%d queued=%d", calls, len(h.queue[0]))
			}
			if h.state.Get(0) != StateLoaded || h.cfg.Dirty.Contains(PageSize) {
				t.Fatal("WP changed content state or marked a clean neighbor")
			}
			if result.err == unix.EIO && h.ctx.Err() == nil {
				t.Fatal("permanent WP failure not fatal")
			}
		})
	}
}

// The reader acknowledges the whole kernel batch before handling faults. A WP
// appearing before REMOVE/UNMAP in that batch must observe the release first.
func TestWPDispatchObservesEntireReleaseBatch(t *testing.T) {
	for _, event := range []byte{uffdEventRemove, uffdEventUnmap} {
		t.Run(map[byte]string{uffdEventRemove: "remove", uffdEventUnmap: "unmap"}[event], func(t *testing.T) {
			var pipe [2]int
			if err := unix.Pipe2(pipe[:], unix.O_NONBLOCK|unix.O_CLOEXEC); err != nil {
				t.Fatal(err)
			}
			defer unix.Close(pipe[1])
			memfd, err := unix.MemfdCreate("wp-batch", unix.MFD_CLOEXEC)
			if err != nil {
				t.Fatal(err)
			}
			defer unix.Close(memfd)
			if err := unix.Ftruncate(memfd, 2*PageSize); err != nil {
				t.Fatal(err)
			}
			m := NewAddressMap(2 * PageSize)
			if err := m.RegisterVMA(ProcessCH, unitCHVA, 2*PageSize, 0); err != nil {
				t.Fatal(err)
			}
			h, err := NewWithBackendUffd(pipe[0], m, Config{MemfdFD: memfd, Size: 2 * PageSize, Source: ZeroSource{}, WriteProtect: true})
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			calls := 0
			h.ops.writeProtect = func(int, uint64, uint64, bool) error {
				calls++
				if !h.released.Contains(0) || !h.cfg.Dirty.Contains(0) {
					t.Error("unprotect preceded release or dirty publication")
				}
				return nil
			}
			// Deliberately place WP first in one complete read batch.
			var raw [64]byte
			raw[0] = uffdEventPagefault
			binary.LittleEndian.PutUint64(raw[8:], uffdPagefaultFlagWP)
			binary.LittleEndian.PutUint64(raw[16:], unitCHVA)
			raw[32] = event
			binary.LittleEndian.PutUint64(raw[40:], unitCHVA)
			binary.LittleEndian.PutUint64(raw[48:], unitCHVA+PageSize)
			if n, err := unix.Write(pipe[1], raw[:]); err != nil || n != len(raw) {
				t.Fatalf("write=%d %v", n, err)
			}
			done := make(chan struct{})
			go func() { h.drain(pipe[0], make([]byte, len(raw)), 32); close(done) }()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("reader held release gate during WP")
			}
			if !h.released.Contains(0) {
				t.Fatal("release missing")
			}
			if event == uffdEventRemove && (calls != 1 || h.ctx.Err() != nil) {
				t.Fatalf("remove: attempts=%d err=%v", calls, h.ctx.Err())
			}
			if event == uffdEventUnmap && (calls != 0 || h.ctx.Err() == nil) {
				t.Fatalf("unmap: attempts=%d err=%v", calls, h.ctx.Err())
			}
		})
	}
}
