package diffstore

import (
	"fmt"
	"io"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

// Four bounded shared mappings are sufficient for overlapping owner and
// publisher I/O. MAP_SHARED is essential: private Go-heap DIO buffers are not
// safe while another goroutine forks a guest/helper process. No borrowed buffer
// escapes a syscall; index caches hold ordinary copies. The pool lives for the
// process lifetime and never exceeds 32 MiB, irrespective of version count.
const directBufferSize = 8 << 20

var directPool struct {
	once    sync.Once
	buffers chan []byte
	err     error
}

func withDirectBuffer(n int, fn func([]byte) error) error {
	if n < 0 || n > directBufferSize || n%PageSize != 0 {
		return fmt.Errorf("diffstore: invalid direct I/O size %d", n)
	}
	directPool.once.Do(func() {
		directPool.buffers = make(chan []byte, 4)
		for i := 0; i < 4; i++ {
			b, err := unix.Mmap(-1, 0, directBufferSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_SHARED)
			if err != nil {
				directPool.err = err
				for len(directPool.buffers) != 0 {
					_ = unix.Munmap(<-directPool.buffers)
				}
				return
			}
			directPool.buffers <- b
		}
	})
	if directPool.err != nil {
		return directPool.err
	}
	b := <-directPool.buffers
	defer func() { clear(b[:n]); directPool.buffers <- b }()
	return fn(b[:n])
}
func directRead(f *os.File, n int, off int64) ([]byte, error) {
	if off < 0 || off%PageSize != 0 {
		return nil, fmt.Errorf("diffstore: unaligned direct read")
	}
	var result []byte
	err := withDirectBuffer(n, func(b []byte) error {
		m, err := unix.Pread(int(f.Fd()), b, off)
		if err != nil {
			return err
		}
		if m != len(b) {
			return io.ErrUnexpectedEOF
		}
		result = append([]byte(nil), b...)
		return nil
	})
	return result, err
}
func directWrite(f *os.File, p []byte, off int64) error {
	if off < 0 || off%PageSize != 0 {
		return fmt.Errorf("diffstore: unaligned direct write")
	}
	return withDirectBuffer(len(p), func(b []byte) error {
		copy(b, p)
		n, err := unix.Pwrite(int(f.Fd()), b, off)
		if err != nil {
			return err
		}
		if n != len(b) {
			return io.ErrShortWrite
		}
		return nil
	})
}
