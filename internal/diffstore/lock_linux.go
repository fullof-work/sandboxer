package diffstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

const ownerByte = 0
const metadataByte = 1
const readerByte = 2
const checkpointLockBase = 16

func openLock(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() {
		f.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("diffstore: lock is not regular")
	}
	return f, nil
}
func ofd(f *os.File, typ int16, offset int64, wait bool) error {
	cmd := unix.F_OFD_SETLK
	if wait {
		cmd = unix.F_OFD_SETLKW
	}
	for {
		err := unix.FcntlFlock(f.Fd(), cmd, &unix.Flock_t{Type: typ, Whence: 0, Start: offset, Len: 1})
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EACCES) {
			return ErrBusy
		}
		return err
	}
}

// DirectoryOwner is independent of a DIFF writer or fixed-version reader.
// The inode is stable and never unlinked during ownership handoff.
type DirectoryOwner struct {
	file      *os.File
	Directory string
	Identity  string
}

func LockDirectory(directory string) (*DirectoryOwner, error) {
	abs, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(filepath.Join(abs, "diffs"), 0700); err != nil {
		return nil, err
	}
	stDiffs, err := os.Lstat(filepath.Join(abs, "diffs"))
	if err != nil {
		return nil, err
	}
	if !stDiffs.IsDir() || stDiffs.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("diffstore: diffs must be a real directory")
	}
	abs, err = filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, err
	}
	f, err := openLock(filepath.Join(abs, "diffs", ".owner.lock"))
	if err != nil {
		return nil, err
	}
	if err = ofd(f, unix.F_WRLCK, ownerByte, false); err != nil {
		f.Close()
		return nil, err
	}
	var st unix.Stat_t
	if err = unix.Fstat(int(f.Fd()), &st); err != nil {
		f.Close()
		return nil, err
	}
	return &DirectoryOwner{file: f, Directory: abs, Identity: fmt.Sprintf("%x:%x", st.Dev, st.Ino)}, nil
}
func (o *DirectoryOwner) Close() error {
	if o == nil || o.file == nil {
		return nil
	}
	f := o.file
	o.file = nil
	return f.Close()
}
