package diffstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"golang.org/x/crypto/xts"
	"golang.org/x/sys/unix"
)

// View is a sequential sparse export with independent file descriptions and a
// fixed-version pin. It creates no activity bitmap, plaintext cache or writer.
// Empty-ID views require the caller to keep the active DIFF quiescent.
type View struct {
	mu         sync.Mutex
	file, lock *os.File
	info       Info
	cipher     *xts.Cipher
	root       uint64
	cache      *pageCache
	overlay    map[uint64][]byte
	closed     bool
}

func (v *View) Info() Info   { return v.info }
func (v *View) Size() uint64 { return uint64(v.info.LogicalSize) }
func (v *View) read(page uint64) ([]byte, error) {
	if b, ok := v.overlay[page]; ok {
		return b, nil
	}
	if b, ok := v.cache.get(page); ok {
		return b, nil
	}
	b, err := readFull(v.file, page)
	if err != nil {
		return nil, err
	}
	v.cache.put(page, b)
	return b, nil
}
func (v *View) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil
	}
	v.closed = true
	err := errors.Join(v.file.Close(), v.lock.Close())
	v.cache = nil
	v.overlay = nil
	v.cipher = nil
	return err
}

func OpenExport(ctx context.Context, path, id string, key *[32]byte) (*View, error) {
	f, err := openData(path, unix.O_RDONLY)
	if err != nil {
		return nil, err
	}
	b, err := readFull(f, 0)
	if err != nil {
		f.Close()
		return nil, err
	}
	info, xc, err := parseHeader(b, key, false)
	if err != nil {
		f.Close()
		return nil, err
	}
	return openView(ctx, f, path+".lock", id, info, xc)
}
func (s *Store) Export(ctx context.Context, id string) (*View, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthy(ctx); err != nil {
		return nil, err
	}
	if s.metadataOnly && s.info.Encrypted {
		return nil, ErrEncryptionRequired
	}
	// Opening procfs gives an independent open file description for the same
	// inode, including after atomic publication renamed the temporary data file.
	path := fmt.Sprintf("/proc/self/fd/%d", s.file.Fd())
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECT, 0)
	if err != nil {
		return nil, err
	}
	return openView(ctx, os.NewFile(uintptr(fd), path), s.lockPath, id, s.info, s.cipher)
}
func openView(ctx context.Context, f *os.File, lockPath, id string, info Info, xc *xts.Cipher) (*View, error) {
	lock, err := openLock(lockPath)
	if err != nil {
		f.Close()
		return nil, err
	}
	fail := func(err error) (*View, error) { f.Close(); lock.Close(); return nil, err }
	if err = ctx.Err(); err != nil {
		return fail(err)
	}
	if err = ofd(lock, unix.F_RDLCK, metadataByte, true); err != nil {
		return fail(err)
	}
	v := &View{file: f, lock: lock, info: info, cipher: xc, cache: newCache(cachePages), overlay: make(map[uint64][]byte)}
	records, err := readRedo(f, nil)
	if err != nil {
		return fail(err)
	}
	for _, r := range records {
		v.overlay[r.page] = r.data
	}
	b, err := v.read(1)
	if err != nil {
		return fail(err)
	}
	var ctrl control
	if err = decodeJSON(b, "KDCONT2", &ctrl); err != nil {
		return fail(err)
	}
	if ctrl.Retired {
		return fail(ErrRetired)
	}
	v.root = ctrl.Root
	if id != "" {
		if err = ValidID(id); err != nil {
			return fail(err)
		}
		found := false
		for slot, count := ctrl.Head, uint64(0); slot != 0; count++ {
			if count >= ctrl.Next || slot >= ctrl.Next {
				return fail(fmt.Errorf("diffstore: checkpoint chain cycle"))
			}
			b, err = v.read(slotPage(slot))
			if err != nil {
				return fail(err)
			}
			var cp checkpoint
			if err = decodeJSON(b, "KDCP2", &cp); err != nil {
				return fail(err)
			}
			if cp.ID == id {
				if !cp.Visible || !cp.Retained {
					return fail(ErrNotExist)
				}
				if err = ofd(lock, unix.F_RDLCK, checkpointLockBase+int64(slot), false); err != nil {
					return fail(err)
				}
				v.root = cp.Root
				found = true
				break
			}
			slot = cp.Next
		}
		if !found {
			return fail(ErrNotExist)
		}
	}
	if err = ofd(lock, unix.F_RDLCK, readerByte, false); err != nil {
		return fail(err)
	}
	if err = ofd(lock, unix.F_UNLCK, metadataByte, false); err != nil {
		return fail(err)
	}
	return v, nil
}

func probeCurrent(path string, f *os.File, info Info) (Info, error) {
	lock, err := openLock(path + ".lock")
	if err != nil {
		return Info{}, err
	}
	defer lock.Close()
	if err = ofd(lock, unix.F_RDLCK, metadataByte, true); err != nil {
		return Info{}, err
	}
	records, err := readRedo(f, nil)
	if err != nil {
		return Info{}, err
	}
	overlay := make(map[uint64][]byte, len(records))
	for _, r := range records {
		overlay[r.page] = r.data
	}
	read := func(page uint64) ([]byte, error) {
		if b, ok := overlay[page]; ok {
			return b, nil
		}
		return readFull(f, page)
	}
	b, err := read(1)
	if err != nil {
		return Info{}, err
	}
	var ctrl control
	if err = decodeJSON(b, "KDCONT2", &ctrl); err != nil {
		return Info{}, err
	}
	if ctrl.Retired {
		return Info{}, ErrRetired
	}
	if ctrl.Current != 0 {
		b, err = read(slotPage(ctrl.Current))
		if err != nil {
			return Info{}, err
		}
		var cp checkpoint
		if err = decodeJSON(b, "KDCP2", &cp); err != nil {
			return Info{}, err
		}
		if cp.Visible {
			info.CurrentCheckpoint = cp.ID
		}
	}
	return info, nil
}

type viewRun struct {
	v        *View
	off, end uint64
	kind     sparse.RunKind
}

func (r viewRun) Offset() uint64       { return r.off }
func (r viewRun) End() uint64          { return r.end }
func (r viewRun) Kind() sparse.RunKind { return r.kind }
func (r viewRun) ReadAt(ctx context.Context, b []byte, off uint64) (int, error) {
	if off > r.end-r.off || uint64(len(b)) > r.end-r.off-off {
		return 0, fmt.Errorf("diffstore: read outside export run")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if r.kind == sparse.Hole {
		clear(b)
		return len(b), nil
	}
	return r.v.ReadAt(ctx, b, r.off+off)
}
func (v *View) RunAt(off, limit uint64) (sparse.Run, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil, os.ErrClosed
	}
	if off >= v.Size() {
		return nil, io.EOF
	}
	if limit == 0 {
		return nil, fmt.Errorf("diffstore: zero run limit")
	}
	end := v.Size()
	if limit < end-off {
		end = off + limit
	}
	block := off / PageSize
	e, ok, err := successor(v, v.root, block, 0)
	if err != nil {
		return nil, err
	}
	if !ok {
		return viewRun{v, off, end, sparse.Hole}, nil
	}
	if e.key > block {
		return viewRun{v, off, min(end, e.key*PageSize), sparse.Hole}, nil
	}
	// Coalesce a bounded run of mapped logical pages without touching payload.
	// Export can then issue full physical extents, while sparse metadata stays
	// independent from any particular consumer's read order.
	end = min(end, off+1<<20)
	cursor := (block + 1) * PageSize
	for cursor < end {
		slot, err := lookup(v, v.root, cursor/PageSize)
		if err != nil {
			return nil, err
		}
		if slot == 0 {
			end = cursor
			break
		}
		cursor += PageSize
	}
	return viewRun{v, off, end, sparse.Data}, nil
}
func (v *View) ReadAt(ctx context.Context, p []byte, off uint64) (int, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return 0, os.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if off > v.Size() {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off == v.Size() {
		return 0, io.EOF
	}
	count := len(p)
	if uint64(count) > v.Size()-off {
		count = int(v.Size() - off)
	}
	done := 0
	for done < count {
		if err := ctx.Err(); err != nil {
			return done, err
		}
		pos := off + uint64(done)
		block := pos / PageSize
		inner := int(pos % PageSize)
		slot, err := lookup(v, v.root, block)
		if err != nil {
			return done, err
		}
		if slot == 0 {
			n := min(count-done, PageSize-inner)
			clear(p[done : done+n])
			done += n
			continue
		}
		blocks := 1
		for blocks < 256 && blocks*PageSize-inner < count-done {
			next, err := lookup(v, v.root, block+uint64(blocks))
			if err != nil {
				return done, err
			}
			if next == 0 || slotPage(next) != slotPage(slot)+uint64(blocks) {
				break
			}
			blocks++
		}
		b, err := directRead(v.file, blocks*PageSize, int64(slotPage(slot))*PageSize)
		if err != nil {
			return done, err
		}
		if v.cipher != nil {
			for page := 0; page < blocks; page++ {
				for unit := 0; unit < PageSize; unit += 512 {
					part := b[page*PageSize+unit : page*PageSize+unit+512]
					v.cipher.Decrypt(part, part, (block+uint64(page))*(PageSize/512)+uint64(unit/512))
				}
			}
		}
		n := min(count-done, len(b)-inner)
		copy(p[done:done+n], b[inner:inner+n])
		clear(b)
		done += n
	}
	if done < len(p) {
		return done, io.EOF
	}
	return done, nil
}
