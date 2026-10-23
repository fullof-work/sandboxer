package versioned

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/crypto/xts"
)

const PageSize = 4096
const MaxBatchPages = 64

var (
	ErrFormat             = errors.New("diffstore: not a v2 DIFF")
	ErrBusy               = errors.New("diffstore: owner or reader busy")
	ErrExists             = errors.New("diffstore: checkpoint identity already used")
	ErrNotExist           = errors.New("diffstore: checkpoint does not exist")
	ErrRetired            = errors.New("diffstore: DIFF retired")
	ErrEncryptionRequired = errors.New("diffstore: encryption key required")
	ErrAuthentication     = errors.New("diffstore: authentication failed")
	ErrPlaintextForbidden = errors.New("diffstore: plaintext DIFF forbidden")
)

// Options binds a DIFF to its immutable lower and logical capacity. Empty
// identity/capacity fields on Open mean inspect, rather than check, that field.
// LockPath is the stable final filename plus ".lock" when Create uses a
// temporary data filename. It must not change when the data file is published.
type Options struct {
	DiffID            string
	BaseID            string
	LogicalSize       int64
	Key               *[32]byte
	RequireEncryption bool
	LockPath          string
}

type Info struct {
	DiffID            string `json:"diff_id"`
	BaseID            string `json:"base_id"`
	LogicalSize       int64  `json:"logical_size"`
	Encrypted         bool   `json:"encrypted"`
	CurrentCheckpoint string `json:"current_checkpoint,omitempty"`
}

type Page struct {
	Block uint64
	Data  []byte
}

// Stats counts completed physical I/O, excluding the immutable header probe.
// Cache counters include descriptor and B+ index lookups. Checkpoint creation
// must leave DataRead/DataWritten unchanged; private overwrite must leave
// IndexWritten/RedoWritten unchanged after MarkModified's one-time fence.
type Stats struct {
	DataRead, DataWritten                   uint64
	IndexRead, IndexWritten                 uint64
	RedoRead, RedoWritten                   uint64
	GCRead, GCWritten, GCPunched            uint64
	GCPunchCalls, GCFreed                   uint64
	CacheHits, CacheMisses                  uint64
	Allocated, Reused, Commits, GCProcessed uint64
}

type Store struct {
	mu            sync.Mutex
	file          *os.File
	lock          *os.File
	lockPath      string
	info          Info
	cipher        *xts.Cipher
	ctrl          control
	cache         *pageCache
	stats         Stats
	fatal         error
	closed        bool
	closedFlag    atomic.Bool
	retiredFlag   atomic.Bool
	fatalFlag     atomic.Pointer[storedError]
	currentFlag   atomic.Uint64
	gc            gcScan
	gcDeletion    uint64
	gcVersionTurn bool
	metadataOnly  bool
	stop          chan struct{}
	done          chan struct{}
	// beforeIO is deliberately package-private fault injection, installed before
	// use by deterministic tests. A process-exit hook also exercises real reopen.
	beforeIO func(stage string) error
	// preadData is a per-store test seam for payload reads, protected by mu.
	// A nil hook uses unix.Pread on the real O_DIRECT file description.
	preadData func(fd int, b []byte, off int64) (int, error)
}
type storedError struct{ err error }

func NewDiffID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func ValidID(id string) error {
	if len(id) == 0 || len(id) > 128 || !utf8.ValidString(id) {
		return fmt.Errorf("diffstore: checkpoint ID must be valid UTF-8 of 1–128 bytes")
	}
	for _, r := range id {
		if unicode.IsControl(r) {
			return fmt.Errorf("diffstore: checkpoint ID contains a control character")
		}
	}
	return nil
}

func (s *Store) Size() int64 { return s.info.LogicalSize }
func (s *Store) Info() Info {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.info
	out.CurrentCheckpoint = ""
	if s.closed || s.fatal != nil || s.ctrl.Retired {
		return out
	}
	if s.ctrl.Current != 0 {
		if cp, err := s.readCheckpoint(s.ctrl.Current, nil); err == nil {
			out.CurrentCheckpoint = cp.ID
		} else {
			s.fail(err)
		}
	}
	return out
}
func (s *Store) Stats() Stats { s.mu.Lock(); defer s.mu.Unlock(); return s.stats }
func (s *Store) Err() error {
	if s.closedFlag.Load() {
		return os.ErrClosed
	}
	if e := s.fatalFlag.Load(); e != nil {
		return e.err
	}
	if s.retiredFlag.Load() {
		return ErrRetired
	}
	return nil
}
func (s *Store) CanResume(id string) bool { return id != "" && s.Info().CurrentCheckpoint == id }

func (s *Store) healthy(ctx context.Context) error {
	if s.closed {
		return os.ErrClosed
	}
	if s.fatal != nil {
		return s.fatal
	}
	if s.ctrl.Retired {
		return ErrRetired
	}
	return ctx.Err()
}
func (s *Store) fail(err error) error {
	if err != nil && s.fatal == nil {
		s.fatal = err
		s.fatalFlag.Store(&storedError{err})
	}
	return err
}
func (s *Store) hook(stage string) error {
	if s.beforeIO != nil {
		return s.beforeIO(stage)
	}
	return nil
}

// MarkModified must precede any mutation of an external plaintext cache. It
// persists the invalidation once per checkpoint cycle, even if the mapping root
// will not change because the forthcoming write overwrites a private data slot.
func (s *Store) MarkModified(ctx context.Context) error {
	if s.currentFlag.Load() == 0 {
		if err := s.Err(); err != nil {
			return err
		}
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthy(ctx); err != nil {
		return err
	}
	if s.ctrl.Current == 0 {
		return nil
	}
	return s.mutate(ctx, func(t *transaction) error { t.ctrl.Current = 0; return nil })
}

func (s *Store) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	if s.fatal != nil {
		return s.fatal
	}
	return s.fail(s.file.Sync())
}

func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.closedFlag.Store(true)
	if s.stop != nil {
		close(s.stop)
	}
	done := s.done
	s.mu.Unlock()
	if done != nil {
		<-done
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	err := errors.Join(s.fatal, s.file.Close(), s.lock.Close())
	s.cache = nil
	s.cipher = nil
	return err
}

func (s *Store) startGC() {
	s.stop = make(chan struct{})
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		delay := 250 * time.Millisecond
		timer := time.NewTimer(delay)
		defer timer.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-timer.C:
			}
			for {
				select {
				case <-s.stop:
					return
				default:
				}
				result, err := s.GC(context.Background(), MaxBatchPages)
				if err != nil {
					return
				}
				if result.State != More {
					delay = 250 * time.Millisecond
					if result.State == BlockedByReaders {
						delay = time.Second
					}
					break
				}
				// GC released mu. Leave a short scheduling window for foreground
				// work instead of immediately competing for the store lock again.
				// Explicit GC/allocation assistance never incurs this worker delay.
				// Zero freed slots still continues, and shutdown interrupts the wait.
				timer.Reset(time.Millisecond)
				select {
				case <-s.stop:
					return
				case <-timer.C:
				}
			}
			timer.Reset(delay)
		}
	}()
}

// WriteAt accepts complete pages, as supplied by the Disk cache or template
// materializer. The caller owns partial-page completion and guest partial-write
// semantics. Each bounded batch may commit independently.
func (s *Store) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 || off%PageSize != 0 || len(p)%PageSize != 0 {
		return 0, fmt.Errorf("diffstore: unaligned write")
	}
	if off > s.Size() || int64(len(p)) > s.Size()-off {
		return 0, io.ErrShortWrite
	}
	n := 0
	for n < len(p) {
		count := min(MaxBatchPages, (len(p)-n)/PageSize)
		pages := make([]Page, count)
		for i := range pages {
			pages[i] = Page{Block: uint64((off + int64(n+i*PageSize)) / PageSize), Data: p[n+i*PageSize : n+(i+1)*PageSize]}
		}
		if err := s.WritePages(context.Background(), pages); err != nil {
			return n, err
		}
		n += count * PageSize
	}
	return n, nil
}
