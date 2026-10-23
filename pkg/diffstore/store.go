package diffstore

import (
	"context"

	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore/internal/versioned"
)

// Store is the versioned engine handle for offline import and administration.
// Live disks must use Disk's checkpoint boundary, never a second Store owner.
type Store = versioned.Store
type Options = versioned.Options
type Info = versioned.Info
type Page = versioned.Page
type Stats = versioned.Stats
type View = versioned.View
type DirectoryOwner = versioned.DirectoryOwner
type GCState = versioned.GCState
type GCResult = versioned.GCResult

const (
	Complete         = versioned.Complete
	More             = versioned.More
	BlockedByReaders = versioned.BlockedByReaders
)

const PageSize = versioned.PageSize
const MaxBatchPages = versioned.MaxBatchPages
const MaxPresencePages = versioned.MaxPresencePages

var (
	ErrBusy               = versioned.ErrBusy
	ErrExists             = versioned.ErrExists
	ErrFormat             = versioned.ErrFormat
	ErrNotExist           = versioned.ErrNotExist
	ErrRetired            = versioned.ErrRetired
	ErrPlaintextForbidden = versioned.ErrPlaintextForbidden
	ErrEncryptionRequired = versioned.ErrEncryptionRequired
	ErrAuthentication     = versioned.ErrAuthentication
)

func Create(path string, o Options) (*Store, error) { return versioned.Create(path, o) }
func Open(path string, o Options) (*Store, error)   { return versioned.Open(path, o) }
func OpenMetadata(path string) (*Store, error)      { return versioned.OpenMetadata(path) }
func Probe(path string) (Info, error)               { return versioned.Probe(path) }
func OpenExport(ctx context.Context, path, id string, key *[32]byte) (*View, error) {
	return versioned.OpenExport(ctx, path, id, key)
}
func NewDiffID() string                                  { return versioned.NewDiffID() }
func LockDirectory(path string) (*DirectoryOwner, error) { return versioned.LockDirectory(path) }
