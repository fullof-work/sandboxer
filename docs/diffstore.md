# Writable disk storage (`pkg/diffstore`)

`pkg/diffstore` owns writable block storage independently of vhost transport. Both
single-layer v3 files and checkpoint-capable v4 files use the same `Disk`
frontend. Both store logical presence separately from physical payload. Legacy
raw/v1/v2 working files are rejected without modification. Export
them with the old executable before upgrading; existing portable tarstreams remain
readable and can seed a new-format DIFF. There is no in-place migration.

## Layers and dependencies

| Layer | Owns | Must not own |
| --- | --- | --- |
| Sandbox / restore / artifact | Disk topology, immutable base composition, VM pause, multi-disk checkpoint coordination, publication | Physical page layout or cache state |
| `pkg/vhost` | Protocol, virtqueues, guest memory, request statistics; queue context forwarded to `Disk` | Writable files, COW cache or checkpoint transactions |
| `pkg/diffstore.Disk` | Admission/lifetime, logical presence, partial writes, discard, shared cache attachment, template materialization and export boundaries | VM or multi-disk atomicity |
| Private `diffBackend` | Synchronous logical-page I/O, presence, format-specific failure cleanup, sync and close | Frontend cache pages or ownership of borrowed page payloads |
| Single-layer `diffFile` | v3 header, presence pages, sparse payload offsets, owned direct-I/O workspaces | B+ tree or checkpoint references |
| `internal/versioned` via `checkpointDiff` | v4 presence, B+ tree, transactions, recovery, version references, reader pins, garbage collection | Frontend admission or cache draining |
| `internal/directio`, `internal/sectorcrypt` | Shared mmap alignment/lifetime and logical 512-byte XTS transforms | Pool policy, file headers, authentication or mapping policy |

The dependency direction is transport/application -> `pkg/diffstore` -> its
private implementations. Filesystem preflight helpers sit above the physical
backends; neither physical backend interprets ext4. Immutable fetch streams use
`StreamReader` as a source adapter. The v4 implementation is inaccessible to other
packages through Go's `internal` rule.

The public `Store`/`Options` aliases and `Create`, `Open`, `OpenMetadata`, `Probe`,
`OpenExport`, and `LockDirectory` functions expose the existing v4 engine handle
for **offline** import, inspection and administration. They are intentionally
separate from `OpenDisk`: they neither attach the sandbox cache nor provide a
live disk's admission/drain boundary. Applications must not open a second writable
`Store` for a live `Disk`, or use offline APIs to bypass that disk's checkpoint
boundary. This refactor retains the administrative API instead of introducing a
second implementation of version management. The v4 `ErrEncryptionRequired` and
`ErrAuthentication` sentinels are public for `errors.Is` checks through all v4
entry points; they are distinct from the single-layer `ErrDiff*` sentinels.

## Choosing a format

```go
// Single-layer v3 by default; WithDiffEncryption selects XTS payload encryption.
disk, err := diffstore.OpenDisk(path, base, init,
    diffstore.WithCache(cache))

// Explicit stable identities select checkpoint-capable v4.
disk, err := diffstore.OpenDisk(path, base, init,
    diffstore.WithCache(cache),
    diffstore.WithCheckpoint(diffID, baseID),
    diffstore.WithDiffEncryption(customerKey, true))
```

Check `err` before using a disk. `DiffInit{Existing: true}` opens an existing file;
`CreateSize` or `TemplatePath` provisions a fresh file. v4 opens authenticate and
check the expected identity/base and never adopt older files. A plaintext raw
provisioning template remains allowed when the new active file must be encrypted.

| Property | Single layer (v3) | Versioned (v4) |
| --- | --- | --- |
| Runtime | Same `Disk` and shared `Cache` | Same `Disk` and shared `Cache` |
| Existing-file presence | Full logical P load; sparse scan identifies physical payload only | Full logical P load; bounded B+ queries identify payload |
| Writeback | Bounded batches may contain disjoint pages; each changed P page is published once per batch | Bounded transactions may contain disjoint logical pages |
| Template staging | Up to 64 pages (256 KiB) | Up to `maxWritebackPages` (256) 4 KiB pages |
| Checkpoint capability | Unavailable | `CheckpointManager` |
| Physical failure cleanup | Hole-punch only newly created upper pages | Transaction/allocation engine owns recovery; never punch guest offsets in the mixed physical file |

The private batch consists of logical block number, borrowed plaintext bytes and
whether the upper page was fresh. It exposes no `cachePage`. The cache freezes and
charges pages until write and failure cleanup finish, and publishes sticky failure
before cleanup can block. The single-layer backend stages into its existing
workspace; v4 performs its existing mapping commit. Buffer pools, request batching
limits and encryption domains retain their existing policies.

Fresh creation shares the claim/recheck/template/temp-file flow, and publication
shares file sync, atomic rename, claim release and directory sync. Materialization
uses one dispatcher and the same stripe/presence conflict checks. v4 retains
batched template commits; single-layer batches at most 64 pages to amortize
presence publication and contiguous payload writes. Its temporary staging grows
from 4 KiB to 256 KiB only while materialization runs. Template bytes
are read before taking write stripes, then presence is rechecked so frontend
writes/discards win. A fully superseded range may consequently be read and then
skipped. `WaitMaterialReady` and `WaitMaterialized` retain distinct readiness and
publication boundaries.

Fresh checkpoint-backed live disks initialize the format asynchronously, with
or without a template. Preparation still validates identity, capacity (including
template-derived capacity), encryption and exclusive ownership. It establishes
an independent empty frontend P bitmap and the existing bounded cache before
`OpenDisk` returns. Offline `Create` runs the same initializer synchronously;
existing-file open still validates committed metadata and loads the real P bitmap.
No file format or configuration switch is added.

| Boundary | Completed work |
| --- | --- |
| `OpenDisk` returns | Validated ownership and logical view; absent upper pages read template/base/zero and cached writes may proceed. The final path may not exist yet. |
| Internal initialization completion | Truncation, fixed reservation and the existing header/control/redo/batched bitmap writes completed, or one terminal error was recorded. Backend I/O and GC wait for success. |
| `WaitMaterialReady` succeeds | Initialization completed and the view no longer depends on the template. This precedes cache drain so checkpoint can quiesce writers. |
| `WaitMaterialized` succeeds | Initialization, optional template import, required writeback, file sync, rename, claim release and directory sync completed. This does not synchronize subsequent writes. |

An empty `Drain` does not wait for initialization. Physical path handoff/reopen,
active export and snapshot publication use `WaitMaterialized`. Immutable device
identity, size, health checks and fresh checkpoint invalidation do not wait for
format I/O. Discard and template backfill wait before taking frontend stripes.
The one cache worker selects the oldest eligible dirty page across its bounded
queue, skipping pending backends before freezing pages; a pending prefix longer
than a batch cannot hide ready work. Pending pages remain readable and rewritable,
but still consume the shared quota. If all work is pending, the worker sleeps
until completion, new eligible work, failure or shutdown wakes it.

Initialization is admitted once before fallible frontend attachment and is joined
directly on construction failure. Request cancellation affects only the waiter;
Close joins actual initialization I/O before releasing the file, lock or buffers.
Successful initialization establishes GC ownership before publishing completion.
The existing bounded MAP_SHARED direct-I/O workspaces remain safe while Guest
process creation overlaps this work. There is no per-page initialization state.

Late initialization/publication I/O errors are sticky runtime failures and may
follow previously accepted cached writes. Completion is published before fatal
callbacks, including late handler replay. A failed returned disk retains template
readers and private resources until Close stops admission and joins existing
users. Disposable Close may stop unfinished template work and prevent publication
after initialization, but cannot cancel the initializer or hide a real error from
an already admitted publication. Retained Close drains and publishes normally.
Intentional disposal is a successful Close, but readiness/publication waiters
receive cancellation when their boundary was not completed.

This removes avoidable foreground waiting, not initialization work. Early backend
use, cache pressure, immediate checkpoint/Close and concurrent disk initialization
can still wait or contend for I/O. Compare current synchronous batched creation
with the candidate using the same bitmap path, capacity, encryption, templates
and cache budget; retain end-to-end startup tails and failures as well as logical
open, first useful I/O, Ready, publication and Close timings.

## Ownership and completion

- `Disk` owns its backend, materializer and its attachment to the cache. Its
  immutable `BlockReader` base is **borrowed**; keep it open until `Disk.Close`
  completes, then close it separately. `StreamReader.Close` closes the wrapped
  stream, not separately owned fetcher/cache clients.
- Without `WithCache`, `Disk` creates and closes a private cache. With `WithCache`,
  the caller owns the shared cache: close all attached disks before the cache.
  A sandbox's writable devices continue sharing one budget and one worker.
- `ReadAtContext`/`WriteAtContext` carry queue cancellation across the package
  boundary. `ReadAt`/`WriteAt` remain conventional context-free entry points.
- Guest `Flush` checks health; it does not promise media persistence. `Drain`
  waits for accepted cache writes to finish. A checkpoint additionally performs
  the versioned persistence boundary. Do not equate request acceptance or Drain
  with a durable checkpoint.
- `Disk.Close` excludes new operations and joins admitted work before releasing
  storage. `WithDiscardOnClose` permits cancellation of unfinished template work
  for disposable runtime-owned disks; it does not transfer ownership of a base.

## Checkpoints and exports

```go
manager, supported := disk.Checkpoints()
if supported {
    err = manager.Checkpoint(ctx, diffstore.CheckpointID("snapshot-1"))
}
```

The manager shares the disk's lifetime and routes operations through the live
admission/cache boundary. A checkpoint waits for template readiness, pauses new
frontend operations, drains accepted writes, waits for materialization publication,
and commits the named version. Multiple disks still require application-level
coordination. Existing direct `Disk` checkpoint methods remain available, but
`Checkpoints()` makes format capability explicit without probing via errors.

`OpenActiveView(ctx)` returns an **upper-only** sparse view that includes accepted
cache writes. The caller must keep the disk open and quiescent until the view is
closed; this is neither a persistent checkpoint nor a base-composed disk image.
`OpenCheckpoint(ctx, id)` requires a nonempty ID and returns an immutable reader
with an independent pin. It survives subsequent writes and owner close; close the
view to release its pin. `Export(ctx, ActiveDiff)` remains the lower-level spelling
of an active view, while an empty ID is rejected by `OpenCheckpoint`.

Live reopen loads the complete logical presence bitmap, without a full B+ scan.
Named exports load their own immutable bitmap and root. Payload lookup validates
visited index pages and adjacent subtree boundaries; corruption is an error,
never a Zero or lower-layer Hole. Unvisited subtrees are not audited at Open.

## Presence and physical layout

Writable logical capacity must be positive and divisible by 4096. Single-layer
v3 supports up to 1 TiB; checkpoint v4 remains limited to 32 GiB because its
active bitmap occupies a fixed reserved region before the record area. These
limits apply to active DIFFs and template-derived writable capacity, and exclude
read-only devices, RAM, artifact metadata tails and physical storage retained
by old versions. One presence bit per 4 KiB page uses at most 32 MiB of net
bitmap bytes for a 1 TiB disk, or 1 MiB for a 32 GiB disk. These are per-bitmap
sizes, not total backend, frontend or cache memory bounds.

The shared capacity check admits up to 1 TiB. The v4 backend separately checks
its 32 GiB limit on creation and open, before initializing a fresh checkpoint
file. Upper-level template or runtime preparation can precede that rejection.

| P | Effective payload | Logical result |
| --- | --- | --- |
| 0 | Ignored, even if stale physical bytes exist | Hole; read the lower layer |
| 1 | None | Zero; mask the lower layer without a payload page |
| 1 | Present, including accepted cache writes | Data |

Ordinary writes set P and store Data without scanning for zero bytes. A partial
write to Zero starts with zero-filled bytes, never lower data. Template and merge
composition stop at Zero. Full template Zero pages set P without writing Data;
partial Zero spans use ordinary page composition. The modification fence also
applies to P-only changes. Guest FLUSH and materialization durability are unchanged.

Single-layer v3 starts with a 4096-byte header describing logical capacity, body
offset, bitmap offset/length/page count and encryption. Checksummed bitmap pages
follow it; each has a 32-byte header, 4060 bitmap bytes and a 4-byte CRC. The body
starts after all bitmap pages. Header/P allocation is excluded from payload extent
enumeration. XTS uses logical 512-byte sectors and never decrypts a physical hole.
Payload is written before publishing changed P pages; discard punches before P
publication. Even P=0 to Zero removes residual payload first. A Zero's first Data
write is fresh, allowing failed-write cleanup to restore its committed P state.

Versioned v4 places the same bitmap pages at physical page 1600, after the bounded
redo region and before the record area at page 2048. They are reserved on creation
and loaded on Open. Changed P pages use the existing atomic metadata transaction;
recovery and read-only redo interpretation validate their exact permitted range.
The backend serializes bitmap updates and merges changed bits/pages; the frontend
never overwrites backend P with a stale full bitmap.

A Checkpoint atomically stores the root plus an independent complete P copy in
ordinary allocator records (at most 259 pages, 1 MiB net). It copies no Data. Its
cost includes bitmap copying, allocation and transaction metadata, including
fragmented free slots; the redo limit remains 1536 existing page images. Reader
pins protect both root-reachable data and bitmap records. Delete only withdraws
the version and marks work pending. Bounded GC sends exclusive P records directly
to the global reclaimable list, persists partial progress, and leaves the version
pending until that copy is exhausted. It needs no payload decryption key.

## Discard and reclamation

`Disk.Discard(offset, length)` changes only fully covered 4 KiB pages;
partial edge pages retain their contents. It removes active payload and sets P=1,
returning Zero without revealing base bytes. The public
`Store.Discard(ctx, offset, length)` has the same complete operation, requiring
page-aligned ranges. PayloadPresence is false and no active Data mapping remains;
retained Checkpoints may still protect old records. Guest DISCARD uses this same
operation. This does not add WRITE_ZEROES or zero compression.
Each batch covers at most 64 full pages. It takes distinct stripe locks in numeric
order, checks all existing cache pages under one cache lock, and only then freezes
them together. Loading or writeback makes the whole batch wait without freezing
any part. Uncached pages require no placeholder and unrelated dirty pages need no
drain. Backend I/O runs without the cache lock and receives a context. Cancellation
can stop a batch before freezing; afterward it must publish the whole batch or
enter the existing sticky failure path and wake waiters. Success publishes the
Zero for every page, including previously absent pages. The normal presence bit
alone prevents template Data or Zero backfill from overwriting foreground content;
there is no separate template-resolution bitmap. Unmodified template Holes still
fall through to the base. Separate batches do not provide an all-or-nothing range
transaction. Normal presence publication only sets bits. Failed first-payload
writes instead restore the old committed bitmap: Hole retains P=0 and Zero
retains P=1, after cleanup of newly allocated payload.

`DeleteCheckpoints` only withdraws visibility and immediately releases protection
when no reader pins the version. It never runs or waits for GC. `OpenMetadata`
and List also do not schedule GC. Active stores have an independent worker;
metadata-only owners explicitly schedule maintenance.

`Store.GC(ctx, budget)` returns `GCResult{State, FreedSlots}`. `Complete` means no
pending versions or globally reclaimable records remain. `More` means bounded
work remains and must be retried even if `FreedSlots` is zero. `BlockedByReaders`
is returned only when the global reclaimable chain is empty and a valid complete
round found all remaining pending versions pinned. A later reader close permits
progress without another deletion. `FreedSlots` counts only locations committed
to the reusable free list, never scans, transfers or punch attempts.

Version visits, predecessor discovery and ownership transfers have bounded work;
global release and version work take turns using full bounded batches, including
budget 1, avoiding two half-sized metadata commits when both are ready.
The active worker waits 1 ms outside the store lock between `More` batches,
interruptibly on shutdown, to reduce competition with foreground work. Explicit
GC and bounded foreground allocation assistance do not incur this delay; the
tradeoff is lower peak background reclamation throughput.
Pure cursors and predecessor search progress live in memory, not redo. A round
captures its tail; new Checkpoints cannot extend it indefinitely. Ordinary writes
do not reset it, and new deletion invalidates a stale blocked conclusion. Reopen
can repeat pure scanning while persisted ownership changes remain recoverable.
The active worker yields between `More` batches and checks reader-blocked work
less frequently. Allocation failure may perform a finite amount of foreground
assistance, retrying only after committed frees; GC never recursively allocates
through that assistance path.

Only the global reclaimable chain is punched. A batch validates unique candidates
and legal states, sorts by physical file page address, and merges physically
adjacent ranges. Descriptor pages between each 128 record positions and all live
pages split ranges. Punching finishes before one existing metadata/redo transaction
removes the old chain entries and publishes free descriptors. The batch links in
ascending address order, then joins the former free head. Failure before commit
publishes no free slots; reopening may safely repeat a punch. GC itself adds no
format version, larger redo area, payload relocation, file truncation or global
free-range index. Encryption and direct-I/O alignment retain their existing rules.

`DirectoryMaintenance` borrows an acquired `DirectoryOwner`, validates its stable
lock identity, and registers all active DiffIDs before enumeration. It never
acquires a second writer for an active disk or releases the owner. Historical
enumeration uses bounded batches; two GC handles and one transient management
handle bound open metadata writers. List/Delete share an existing GC handle, with
serialized requests between GC batches. Management of another historical file
can use the reserved handle even when both GC slots are occupied. Startup and
periodic sweeps report per-DiffID errors and continue other files. They skip
retired files without calling Retire or unlinking them. Close stops admission and
scheduling, joins admitted operations/current batches, and closes historical
handles. The caller then closes active disks and finally the directory owner.

Reclamation does not choose retention. Deleted Checkpoint descriptors, descriptor
group pages and fixed management areas remain, as does the active root in a
historical DIFF. Files need not return to their initial allocated size or length;
used Checkpoint IDs remain unavailable for reuse. Independent Retire still owns
whole-file withdrawal/deletion. Runtime and offline usage belongs to
[sandbox lifecycle and CLI](sandbox.md#211-local-disk-checkpoints).

## Migration and validation

| Previous entry | Current entry |
| --- | --- |
| `vhost.OpenBlockCOW`, `vhost.BlockCOW` | `diffstore.OpenDisk`, `diffstore.Disk` |
| `vhost.BlockCOWOption` | `diffstore.DiskOption` |
| `vhost.NewCOWCache`, `WithCOWCache`, `COWCache` | `diffstore.NewCache`, `WithCache`, `Cache` |
| `vhost.COWCacheStats` | `diffstore.CacheStats` |
| `vhost.NewStreamReader` / `StreamReader` | `diffstore.NewStreamReader` / `StreamReader` |
| `internal/diffstore` administrative APIs | `pkg/diffstore` administrative APIs |
| Storage tests formerly in `pkg/vhost` | `pkg/diffstore` |
| B+ engine tests formerly in `internal/diffstore` | `pkg/diffstore/internal/versioned` |

The earlier package relocation in this table changed Go source imports without
changing CLI/configuration or disk bytes. The presence-enabled v3/v4 formats are a
separate, incompatible working-file change: export retained raw/v1/v2 work-file
content with the old version before upgrading. Provisioning templates remain
immutable inputs with their existing raw/encrypted input contract; their readers
do not reopen or upgrade the source as an active working file. Existing raw ext4
preparation and old portable tarstream import remain supported.

All repository callers use the relocated API. `vhost.BlockReader` remains a type alias
for the source accepted by read-only transport devices. Storage benchmark flags
and `VHOST_*` environment names remain compatible; their compiled test package is
now `./pkg/diffstore`. The existing ENOSPC script name remains a CI entry point.

Run `go test ./...`, `go test -race ./pkg/diffstore/... ./pkg/vhost ./pkg/sandbox
./pkg/restore ./pkg/artifact ./pkg/snapshot`, `go vet ./...`, `make test-e2e-scripts`,
`bash scripts/test-vhost-tmpfs-runner.sh`, and the real
`scripts/test-vhost-tmpfs-enospc.sh`. Direct-I/O tests need a supported filesystem
for `TMPDIR`; userfaultfd transport tests and isolated ENOSPC need their documented
privileges. A skip or prerequisite failure is not passing coverage.

Contract tests cover both formats with plaintext/XTS, partial writes over a base,
shared-cache ownership, cancellation, active sparse exports, named-view pins and
unsupported checkpoint capability. Existing format, corruption, recovery,
short-I/O, failure cleanup, concurrency and transport tests remain at their owning
layer. Performance methods and interpretation remain in [sandbox validation](sandbox.md).
