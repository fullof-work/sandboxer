# Writable disk storage (`pkg/diffstore`)

`pkg/diffstore` owns writable block storage independently of vhost transport. Both
single-layer raw/v1 files and checkpoint-capable v2 files use the same `Disk`
frontend. The refactor does not convert existing files or change format defaults.

## Layers and dependencies

| Layer | Owns | Must not own |
| --- | --- | --- |
| Sandbox / restore / artifact | Disk topology, immutable base composition, VM pause, multi-disk checkpoint coordination, publication | Physical page layout or cache state |
| `pkg/vhost` | Protocol, virtqueues, guest memory, request statistics; queue context forwarded to `Disk` | Writable files, COW cache or checkpoint transactions |
| `pkg/diffstore.Disk` | Admission/lifetime, logical presence, partial writes, discard, shared cache attachment, template materialization and export boundaries | VM or multi-disk atomicity |
| Private `diffBackend` | Synchronous logical-page I/O, presence, format-specific failure cleanup, sync and close | Frontend cache pages or ownership of borrowed page payloads |
| Single-layer `diffFile` | Raw/v1 header, sparse file offsets, owned direct-I/O read/write workspaces | B+ tree or checkpoint references |
| `internal/versioned` via `checkpointDiff` | v2 B+ tree, transactions, recovery, version references, reader pins, garbage collection | Frontend admission or cache draining |
| `internal/directio`, `internal/sectorcrypt` | Shared mmap alignment/lifetime and logical 512-byte XTS transforms | Pool policy, file headers, authentication or mapping policy |

The dependency direction is transport/application -> `pkg/diffstore` -> its
private implementations. Filesystem preflight helpers sit above the physical
backends; neither physical backend interprets ext4. Immutable fetch streams use
`StreamReader` as a source adapter. The v2 implementation is inaccessible to other
packages through Go's `internal` rule.

The public `Store`/`Options` aliases and `Create`, `Open`, `OpenMetadata`, `Probe`,
`OpenExport`, and `LockDirectory` functions expose the existing v2 engine handle
for **offline** import, inspection and administration. They are intentionally
separate from `OpenDisk`: they neither attach the sandbox cache nor provide a
live disk's admission/drain boundary. Applications must not open a second writable
`Store` for a live `Disk`, or use offline APIs to bypass that disk's checkpoint
boundary. This refactor retains the administrative API instead of introducing a
second implementation of version management. The v2 `ErrEncryptionRequired` and
`ErrAuthentication` sentinels are public for `errors.Is` checks through all v2
entry points; they are distinct from the single-layer `ErrDiff*` sentinels.

## Choosing a format

```go
// Raw single-layer by default. WithDiffEncryption selects encrypted v1.
disk, err := diffstore.OpenDisk(path, base, init,
    diffstore.WithCache(cache))

// Explicit stable identities select checkpoint-capable v2.
disk, err := diffstore.OpenDisk(path, base, init,
    diffstore.WithCache(cache),
    diffstore.WithCheckpoint(diffID, baseID),
    diffstore.WithDiffEncryption(customerKey, true))
```

Check `err` before using a disk. `DiffInit{Existing: true}` opens an existing file;
`CreateSize` or `TemplatePath` provisions a fresh file. v2 opens authenticate and
check the expected identity/base and never adopt raw/v1 files. A plaintext
provisioning template remains allowed when the new active file must be encrypted.

| Property | Single layer (raw/v1) | Versioned (v2) |
| --- | --- | --- |
| Runtime | Same `Disk` and shared `Cache` | Same `Disk` and shared `Cache` |
| Existing-file presence | Eager sparse-file scan | Unknown/absent/present, bounded resolution on first access |
| Writeback | Contiguous logical runs | Bounded transactions may contain disjoint logical pages |
| Template staging | One 4 KiB page | Up to `maxWritebackPages` (256) 4 KiB pages |
| Checkpoint capability | Unavailable | `CheckpointManager` |
| Physical failure cleanup | Hole-punch only newly created upper pages | Transaction/allocation engine owns recovery; never punch guest offsets in the mixed physical file |

The private batch consists of logical block number, borrowed plaintext bytes and
whether the upper page was fresh. It exposes no `cachePage`. The cache freezes and
charges pages until write and failure cleanup finish, and publishes sticky failure
before cleanup can block. The single-layer backend stages into its existing
workspace; v2 performs its existing mapping commit. Buffer pools, request batching
limits and encryption domains retain their existing policies.

Fresh creation shares the claim/recheck/template/temp-file flow, and publication
shares file sync, atomic rename, claim release and directory sync. Materialization
uses one dispatcher and the same stripe/presence conflict checks. v2 retains
batched template commits; single-layer retains one-page staging. Template bytes
are read before taking write stripes, then presence is rechecked so frontend
writes/discards win. A fully superseded range may consequently be read and then
skipped. `WaitMaterialReady` and `WaitMaterialized` retain distinct readiness and
publication boundaries.

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

v2 live reopen does not wait for a full presence scan. Only an operation requiring
a complete active sparse export resolves all unknown presence. Named version
exports retain their independent index view. Errors never turn unknown presence
into a lower-layer hole.

## Discard and reclamation

`Disk.Discard` withdraws complete 4 KiB pages from the active upper layer. Reads
then fall back to the immutable lower layer; partial edge pages retain their
contents. It does not mean Guest DISCARD, explicit zero pages or zero compression.
Each batch covers at most 64 full pages. It takes distinct stripe locks in numeric
order, checks all existing cache pages under one cache lock, and only then freezes
them together. Loading or writeback makes the whole batch wait without freezing
any part. Uncached pages require no placeholder and unrelated dirty pages need no
drain. Backend I/O runs without the cache lock and receives a context. Cancellation
can stop a batch before freezing; afterward it must publish the whole batch or
enter the existing sticky failure path and wake waiters. Success publishes absent
presence and resolves template backfill for every page, including already absent
pages. Separate batches do not provide an all-or-nothing range transaction.

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
publishes no free slots; reopening may safely repeat a punch. No new format version,
larger redo area, payload relocation, file truncation or global free-range index
is introduced. Encryption and direct-I/O alignment retain their existing rules.

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

This changes Go source imports; it does not change CLI/configuration or disk bytes.
All repository callers migrate together. `vhost.BlockReader` remains a type alias
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
