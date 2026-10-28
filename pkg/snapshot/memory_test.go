package snapshot

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"github.com/kuasar-sandbox/sandboxer/pkg/memory"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
)

func pageMemory(kinds []sparse.RunKind, values []byte) sparse.Source {
	source := &memorySource{size: uint64(len(kinds)) * 4096}
	source.resolve = func(off, limit uint64) (sparse.Run, error) {
		end := min(off+limit, (off/4096+1)*4096)
		return memoryRun{off: off, end: end, kind: kinds[off/4096], read: func(_ context.Context, p []byte, offset uint64) (int, error) {
			for i := range p {
				p[i] = values[(offset+uint64(i))/4096]
			}
			return len(p), nil
		}}, nil
	}
	return source
}
func TestCaptureMemoryCompleteLogicalView(t *testing.T) {
	ctx := context.Background()
	const pages = 3
	baseline := bytes.Repeat([]byte{0x31}, pages*4096)
	for mask := 0; mask < 1<<(pages*3); mask++ {
		v := &memory.CaptureView{Size: pages * 4096, Materialized: []byte{byte(mask & 7)}, Dirty: []byte{byte(mask >> 3 & 7)}, Released: []byte{byte(mask >> 6 & 7)}}
		data := append([]byte(nil), baseline...)
		want := append([]byte(nil), baseline...)
		for i := uint64(0); i < pages; i++ {
			if pageBit(v.Materialized, i) {
				if pageBit(v.Dirty, i) {
					for j := i * 4096; j < (i+1)*4096; j++ {
						data[j] = 0x73
						want[j] = 0x73
					}
				}
			} else if pageBit(v.Released, i) {
				clear(want[i*4096 : (i+1)*4096])
			}
		}
		for _, working := range []bool{false, true} {
			source, dirty, err := CaptureMemory(bytes.NewReader(data), v, working)
			if err != nil {
				t.Fatal(err)
			}
			if err := snapshotfile.ValidateDirty(source, dirty); err != nil {
				t.Fatal(err)
			}
			parent, _ := sparse.NewSource(bytes.NewReader(baseline), uint64(len(baseline)), nil)
			full := fetch.NewLayered(memoryBorrow{source}, memoryBorrow{parent})
			got := make([]byte, len(want))
			if _, err := full.ReadAt(ctx, got, 0); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("mask=%d working=%t logical bytes differ: %v", mask, working, err)
			}
			if working && v.Materialized[0]&^v.Dirty[0] != 0 && dirty == nil {
				t.Fatal("clean working copy lost metadata")
			}
		}
	}
}
func TestHistorySelectsFinalValueFromFullView(t *testing.T) {
	ctx := context.Background()
	latest := pageMemory([]sparse.RunKind{sparse.Data, sparse.Zero, sparse.Hole}, []byte{9, 0, 0})
	older := pageMemory([]sparse.RunKind{sparse.Data, sparse.Data, sparse.Data}, []byte{7, 7, 7})
	clean, _ := snapshotfile.EncodeDirty(latest.Size(), []byte{0})
	newerCandidate, err := DirtyMemory(latest, clean)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := SelectMemory(fetch.NewLayered(memoryBorrow{latest}, memoryBorrow{older}), fetch.NewLayered(memoryBorrow{newerCandidate}, memoryBorrow{older}))
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 3*4096)
	if _, err := selected.ReadAt(ctx, got, 0); err != nil {
		t.Fatal(err)
	}
	want := append(bytes.Repeat([]byte{9}, 4096), make([]byte, 4096)...)
	want = append(want, bytes.Repeat([]byte{7}, 4096)...)
	if !bytes.Equal(got, want) {
		t.Fatal("candidate bytes replaced full V")
	}
	r, _ := selected.RunAt(4096, 4096)
	if r.Kind() != sparse.Zero {
		t.Fatal("logical Zero lost")
	}
}
func TestMemoryFallbackDoesNotExpandOwnership(t *testing.T) {
	ctx := context.Background()
	// P=[0,8192), with a required released Zero beyond P and an external Hole.
	required := pageMemory([]sparse.RunKind{sparse.Data, sparse.Zero, sparse.Zero, sparse.Hole}, []byte{3, 0, 0, 0})
	coverage := pageMemory([]sparse.RunKind{sparse.Data, sparse.Data, sparse.Hole, sparse.Hole}, []byte{1, 1, 0, 0})
	fallback, err := MemoryFallback(required, coverage, required)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []sparse.RunKind{sparse.Data, sparse.Data, sparse.Zero, sparse.Hole} {
		r, err := fallback.RunAt(uint64(i*4096), 4096)
		if err != nil || r.Kind() != want {
			t.Fatalf("page %d: %v %v", i, r, err)
		}
	}
	var encoded bytes.Buffer
	_, digest, err := tarstream.WriteTo(ctx, &encoded, "snapshot", fallback)
	if err != nil {
		t.Fatal(err)
	}
	reopened, _, err := tarstream.SourceFrom(bytes.NewReader(encoded.Bytes()), "", tarstream.WithExpectedDigest(tarstream.DigestScheme, digest))
	if err != nil {
		t.Fatal(err)
	}
	got, want := make([]byte, 4*4096), make([]byte, 4*4096)
	if _, err := reopened.ReadAt(ctx, got, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := required.ReadAt(ctx, want, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("fallback bytes changed")
	}
	r, _ := reopened.RunAt(3*4096, 4096)
	if r.Kind() != sparse.Hole {
		t.Fatal("external hole filled")
	}
}
func TestMemoryBudgetIncludesSentinelAndFallsBackWithoutDataRead(t *testing.T) {
	ctx := context.Background()
	const size = uint64(8 << 30)
	reader := func(context.Context, []byte, uint64) (int, error) {
		t.Fatal("planner read payload")
		return 0, io.ErrUnexpectedEOF
	}
	alternating := &memorySource{size: size, resolve: func(off, limit uint64) (sparse.Run, error) {
		kind := sparse.Data
		if off/4096%2 == 1 {
			kind = sparse.Hole
		}
		return memoryRun{off: off, end: min(off+limit, (off/4096+1)*4096), kind: kind, read: reader}, nil
	}}
	dense := &memorySource{size: size, resolve: func(off, limit uint64) (sparse.Run, error) {
		return memoryRun{off: off, end: off + limit, kind: sparse.Data, read: reader}, nil
	}}
	source, err := snapshotfile.BuildSource(alternating, []byte("{}"), []byte("{}"), []byte("{}"))
	if err != nil {
		t.Fatal(err)
	}
	if err = checkMemoryCarrier(ctx, source, 1<<20, ^uint64(0)); !errors.Is(err, errMemoryBudget) {
		t.Fatalf("hard extent cap: %v", err)
	}
	if _, _, err = tarstream.WriteTo(ctx, io.Discard, "snapshot", source); err == nil {
		t.Fatal("real writer accepted over-limit map")
	}
	built, stats, err := BuildCaptureSource(ctx, NewFileSink(t.TempDir(), "sid", nil, false, nil), alternating, nil, []byte("{}"), []byte("{}"), []byte("{}"), dense, dense, nil)
	if err != nil {
		t.Fatal(err)
	}
	if stats.DataBytes != size || stats.ExtraDataBytes != size/2 || stats.Fallback == "" || stats.BitmapBytes != 0 {
		t.Fatalf("fallback stats: %+v", stats)
	}
	if err = checkMemoryCarrier(ctx, built, 2, memoryIndexBudget); err != nil {
		t.Fatal("dense plus sentinel", err)
	}
	// A P which is itself too fragmented must fail; it cannot fill external holes.
	if _, _, err = BuildCaptureSource(ctx, NewFileSink(t.TempDir(), "sid", nil, false, nil), alternating, nil, []byte("{}"), []byte("{}"), []byte("{}"), alternating, alternating, nil); err == nil {
		t.Fatal("expanded P to evade budget")
	}
	// No tarstream cap is imposed on Bundle/ingest planning.
	if _, stats, err = BuildCaptureSource(ctx, nil, alternating, nil, []byte("{}"), []byte("{}"), []byte("{}"), dense, dense, nil); err != nil || stats.Fallback != "" {
		t.Fatal("non-file carrier inherited tarstream budget", err)
	}
}
func TestCaptureMayDropOnlyNewBitmapOnTailOverflow(t *testing.T) {
	const size = uint64(1 << 40)
	dense := &memorySource{size: size, resolve: func(off, limit uint64) (sparse.Run, error) {
		return memoryRun{off: off, end: off + limit, kind: sparse.Data}, nil
	}}
	dirty, err := snapshotfile.EncodeDirty(size, make([]byte, size/4096/8))
	if err != nil {
		t.Fatal(err)
	}
	config, state := bytes.Repeat([]byte{' '}, 16<<20), bytes.Repeat([]byte{' '}, 16<<20)
	if _, err := snapshotfile.BuildSourceWithDirty(dense, config, state, []byte("{}"), dirty); !errors.Is(err, snapshotfile.ErrTailBudget) {
		t.Fatal("ordinary reconstruction dropped bitmap", err)
	}
	_, stats, err := BuildCaptureSource(context.Background(), nil, dense, dirty, config, state, []byte("{}"), dense, dense, nil)
	if err != nil || stats.BitmapBytes != 0 || stats.Fallback == "" {
		t.Fatalf("new capture tail fallback: %+v %v", stats, err)
	}
}
