package sandbox

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/memory"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
)

func TestDirtyHistorySingleMultipleAndUnprovedParents(t *testing.T) {
	for _, multi := range []bool{false, true} {
		for _, proved := range []bool{false, true} {
			for _, merge := range []bool{false, true} {
				ctx := context.Background()
				dir := t.TempDir()
				external := []string{"manifest://" + strings.Repeat("e", 64), "manifest://" + strings.Repeat("f", 64)}
				opts := RunOptions{checkpointDir: dir, Fetcher: &checkpointNoFetch{t: t}}
				write := func(parents []string, bit byte) (string, string) {
					sink := snapshot.NewFileSink(dir, "sid", nil, false, nil)
					mem, _ := sparse.NewSource(bytes.NewReader(bytes.Repeat([]byte{9}, 3*4096)), 3*4096, nil)
					dirty, _ := snapshotfile.EncodeDirty(3*4096, []byte{bit})
					cfg, err := snapshot.MarshalConfig(&snapshot.Config{Version: 1, SandboxRef: "manifest://" + strings.Repeat("a", 64), FromRefs: parents})
					if err != nil {
						t.Fatalf("parents=%v: %v", parents, err)
					}
					src, err := snapshotfile.BuildSourceWithDirty(mem, []byte("{}"), []byte("{}"), cfg, dirty)
					if err != nil {
						t.Fatal(err)
					}
					ref, path, err := sink.AbsorbSnapshot(ctx, src)
					if err != nil {
						t.Fatal(err)
					}
					sink.Close()
					return ref, path
				}
				actual := append([]string(nil), external...)
				if multi {
					older, _ := write(external, 1)
					actual = append([]string{older}, actual...)
				}
				declared := append([]string(nil), actual...)
				if !proved {
					declared = append([]string(nil), external...)
					declared[0], declared[1] = declared[1], declared[0]
				}
				latest, path := write(declared, 2)
				r, _ := manifest.ParseRef(latest)
				r.Path = path
				opts.MemoryBinding = &MemorySourceBinding{SnapshotRef: latest, RuntimeRef: r.String(), RelativeDir: filepath.Dir(path), FromRefs: actual}
				h, err := prepareMemoryHistory(ctx, opts, merge, 3*4096)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(h.refs, external) {
					t.Fatal("external boundary or order changed", h.refs)
				}
				for page := 0; page < 3; page++ {
					r, err := h.memory.RunAt(uint64(page*4096), 4096)
					if err != nil {
						t.Fatal(err)
					}
					want := sparse.Hole
					if !proved || page == 1 || multi && page == 0 {
						want = sparse.Data
					}
					if r.Kind() != want {
						t.Fatalf("multi=%t proved=%t merge=%t page=%d kind=%v want=%v", multi, proved, merge, page, r.Kind(), want)
					}
					if want == sparse.Data {
						got := make([]byte, 4096)
						if _, err := r.ReadAt(ctx, got, 0); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{9}, 4096)) {
							t.Fatal("historical final bytes", err)
						}
					}
				}
				_, stats, err := snapshot.BuildCaptureSource(ctx, nil, h.memory, nil, []byte("{}"), []byte("{}"), []byte("{}"), h.fullMemory, h.fullMemory, h.proofCandidates, nil)
				if err != nil {
					t.Fatal(err)
				}
				wantExtra := uint64(0)
				if !proved {
					wantExtra = 2 * 4096
					if multi {
						wantExtra = 4096
					}
				}
				if stats.ParentProofExtraDataBytes != wantExtra || stats.ParentProofExtraZeroBytes != 0 || (stats.ParentProofFallback != "") != !proved || stats.ExtraDataBytes != 0 {
					t.Fatalf("multi=%t proved=%t merge=%t proof costs: %+v", multi, proved, merge, stats)
				}
				if err := h.Close(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}
func TestMemoryParentProofUsesCompleteOrderedIdentities(t *testing.T) {
	a, b := "manifest://"+strings.Repeat("a", 64), "manifest://"+strings.Repeat("b", 64)
	fileA := (manifest.Ref{Scheme: manifest.RefSchemeFile, Path: "/changed/location.bundle", DigestScheme: "manifest", Digest: strings.Repeat("a", 64)}).String()
	if !memoryParentViewEqual([]string{a, b}, []string{fileA, b}) {
		t.Fatal("physical relocation changed identity")
	}
	for _, other := range [][]string{{b, a}, {a}, {a, b, a}, {"file://unbound", b}, {b, b}} {
		if memoryParentViewEqual([]string{a, b}, other) {
			t.Fatal("unproved ordered suffix accepted", other)
		}
	}
}

func TestDirtyHistoryFinalValuesAndZeroAgainstActualParent(t *testing.T) {
	for _, multi := range []bool{false, true} {
		for _, merge := range []bool{false, true} {
			ctx := context.Background()
			dir := t.TempDir()
			mk := func(values []byte, zero byte) sparse.Source {
				data := make([]byte, 3*4096)
				for page, value := range values {
					for i := page * 4096; i < (page+1)*4096; i++ {
						data[i] = value
					}
				}
				src, _, err := snapshot.CaptureMemory(bytes.NewReader(data), &memory.CaptureView{Size: 3 * 4096, Materialized: []byte{7 &^ zero}, Dirty: []byte{7}, Released: []byte{zero}}, true)
				if err != nil {
					t.Fatal(err)
				}
				return src
			}
			write := func(where string, parents []string, src sparse.Source, bit byte) (string, string) {
				sink := snapshot.NewFileSink(where, "sid", nil, false, nil)
				defer sink.Close()
				dirty, _ := snapshotfile.EncodeDirty(3*4096, []byte{bit})
				cfg, err := snapshot.MarshalConfig(&snapshot.Config{Version: 1, SandboxRef: "manifest://" + strings.Repeat("a", 64), FromRefs: parents})
				if err != nil {
					t.Fatalf("parents=%v: %v", parents, err)
				}
				out, err := snapshotfile.BuildSourceWithDirty(src, []byte("{}"), []byte("{}"), cfg, dirty)
				if err != nil {
					t.Fatal(err)
				}
				ref, path, err := sink.AbsorbSnapshot(ctx, out)
				if err != nil {
					t.Fatal(err)
				}
				return ref, path
			}
			base := mk([]byte{0, 7, 9}, 0)
			ext, extPath := write(t.TempDir(), nil, base, 7)
			parsed, _ := manifest.ParseRef(ext)
			parsed.Path = filepath.Base(extPath)
			parsed.Location = "template"
			ext = parsed.String()
			parents := []string{ext}
			first := byte(0)
			if multi {
				older, _ := write(dir, parents, mk([]byte{1, 7, 9}, 0), 1)
				parents = append([]string{older}, parents...)
				first = 1
			}
			latest, path := write(dir, parents, mk([]byte{first, 0, 9}, 2), 2)
			parsed, _ = manifest.ParseRef(latest)
			parsed.Path = path
			opts := RunOptions{checkpointDir: dir, RefLocations: manifest.RefLocations{"template": filepath.Dir(extPath)}, Fetcher: &checkpointNoFetch{t: t}, MemoryBinding: &MemorySourceBinding{SnapshotRef: latest, RuntimeRef: parsed.String(), RelativeDir: dir, FromRefs: parents}}
			h, err := prepareMemoryHistory(ctx, opts, merge, 3*4096)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(h.refs, []string{ext}) {
				t.Fatal("external parent changed", h.refs)
			}
			q, err := h.memory.RunAt(4096, 4096)
			if err != nil || q.Kind() != sparse.Zero {
				t.Fatal("release Zero lost", err)
			}
			full := fetch.NewLayered(&checkpointSourceStream{Source: h.memory, close: func() error { return nil }}, &checkpointSourceStream{Source: base, close: func() error { return nil }})
			got := make([]byte, 3*4096)
			if _, err = full.ReadAt(ctx, got, 0); err != nil {
				t.Fatal(err)
			}
			want := append(bytes.Repeat([]byte{first}, 4096), make([]byte, 4096)...)
			want = append(want, bytes.Repeat([]byte{9}, 4096)...)
			if !bytes.Equal(got, want) {
				t.Fatalf("multi=%t merge=%t full memory differs", multi, merge)
			}
			if err = h.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
