package sandbox

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
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
					cfg, _ := snapshot.MarshalConfig(&snapshot.Config{Version: 1, SandboxRef: "manifest://" + strings.Repeat("a", 64), FromRefs: parents})
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
