package diffstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

func TestPresenceZeroDiscardReopen(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		for _, encrypted := range []bool{false, true} {
			t.Run(fmt.Sprintf("checkpoint=%t/encrypted=%t", checkpoint, encrypted), func(t *testing.T) {
				const size = 8 * cowBlockSize
				path := filepath.Join(t.TempDir(), "active.diff")
				base := &fakeReader{data: bytes.Repeat([]byte{0x6d}, size)}
				var opts []DiskOption
				if checkpoint {
					opts = append(opts, WithCheckpoint("zero-test", "base"))
				}
				if encrypted {
					opts = append(opts, WithDiffEncryption(testDiffKey(31), true))
				}
				d, err := OpenDisk(path, base, DiffInit{CreateSize: size}, opts...)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = d.Close() }()
				data := bytes.Repeat([]byte{0xa5}, 5*cowBlockSize)
				if _, err := d.WriteAt(data, cowBlockSize); err != nil {
					t.Fatal(err)
				}
				if err := d.Drain(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := d.Discard(cowBlockSize, cowBlockSize, true); err != nil {
					t.Fatal(err)
				}
				if err := d.Discard(2*cowBlockSize, cowBlockSize, false); err != nil {
					t.Fatal(err)
				}
				if err := d.Discard(6*cowBlockSize, cowBlockSize, false); err != nil {
					t.Fatal(err)
				}
				// Only page 4 is fully covered; neither boundary page is changed.
				if err := d.Discard(3*cowBlockSize+17, 3*cowBlockSize-34, false); err != nil {
					t.Fatal(err)
				}
				view, err := d.Export(context.Background(), ActiveDiff)
				if err != nil {
					t.Fatal(err)
				}
				kinds := []sparse.RunKind{sparse.Hole, sparse.Hole, sparse.Zero, sparse.Data, sparse.Zero, sparse.Data, sparse.Zero, sparse.Hole}
				for i, want := range kinds {
					run, err := view.RunAt(uint64(i*cowBlockSize), cowBlockSize)
					if err != nil || run.Kind() != want {
						t.Fatalf("page %d kind=%v err=%v", i, run, err)
					}
				}
				_ = view.Close()
				if checkpoint {
					if err := d.Checkpoint(context.Background(), "zero"); err != nil {
						t.Fatal(err)
					}
				}
				// Partial writes to Zero initialize from zero, never from the base.
				if _, err := d.WriteAt([]byte{1, 2, 3}, 2*cowBlockSize+37); err != nil {
					t.Fatal(err)
				}
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
				d, err = OpenDisk(path, base, DiffInit{Existing: true}, opts...)
				if err != nil {
					t.Fatal(err)
				}
				want := append([]byte(nil), base.data...)
				for _, p := range []int{2, 4, 6} {
					clear(want[p*cowBlockSize : (p+1)*cowBlockSize])
				}
				for _, p := range []int{3, 5} {
					copy(want[p*cowBlockSize:], data[:cowBlockSize])
				}
				copy(want[2*cowBlockSize+37:], []byte{1, 2, 3})
				got := make([]byte, size)
				if _, err := d.ReadAt(got, 0); err != nil || !bytes.Equal(got, want) {
					t.Fatal("reopened logical contents", err)
				}
				if d.DirtyCount() != 5 {
					t.Fatal("presence lost Zero", d.DirtyCount())
				}
				if checkpoint {
					v, err := d.Export(context.Background(), "zero")
					if err != nil {
						t.Fatal(err)
					}
					defer v.Close()
					r, err := v.RunAt(2*cowBlockSize, cowBlockSize)
					if err != nil || r.Kind() != sparse.Zero {
						t.Fatal("checkpoint bitmap changed", r, err)
					}
				}
			})
		}
	}
}

func TestActiveZeroExportIncludesAcceptedWrite(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		t.Run(fmt.Sprint(checkpoint), func(t *testing.T) {
			gate := make(chan struct{})
			var once sync.Once
			cache, err := newCache(4*cowBlockSize, 2*cowBlockSize, cacheHooks{beforeWrite: func([]*cachePage) error { <-gate; return nil }})
			if err != nil {
				t.Fatal(err)
			}
			defer cache.Close()
			opts := []DiskOption{WithCache(cache)}
			if checkpoint {
				opts = append(opts, WithCheckpoint("cached-zero", ""))
			}
			d, err := OpenDisk(filepath.Join(t.TempDir(), "d"), nil, DiffInit{CreateSize: 2 * cowBlockSize}, opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			defer once.Do(func() { close(gate) })
			if err := d.Discard(0, cowBlockSize, false); err != nil {
				t.Fatal(err)
			}
			if _, err := d.WriteAt([]byte{0x51}, 17); err != nil {
				t.Fatal(err)
			}
			v, err := d.Export(context.Background(), ActiveDiff)
			if err != nil {
				t.Fatal(err)
			}
			defer v.Close()
			r, err := v.RunAt(0, cowBlockSize)
			if err != nil || r.Kind() != sparse.Data {
				t.Fatal("cached payload classified as Zero", r, err)
			}
			p := make([]byte, cowBlockSize)
			if _, err := v.ReadAt(context.Background(), p, 0); err != nil || p[17] != 0x51 {
				t.Fatal(err)
			}
			p[17] = 0
			if !allZero(p) {
				t.Fatal("partial write leaked base")
			}
		})
	}
}

type partialNewPayload struct {
	diffBodyIO
	body   int64
	failed bool
}

func (f *partialNewPayload) WriteAt(p []byte, off int64) (int, error) {
	if off >= f.body && !f.failed {
		f.failed = true
		if _, err := f.diffBodyIO.WriteAt(p, off); err != nil {
			return 0, err
		}
		return 512, io.ErrShortWrite
	}
	return f.diffBodyIO.WriteAt(p, off)
}

func TestSingleZeroFirstPayloadFailureCleanup(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "d")
			var enc *diffEncryption
			if encrypted {
				enc = mustDiffEncryption(t, testDiffKey(22))
			}
			d, err := initializeFreshDiffFile(path, 2*cowBlockSize, enc, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = d.Close() }()
			if err := d.discard(context.Background(), 0, cowBlockSize, false); err != nil {
				t.Fatal(err)
			}
			d.bodyIO = &partialNewPayload{diffBodyIO: d.bodyIO, body: d.bodyOffset}
			if _, err := d.WriteAt(bytes.Repeat([]byte{0x81}, cowBlockSize), 0); !errors.Is(err, io.ErrShortWrite) {
				t.Fatal(err)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			d, err = openExistingDiffFile(path, enc, encrypted, false)
			if err != nil {
				t.Fatal(err)
			}
			kind, _ := d.classify(0, cowBlockSize)
			if kind != sparse.Zero {
				t.Fatal("failed first payload changed Zero", kind)
			}
			p := make([]byte, cowBlockSize)
			if _, err := d.ReadAt(p, 0); err != nil || !allZero(p) {
				t.Fatal("failed bytes exposed", err)
			}
		})
	}
}

func TestLegacyDiffRejectedWithoutMutation(t *testing.T) {
	for _, version := range []byte{0, 1, 2, 7} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "old")
			p := bytes.Repeat([]byte{0x7a}, 2*cowBlockSize)
			if version != 0 {
				copy(p, diffMagic[:])
				p[8] = 0
				p[9] = version
			}
			if err := os.WriteFile(path, p, 0600); err != nil {
				t.Fatal(err)
			}
			if d, err := OpenDisk(path, nil, DiffInit{Existing: true}); !errors.Is(err, ErrFormat) {
				if d != nil {
					_ = d.Close()
				}
				t.Fatal("old format accepted", err)
			}
			got, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(p, got) {
				t.Fatal("old file mutated", err)
			}
		})
	}
}

func TestZeroTemplateSeedKeepsPayloadSparse(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprint(encrypted), func(t *testing.T) {
			var enc *diffEncryption
			if encrypted {
				enc = mustDiffEncryption(t, testDiffKey(19))
			}
			dir := t.TempDir()
			source, err := initializeFreshDiffFile(filepath.Join(dir, "source"), 130*cowBlockSize, enc, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := source.discard(context.Background(), 0, 130*cowBlockSize, false); err != nil {
				t.Fatal(err)
			}
			if err := source.Close(); err != nil {
				t.Fatal(err)
			}
			template, err := openDiffTemplate(filepath.Join(dir, "source"), enc)
			if err != nil {
				t.Fatal(err)
			}
			defer template.Close()
			target, err := initializeFreshDiffFile(filepath.Join(dir, "target"), 130*cowBlockSize, enc, template)
			if err != nil {
				t.Fatal(err)
			}
			defer target.Close()
			for off := uint64(0); off < uint64(target.Size()); off += cowBlockSize {
				kind, _ := target.classify(off, off+cowBlockSize)
				if kind != sparse.Zero {
					t.Fatal("template Zero expanded", off, kind)
				}
			}
		})
	}
}
