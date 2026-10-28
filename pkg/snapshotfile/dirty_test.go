package snapshotfile

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tailzip"
)

type dirtyTestMemory struct {
	size  uint64
	holes []sparse.Extent
	split uint64
}

func (s dirtyTestMemory) Size() uint64 { return s.size }
func (s dirtyTestMemory) Close() error { return nil }
func (s dirtyTestMemory) ReadAt(_ context.Context, b []byte, off uint64) (int, error) {
	if off >= s.size {
		return 0, io.EOF
	}
	n := min(uint64(len(b)), s.size-off)
	clear(b[:n])
	if n < uint64(len(b)) {
		return int(n), io.EOF
	}
	return len(b), nil
}
func (s dirtyTestMemory) RunAt(off, limit uint64) (sparse.Run, error) {
	if off >= s.size {
		return nil, io.EOF
	}
	end := min(s.size, off+limit)
	kind := sparse.Data
	if s.split != 0 {
		end = min(end, (off/s.split+1)*s.split)
		if off/s.split%2 == 1 {
			kind = sparse.Zero
		}
	}
	for _, h := range s.holes {
		if off >= h.Offset && off < h.Offset+h.Size {
			kind = sparse.Hole
			end = min(end, h.Offset+h.Size)
			break
		}
		if h.Offset > off {
			end = min(end, h.Offset)
		}
	}
	return dirtyTestRun{off, end, kind}, nil
}

type dirtyTestRun struct {
	off, end uint64
	kind     sparse.RunKind
}

func (r dirtyTestRun) Offset() uint64       { return r.off }
func (r dirtyTestRun) End() uint64          { return r.end }
func (r dirtyTestRun) Kind() sparse.RunKind { return r.kind }
func (r dirtyTestRun) ReadAt(_ context.Context, b []byte, off uint64) (int, error) {
	if off > r.end-r.off || uint64(len(b)) > r.end-r.off-off {
		return 0, io.EOF
	}
	clear(b)
	return len(b), nil
}

type dirtyTestStream struct{ sparse.Source }

func (dirtyTestStream) Close() error { return nil }

func TestDirtyPresenceAndFullMemory(t *testing.T) {
	for _, value := range []int{-1, 0, 1, 3} {
		t.Run(string(rune('b'+value)), func(t *testing.T) {
			var dirty []byte
			if value >= 0 {
				var err error
				dirty, err = EncodeDirty(8192, []byte{byte(value)})
				if err != nil {
					t.Fatal(err)
				}
			}
			source, err := BuildSourceWithDirty(dirtyTestMemory{size: 8192, split: 997}, []byte("{}"), []byte("{}"), []byte("version: 1\n"), dirty)
			if err != nil {
				t.Fatal(err)
			}
			root, err := Open(context.Background(), dirtyTestStream{source})
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			if !bytes.Equal(root.MemoryDirty, dirty) || (root.MemoryDirty == nil) != (dirty == nil) {
				t.Fatal("entry presence/body changed")
			}
			for off := uint64(0); off < 8192; {
				run, err := root.Memory.RunAt(off, 8192-off)
				if err != nil {
					t.Fatal(err)
				}
				if run.Kind() == sparse.Hole {
					t.Fatal("Open filtered clean explicit memory")
				}
				off = run.End()
			}
		})
	}
	// Legacy byte geometry remains valid without the optional entry.
	source, err := BuildSource(dirtyTestMemory{size: 37, holes: []sparse.Extent{{Offset: 3, Size: 7}}}, []byte("{}"), []byte("{}"), []byte("version: 1\n"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := Open(context.Background(), dirtyTestStream{source})
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	run, err := root.Memory.RunAt(3, 34)
	if err != nil || run.Kind() != sparse.Hole || run.End() != 10 {
		t.Fatalf("legacy geometry changed: %v %v", run, err)
	}
}

func TestDirtyRejectsInvalidEncodingAndCoverage(t *testing.T) {
	good, err := EncodeDirty(8192, []byte{3})
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]func([]byte) []byte{
		"version": func(b []byte) []byte { b[0] = 2; return b }, "shift": func(b []byte) []byte { b[4] = 13; return b },
		"size":      func(b []byte) []byte { binary.LittleEndian.PutUint64(b[8:], 4096); return b },
		"unaligned": func(b []byte) []byte { binary.LittleEndian.PutUint64(b[8:], 8191); return b },
		"unused":    func(b []byte) []byte { b[16] = 4; return b }, "short": func(b []byte) []byte { return b[:16] },
		"long": func(b []byte) []byte { return append(b, 0) }, "empty": func(b []byte) []byte { return b[:0] },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			body := mutate(bytes.Clone(good))
			tail, err := tailzip.EncodeCanonical([]tailzip.Entry{{Name: ConfigJSONName, Body: []byte("{}")}, {Name: StateJSONName, Body: []byte("{}")}, {Name: SnapshotCfgName, Body: []byte("version: 1\n")}, {Name: MemoryDirtyName, Body: body}})
			if err != nil {
				t.Fatal(err)
			}
			source, err := tailzip.Append(dirtyTestMemory{size: 8192}, tail, tailzip.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = Open(context.Background(), dirtyTestStream{source}); err == nil {
				t.Fatal("invalid encoding accepted")
			}
		})
	}
	// A dirty page may span Data/Zero runs, but even a one-byte Hole is invalid.
	if err := ValidateDirty(dirtyTestMemory{size: 8192, split: 123}, good); err != nil {
		t.Fatal(err)
	}
	for _, hole := range []sparse.Extent{{Offset: 4095, Size: 1}, {Offset: 4096, Size: 1}, {Offset: 8191, Size: 1}} {
		if err := ValidateDirty(dirtyTestMemory{size: 8192, holes: []sparse.Extent{hole}}, good); err == nil {
			t.Fatalf("partial hole %v accepted", hole)
		}
	}
}

func TestReadConfigSkipsDirtyBodyButOpenChecksCRC(t *testing.T) {
	const size = uint64(8) << 30
	bitmap := make([]byte, (size/4096+7)/8)
	dirty, err := EncodeDirty(size, bitmap)
	if err != nil {
		t.Fatal(err)
	}
	tail, err := BuildZIPWithDirty([]byte("{}"), []byte("{}"), []byte("version: 1\n"), dirty)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt only bitmap payload; keep header and original CRC. The lightweight
	// reader must succeed without reading that body; the full reader must fail.
	bodyStart := uint64(localHeaderSize + len(ConfigJSONName) + 2 + localHeaderSize + len(StateJSONName) + 2 + localHeaderSize + len(SnapshotCfgName) + len("version: 1\n") + localHeaderSize + len(MemoryDirtyName))
	tail[bodyStart+16] ^= 1
	source, err := sparse.NewSource(dirtyArchiveReader{size: size, tail: tail}, size+uint64(len(tail)), nil)
	if err != nil {
		t.Fatal(err)
	}
	stream := &configReadStream{Source: source, forbiddenStart: size + bodyStart + 16, forbiddenEnd: size + bodyStart + uint64(len(dirty))}
	cfg, err := ReadConfig(context.Background(), stream)
	if err != nil || string(cfg) != "version: 1\n" {
		t.Fatalf("light read: %q %v", cfg, err)
	}
	if stream.readBytes > 1024 {
		t.Fatalf("ReadConfig read %d bytes", stream.readBytes)
	}
	if _, err := Open(context.Background(), dirtyTestStream{source}); err == nil || !strings.Contains(err.Error(), "CRC") {
		t.Fatalf("full reader CRC: %v", err)
	}
}

func TestReadConfigRejectsOversizeTailBeforeBodyRead(t *testing.T) {
	// Footer alone declares a syntactically bounded central directory but a
	// >64 MiB aggregate tail. Neither body nor huge allocation is necessary.
	const size = 4096 + MaxTailBytes + 1
	footer := make([]byte, 22)
	binary.LittleEndian.PutUint32(footer, eocdSignature)
	binary.LittleEndian.PutUint16(footer[8:], 3)
	binary.LittleEndian.PutUint16(footer[10:], 3)
	central := uint32(3*centralHeaderSize + len(ConfigJSONName) + len(StateJSONName) + len(SnapshotCfgName))
	binary.LittleEndian.PutUint32(footer[12:], central)
	binary.LittleEndian.PutUint32(footer[16:], uint32(MaxTailBytes+1-22)-central)
	src := &dirtyFooterStream{dirtyTestMemory: dirtyTestMemory{size: size}, footer: footer}
	if _, err := ReadConfig(context.Background(), src); err == nil || !strings.Contains(err.Error(), "tail exceeds") {
		t.Fatalf("ReadConfig tail budget: %v", err)
	}
}

type dirtyFooterStream struct {
	dirtyTestMemory
	footer []byte
}

func (s *dirtyFooterStream) ReadAt(_ context.Context, b []byte, off uint64) (int, error) {
	if off != s.size-22 || len(b) != 22 {
		return 0, io.ErrUnexpectedEOF
	}
	return copy(b, s.footer), nil
}

type dirtyArchiveReader struct {
	size uint64
	tail []byte
}

func (r dirtyArchiveReader) ReadAt(b []byte, off int64) (int, error) {
	if off < 0 {
		return 0, io.EOF
	}
	if uint64(off) < r.size {
		n := min(uint64(len(b)), r.size-uint64(off))
		clear(b[:n])
		if n == uint64(len(b)) {
			return len(b), nil
		}
		m, err := r.ReadAt(b[n:], off+int64(n))
		return int(n) + m, err
	}
	off -= int64(r.size)
	if off >= int64(len(r.tail)) {
		return 0, io.EOF
	}
	n := copy(b, r.tail[off:])
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}
