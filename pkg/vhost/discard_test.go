package vhost

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
)

type discardRecordingBackend struct {
	*memBackend
	off, length int64
	calls       int
	failure     error
	supported   bool
}

func (b *discardRecordingBackend) Discard(off, n int64) error {
	b.calls++
	b.off = off
	b.length = n
	return b.failure
}
func (b *discardRecordingBackend) SupportsDiscard() bool { return b.supported }

func TestDiscardRangeValidation(t *testing.T) {
	for _, tc := range []struct {
		name         string
		sector       uint64
		count, flags uint32
		size         int
		status       byte
	}{
		{"whole", 0, 8, 0, 16, BlkStatusOK},
		{"unaligned-inward", 1, 15, 0, 16, BlkStatusOK},
		{"limit", 0, 8192, 0, 16, BlkStatusOK},
		{"too-large", 0, 8193, 0, 16, BlkStatusIOErr},
		{"overflow", ^uint64(0), 8, 0, 16, BlkStatusIOErr},
		{"out-of-bounds-before-rounding", 8191, 2, 0, 16, BlkStatusIOErr},
		{"empty-at-end", 8192, 0, 0, 16, BlkStatusOK},
		{"flags", 0, 8, 1, 16, BlkStatusUnsupp},
		{"short", 0, 8, 0, 15, BlkStatusIOErr},
		{"extra-byte", 0, 8, 0, 17, BlkStatusIOErr},
		{"multiple-ranges", 0, 8, 0, 32, BlkStatusIOErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for split := 0; split <= tc.size; split++ {
				b := &discardRecordingBackend{memBackend: &memBackend{data: make([]byte, 4<<20)}, supported: true}
				p := make([]byte, max(16, tc.size))
				binary.LittleEndian.PutUint64(p, tc.sector)
				binary.LittleEndian.PutUint32(p[8:], tc.count)
				binary.LittleEndian.PutUint32(p[12:], tc.flags)
				c := &chain{readSegs: [][]byte{make([]byte, 16), p[:split], p[split:tc.size]}, writeSegs: [][]byte{{0xff}}}
				if got := discardChain(b, c); got != tc.status {
					t.Fatalf("split %d: status=%d want=%d", split, got, tc.status)
				}
				if tc.status == BlkStatusOK {
					if b.calls != 1 || b.off != int64(tc.sector)*512 || b.length != int64(tc.count)*512 {
						t.Fatal("range changed before validation", b)
					}
				} else if b.calls != 0 {
					t.Fatal("invalid range reached backend")
				}
			}
		})
	}
	bad := &discardRecordingBackend{memBackend: &memBackend{data: make([]byte, 4096)}, supported: true, failure: errors.New("I/O")}
	p := make([]byte, 16)
	binary.LittleEndian.PutUint32(p[8:], 8)
	if status := discardChain(bad, &chain{readSegs: [][]byte{append(make([]byte, 16), p...)}, writeSegs: [][]byte{{0}}}); status != BlkStatusIOErr {
		t.Fatal(status)
	}
}

func discardWireQueue(b Backend, negotiated bool, split int) (*Server, *virtq, []byte) {
	s := NewServer("unused", b, nil)
	if negotiated {
		s.features = bitVirtioBlkDiscard
	}
	m := make([]byte, 2048)
	const uva = 0x1000
	s.SetMemoryLoader(initializedTestMemory)
	s.memTable.SetRegions([]MemRegion{{GuestPhysAddr: 0, UserspaceAddr: uva, MemorySize: uint64(len(m)), mmapBytes: m}})
	q := &virtq{num: 8, descAddr: uva, usedAddr: uva + 128, kickFd: -1, callFd: -1}
	put := func(i int, addr uint64, n int, flags, next uint16) {
		d := m[i*16 : (i+1)*16]
		binary.LittleEndian.PutUint64(d, addr)
		binary.LittleEndian.PutUint32(d[8:], uint32(n))
		binary.LittleEndian.PutUint16(d[12:], flags)
		binary.LittleEndian.PutUint16(d[14:], next)
	}
	binary.LittleEndian.PutUint32(m[256:], BlkTypeDiscard)
	binary.LittleEndian.PutUint64(m[512:], 1)
	binary.LittleEndian.PutUint32(m[520:], 22) // [512,11776): only page 1 is complete
	put(0, 256, 16, descFlagNext, 1)
	put(1, 512, split, descFlagNext, 2)
	put(2, 512+uint64(split), 16-split, descFlagNext, 3)
	put(3, 1536, 1, descFlagWrite, 0)
	m[1536] = 0xff
	return s, q, m
}

func TestGuestDiscardMasksBaseAndPreservesEdges(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		for _, encrypted := range []bool{false, true} {
			t.Run(fmt.Sprintf("checkpoint=%t/encrypted=%t", checkpoint, encrypted), func(t *testing.T) {
				base := &memBackend{data: bytes.Repeat([]byte{0x5c}, 3*4096)}
				var opts []diffstore.DiskOption
				if checkpoint {
					opts = append(opts, diffstore.WithCheckpoint("discard-wire", ""))
				}
				if encrypted {
					opts = append(opts, diffstore.WithDiffEncryption([32]byte{7}, true))
				}
				d, err := diffstore.OpenDisk(filepath.Join(t.TempDir(), "d"), base, diffstore.DiffInit{CreateSize: 3 * 4096}, opts...)
				if err != nil {
					t.Fatal(err)
				}
				defer d.Close()
				backend := &CowBackend{C: d}
				for _, negotiated := range []bool{false, true} {
					s, q, m := discardWireQueue(backend, negotiated, 7)
					n, err := s.processChain(q, 0)
					want := BlkStatusUnsupp
					if negotiated {
						want = BlkStatusOK
					}
					if err != nil || n != 1 || m[1536] != want {
						t.Fatal("wire completion", n, err, m[1536])
					}
					got := make([]byte, 3*4096)
					if _, err := d.ReadAt(got, 0); err != nil {
						t.Fatal(err)
					}
					expected := append([]byte(nil), base.data...)
					if negotiated {
						clear(expected[4096:8192])
					}
					if !bytes.Equal(got, expected) {
						t.Fatal("DISCARD leaked base or changed partial edge")
					}
				}
			})
		}
	}
}

func TestPooledDiscardCapabilityAndCapacity(t *testing.T) {
	set := bindingSetForTest(t, BlockDeviceSpec{Capacity: 4096})
	s := NewServer("unused", set.DeviceBackends()[0], nil)
	before := s.advertisedFeatures()
	bad := &discardRecordingBackend{memBackend: &memBackend{data: make([]byte, 4096)}}
	if err := set.Bind([]Backend{bad}); err == nil {
		t.Fatal("bound backend without DISCARD")
	}
	bad.supported = true
	if err := set.Bind([]Backend{bad}); err != nil {
		t.Fatal(err)
	}
	if before != s.advertisedFeatures() || before&bitVirtioBlkDiscard == 0 {
		t.Fatal("binding changed fixed capability")
	}
	for _, size := range []int64{512, 4097, diffstore.MaxWritableSize + 4096} {
		if _, err := NewBackendSet([]BlockDeviceSpec{{Capacity: size}}); err == nil {
			t.Fatal("invalid writable capacity", size)
		}
	}
	if _, err := NewBackendSet([]BlockDeviceSpec{{Capacity: diffstore.MaxWritableSize + 512, ReadOnly: true}}); err != nil {
		t.Fatal("DIFF bound applied to readonly", err)
	}
}
