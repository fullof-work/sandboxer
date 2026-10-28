package memory

import (
	"bytes"
	"sync"
	"testing"
)

func TestDirtyMonotoneConcurrentAndOffsetConversion(t *testing.T) {
	d, err := NewDirty(193 * 4096)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range 128 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := d.Mark(uint64(i)*4096+4095, 2); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	before := d.Snapshot()
	for i := 0; i < 16; i++ {
		if before[i] != 255 {
			t.Fatal("lost concurrent bit")
		}
	}
	if err := d.MergeWords(129*4096, 64*4096, []uint64{1<<63 | 1}); err != nil {
		t.Fatal(err)
	}
	after := d.Snapshot()
	if after[16]&2 == 0 || after[24]&1 == 0 {
		t.Fatal("region-relative bitmap shifted incorrectly")
	}
	if err := d.MergeWords(129*4096, 64*4096, []uint64{0}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, d.Snapshot()) {
		t.Fatal("consumption/retry cleared cumulative bits")
	}
	for _, r := range [][2]uint64{{193 * 4096, 1}, {^uint64(0), 1}, {4096, ^uint64(0)}} {
		if err := d.Mark(r[0], r[1]); err == nil {
			t.Fatal("invalid range accepted")
		}
	}
	if err := d.MergeWords(0, 4096, []uint64{2}); err == nil {
		t.Fatal("unused bits accepted")
	}
}
