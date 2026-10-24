package vhost

import "encoding/binary"

const maxDiscardSectors = 8192 // 4 MiB, one range per request

// The single 16-byte range is a byte stream, not one descriptor. Validate its
// original sector bounds before Disk rounds inward to complete 4 KiB pages.
func discardChain(backend Backend, c *chain) byte {
	var rangeBytes [16]byte
	n := 0
	for i, seg := range c.readSegs {
		if i == 0 {
			seg = seg[16:]
		}
		if len(seg) > len(rangeBytes)-n {
			return BlkStatusIOErr
		}
		n += copy(rangeBytes[n:], seg)
	}
	if n != len(rangeBytes) || len(c.writeSegs) != 1 || len(c.writeSegs[0]) != 1 {
		return BlkStatusIOErr
	}
	sector := binary.LittleEndian.Uint64(rangeBytes[:8])
	count := binary.LittleEndian.Uint32(rangeBytes[8:12])
	flags := binary.LittleEndian.Uint32(rangeBytes[12:])
	if flags != 0 {
		return BlkStatusUnsupp
	}
	if count > maxDiscardSectors || backend.Size() < 0 {
		return BlkStatusIOErr
	}
	capacity := uint64(backend.Size() / SectorSize)
	if sector > capacity || uint64(count) > capacity-sector {
		return BlkStatusIOErr
	}
	if err := backend.Discard(int64(sector)*SectorSize, int64(count)*SectorSize); err != nil {
		return BlkStatusIOErr
	}
	return BlkStatusOK
}
