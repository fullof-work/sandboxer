// Package sectorcrypt shares only the payload transform. Version-specific
// headers, authentication domains and key wrapping stay in their codecs.
package sectorcrypt

import "golang.org/x/crypto/xts"

const UnitSize = 512

// Transform uses logical sectors, never physical B+ tree slot addresses. Data
// must contain complete sectors, and callers exclusively own its writable bytes.
func Transform(cipher *xts.Cipher, data []byte, first uint64, encrypt bool) {
	if cipher == nil {
		return
	}
	for off := 0; off < len(data); off += UnitSize {
		sector := data[off : off+UnitSize]
		number := first + uint64(off/UnitSize)
		if encrypt {
			cipher.Encrypt(sector, sector, number)
		} else {
			cipher.Decrypt(sector, sector, number)
		}
	}
}
