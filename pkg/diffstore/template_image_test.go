package diffstore

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore/internal/sectorcrypt"
	"golang.org/x/crypto/xts"
	"golang.org/x/sys/unix"
)

// Build the pre-existing v1 template envelope independently of the work-file
// writer. This fixture must not switch formats when active DIFF versions change.
func writeV1Template(t *testing.T, path string, key [32]byte, logical []byte, blocks []int) {
	t.Helper()
	mac := hmac.New(sha256.New, key[:])
	mac.Write([]byte("kuasar/diff/header-key/aes-gcm/v1\x00"))
	block, err := aes.NewCipher(mac.Sum(nil))
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	var header [4096]byte
	copy(header[:8], []byte{0x89, 'K', 'D', 'X', 'T', 'S', '1', '\n'})
	binary.BigEndian.PutUint16(header[8:10], 1)
	binary.BigEndian.PutUint16(header[10:12], 16)
	var plain [128]byte
	binary.BigEndian.PutUint64(plain[:8], uint64(len(logical)))
	binary.BigEndian.PutUint32(plain[8:12], 4096)
	binary.BigEndian.PutUint32(plain[12:16], 512)
	binary.BigEndian.PutUint32(plain[16:20], 4096)
	binary.BigEndian.PutUint32(plain[20:24], 64)
	for i := 24; i < 88; i++ {
		plain[i] = byte(i)
	}
	x, err := xts.NewCipher(aes.NewCipher, plain[24:88])
	if err != nil {
		t.Fatal(err)
	}
	sealed := gcm.Seal(nil, header[16:28], plain[:], append([]byte("kuasar/diff/header/v1\x00"), header[:16]...))
	copy(header[28:], sealed)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err = f.Truncate(4096 + int64(len(logical))); err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(header[:], 0); err != nil {
		t.Fatal(err)
	}
	for _, i := range blocks {
		data := append([]byte(nil), logical[i*4096:(i+1)*4096]...)
		sectorcrypt.Transform(x, data, uint64(i*8), true)
		if _, err = f.WriteAt(data, 4096+int64(i*4096)); err != nil {
			t.Fatal(err)
		}
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestProvisioningTemplateContractIndependentOfWorkFormat(t *testing.T) {
	ctx := context.Background()
	key := testDiffKey(91)
	for _, encryptedTemplate := range []bool{false, true} {
		for _, checkpoint := range []bool{false, true} {
			for _, encryptedTarget := range []bool{false, true} {
				if encryptedTemplate && !encryptedTarget {
					continue // The existing encryption option also encrypts fresh targets.
				}
				t.Run(fmt.Sprintf("templateEncrypted=%v/checkpoint=%v/targetEncrypted=%v", encryptedTemplate, checkpoint, encryptedTarget), func(t *testing.T) {
					dir := t.TempDir()
					path := filepath.Join(dir, "template.ext4")
					logical := make([]byte, 4*cowBlockSize)
					copy(logical[cowBlockSize:], bytes.Repeat([]byte{0x6d}, cowBlockSize))
					// Page 2 is allocated zero Data. Pages 0 and 3 are Holes.
					if encryptedTemplate {
						writeV1Template(t, path, key, logical, []int{1, 2})
					} else {
						writeSparseTemplate(t, path, logical, []sparse.Extent{{Offset: cowBlockSize, Size: 2 * cowBlockSize}})
					}
					before, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					var enc *diffEncryption
					if encryptedTemplate {
						enc = mustDiffEncryption(t, key)
					}
					src, err := openLazyDiffTemplate(path, enc)
					if err != nil {
						t.Fatal(err)
					}
					image := src.(*templateImage)
					flags, err := unix.FcntlInt(image.f.Fd(), unix.F_GETFL, 0)
					if err != nil || flags&unix.O_ACCMODE != unix.O_RDONLY {
						t.Fatalf("template flags %d: %v", flags, err)
					}
					for i, want := range []sparse.RunKind{sparse.Hole, sparse.Data, sparse.Data, sparse.Hole} {
						r, err := src.RunAt(uint64(i*cowBlockSize), cowBlockSize)
						if err != nil || r.Kind() != want {
							t.Fatalf("page %d: %v %v", i, r, err)
						}
					}
					got := make([]byte, len(logical)-26)
					if _, err = src.ReadAt(ctx, got, 13); err != nil || !bytes.Equal(got, logical[13:len(logical)-13]) {
						t.Fatal("partial template read", err)
					}
					src.Close()
					if _, err := openExistingDiffFile(path, enc, false, false); !errors.Is(err, ErrFormat) {
						t.Fatalf("template accepted as existing work file: %v", err)
					}
					var options []DiskOption
					if encryptedTarget || encryptedTemplate {
						options = append(options, WithDiffEncryption(key, encryptedTarget))
					}
					if checkpoint {
						options = append(options, WithCheckpoint("template-contract", "base"))
					}
					base := bytes.Repeat([]byte{0xa7}, len(logical))
					disk, err := OpenDisk(filepath.Join(dir, "active"), &fakeReader{data: base}, DiffInit{TemplatePath: path}, options...)
					if err != nil {
						t.Fatal(err)
					}
					defer disk.Close()
					got = make([]byte, len(logical))
					if _, err = disk.ReadAt(got, 0); err != nil {
						t.Fatal(err)
					}
					want := append([]byte(nil), base...)
					copy(want[cowBlockSize:3*cowBlockSize], logical[cowBlockSize:3*cowBlockSize])
					if !bytes.Equal(got, want) {
						t.Fatal("template Hole/Data lower-mask semantics changed")
					}
					if err = disk.Close(); err != nil {
						t.Fatal(err)
					}
					after, err := os.ReadFile(path)
					if err != nil || !bytes.Equal(before, after) {
						t.Fatal("immutable source changed", err)
					}
				})
			}
		}
	}
}

func TestEncryptedProvisioningTemplateAuthentication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "template")
	writeV1Template(t, path, testDiffKey(17), make([]byte, 2*cowBlockSize), []int{1})
	if _, err := openLazyDiffTemplate(path, nil); !errors.Is(err, ErrDiffEncryptionRequired) {
		t.Fatal(err)
	}
	if _, err := openLazyDiffTemplate(path, mustDiffEncryption(t, testDiffKey(18))); !errors.Is(err, ErrDiffAuthentication) {
		t.Fatal(err)
	}
}

func TestSingleTemplatePresencePublicationIsBatched(t *testing.T) {
	const size = 130 * cowBlockSize
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%v", encrypted), func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			input := filepath.Join(dir, "template.ext4")
			want := bytes.Repeat([]byte{0x53}, size)
			writeSparseTemplate(t, input, want, []sparse.Extent{{Offset: 0, Size: size}})
			source, err := openLazyDiffTemplate(input, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			var options []DiskOption
			if encrypted {
				options = append(options, WithDiffEncryption(testDiffKey(81), true))
			}
			path := filepath.Join(dir, "active")
			disk, err := OpenDisk(path, nil, DiffInit{CreateSize: size}, options...)
			if err != nil {
				t.Fatal(err)
			}
			defer disk.Close()
			// Foreground pages split the first template batch into disjoint runs.
			foreground := bytes.Repeat([]byte{0xa1}, cowBlockSize)
			for _, block := range []int{1, 63} {
				if _, err := disk.WriteAt(foreground, int64(block*cowBlockSize)); err != nil {
					t.Fatal(err)
				}
				copy(want[block*cowBlockSize:], foreground)
			}
			if err := disk.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			backend := disk.diff.(*diffFile)
			writes := 0
			backend.beforePresenceWrite = func(int) error { writes++; return nil }
			staging := make([]byte, disk.diff.policy().materialPages*cowBlockSize)
			if err := disk.materializeRange(ctx, source, 0, size, staging); err != nil {
				t.Fatal(err)
			}
			if writes != 3 {
				t.Fatalf("130 pages with foreground gaps published presence %d times, want 3 bounded batches", writes)
			}
			if err := disk.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenDisk(path, nil, DiffInit{Existing: true}, options...)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			got := make([]byte, size)
			if _, err := reopened.ReadAt(got, 0); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("reopened template/foreground bytes differ: %v", err)
			}
		})
	}
}
