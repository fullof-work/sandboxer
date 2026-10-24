package diffstore

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/internal/readretry"
)

// Filesystem preflight is a composition helper above the storage backends.
// Neither Single nor Versioned interprets filesystem bytes.
// ValidateExistingDiffExt4 verifies the effective filesystem seen through an
// existing active diff and its optional immutable base. It opens the diff
// read-only, applies the same local-encryption policy as OpenDisk, and
// checks the ext4 superblock magic in the combined view without mutating the
// diff or inventing sparse data.
func ValidateExistingDiffExt4(ctx context.Context, path string, base BlockReader, rawOptions ...DiskOption) error {
	options, err := parseDiskOptions(rawOptions)
	if err != nil {
		return err
	}
	if options.checkpoint != nil {
		source, err := openCheckpointPreflight(ctx, path, options)
		if err != nil {
			return err
		}
		return validateDiffSourceExt4(ctx, source, base)
	}
	diff, err := openExistingDiffFile(path, options.encryption, options.required, true)
	if err != nil {
		return err
	}
	if err := diff.enableDirect(); err != nil {
		return errors.Join(err, diff.Close())
	}
	return validateDiffSourceExt4(ctx, &fileDiffTemplate{diff: diff}, base)
}

// ValidateDiffTemplateExt4 verifies the effective filesystem produced by a
// provisioning template over an optional immutable base. Templates retain the
// existing policy that plaintext is accepted even when newly-created active
// diffs must be encrypted.
func ValidateDiffTemplateExt4(ctx context.Context, path string, base BlockReader, rawOptions ...DiskOption) error {
	options, err := parseDiskOptions(rawOptions)
	if err != nil {
		return err
	}
	source, err := openLazyDiffTemplate(path, options.encryption)
	if err != nil {
		return err
	}
	return validateDiffSourceExt4(ctx, source, base)
}

func validateDiffSourceExt4(ctx context.Context, source diffTemplateSource, base BlockReader) error {
	if ctx == nil {
		ctx = context.Background()
	}
	validationErr := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if source.Size() < uint64(ext4MagicOffset+2) {
			return fmt.Errorf("diffstore: ext4 source is too small")
		}
		if base != nil && base.Size() > int64(source.Size()) {
			return fmt.Errorf("diffstore: base size %d > diff size %d", base.Size(), source.Size())
		}
		var magic [2]byte
		run, err := source.RunAt(uint64(ext4MagicOffset), uint64(len(magic)))
		if err != nil {
			return fmt.Errorf("diffstore: inspect ext4 superblock run: %w", err)
		}
		switch run.Kind() {
		case sparse.Zero:
			// An explicit zero masks even a valid ext4 superblock in the base.
		case sparse.Hole:
			if base != nil && base.Size() >= ext4MagicOffset+int64(len(magic)) {
				n, readErr := base.ReadAt(magic[:], ext4MagicOffset)
				if readErr != nil && (readretry.IsTerminal(readErr) || readerr.IsPermanent(readErr) || !(errors.Is(readErr, io.EOF) && n == len(magic))) {
					return fmt.Errorf("diffstore: read ext4 magic from base: %w", readErr)
				}
				if n != len(magic) {
					return io.ErrUnexpectedEOF
				}
			}
		default:
			n, readErr := source.ReadAt(ctx, magic[:], uint64(ext4MagicOffset))
			if readErr != nil && (readretry.IsTerminal(readErr) || readerr.IsPermanent(readErr) || !(errors.Is(readErr, io.EOF) && n == len(magic))) {
				return fmt.Errorf("diffstore: read ext4 magic from diff: %w", readErr)
			}
			if n != len(magic) {
				return io.ErrUnexpectedEOF
			}
		}
		if magic != [2]byte{0x53, 0xef} {
			return fmt.Errorf("diffstore: effective writable disk is not a formatted ext4 filesystem")
		}
		return nil
	}()
	return errors.Join(validationErr, source.Close())
}
