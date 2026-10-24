package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
	"github.com/kuasar-sandbox/sandboxer/pkg/util"
	"golang.org/x/sys/unix"
)

func diffCmd(args []string) int {
	if len(args) == 0 || args[0] != "create" {
		fmt.Fprintln(os.Stderr, "usage: sandbox-ctl diff create --output NEW.diff --size SIZE [--filesystem ext4|none] [--directory DIR] [--journal]")
		return 2
	}
	fs := flag.NewFlagSet("diff create", flag.ContinueOnError)
	output := fs.String("output", "", "new plaintext DIFF or provisioning template; existing paths are never replaced")
	sizeText := fs.String("size", "", "positive 4 KiB aligned logical capacity, at most 32 GiB")
	filesystem := fs.String("filesystem", "ext4", "ext4, or none for an all-Hole upper over an existing filesystem")
	directory := fs.String("directory", "", "directory used to populate a new ext4 filesystem")
	journal := fs.Bool("journal", false, "enable the ext4 journal (default disabled)")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	size, err := util.ParseSize(*sizeText)
	if err != nil || *output == "" || fs.NArg() != 0 || (*filesystem != "ext4" && *filesystem != "none") || (*filesystem == "none" && (*directory != "" || *journal)) {
		fmt.Fprintln(os.Stderr, "diff create: provide --output and --size; directory/journal require ext4")
		return 2
	}
	if err := diffstore.ValidateWritableSize(int64(size)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	ctx, cancel := commandContext()
	defer cancel()
	if err := createProvisioningDiff(ctx, *output, int64(size), *filesystem, *directory, *journal); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Fprintln(os.Stdout, *output)
	return 0
}

// This creates a new filesystem, not an importer for old raw/v1/v2 work files.
// mkfs sees only a private, newly-created temporary raw image. The result is
// copied through the current storage API and published without replacement.
func createProvisioningDiff(ctx context.Context, output string, size int64, filesystem, directory string, journal bool) (retErr error) {
	if err := diffstore.ValidateWritableSize(size); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := os.Lstat(output); err == nil {
		return fmt.Errorf("diff create: output already exists: %s", output)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(output)
	stage, err := os.MkdirTemp(parent, ".diff-create-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	diskPath := filepath.Join(stage, "disk.diff")
	disk, err := diffstore.OpenDisk(diskPath, nil, diffstore.DiffInit{CreateSize: size})
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			retErr = errors.Join(retErr, disk.Close())
		}
	}()
	if filesystem == "ext4" {
		rawPath := filepath.Join(stage, "filesystem.raw")
		raw, err := os.OpenFile(rawPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		defer raw.Close()
		if err := raw.Truncate(size); err != nil {
			return err
		}
		features := "^has_journal"
		if journal {
			features = "has_journal"
		}
		args := []string{"-q", "-F", "-b", "4096", "-O", features, "-E", "lazy_itable_init=0,lazy_journal_init=0"}
		if directory != "" {
			args = append(args, "-d", directory)
		}
		args = append(args, rawPath)
		command := exec.CommandContext(ctx, "mkfs.ext4", args...)
		if out, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("diff create: mkfs.ext4: %w: %s", err, out)
		}
		if err := copyProvisionedPages(ctx, disk, raw, size); err != nil {
			return err
		}
	} else if filesystem != "none" {
		return fmt.Errorf("diff create: unknown filesystem %q", filesystem)
	}
	if err := disk.Close(); err != nil {
		closed = true
		return err
	}
	closed = true
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := unix.Renameat2(unix.AT_FDCWD, diskPath, unix.AT_FDCWD, output, unix.RENAME_NOREPLACE); err != nil {
		return err
	}
	dir, err := os.Open(parent)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func copyProvisionedPages(ctx context.Context, disk *diffstore.Disk, raw *os.File, size int64) error {
	buf := make([]byte, 256<<10)
	defer clear(buf)
	for pos := int64(0); pos < size; {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := unix.Seek(int(raw.Fd()), pos, unix.SEEK_DATA)
		if errors.Is(err, unix.ENXIO) {
			return nil
		}
		if err != nil {
			return err
		}
		hole, err := unix.Seek(int(raw.Fd()), data, unix.SEEK_HOLE)
		if err != nil {
			return err
		}
		end := min(size, (hole+4095)/4096*4096)
		if end <= pos || data >= size {
			return fmt.Errorf("diff create: invalid generated filesystem extent")
		}
		for off := data / 4096 * 4096; off < end; {
			if err := ctx.Err(); err != nil {
				return err
			}
			n := min(int64(len(buf)), end-off)
			if _, err := raw.ReadAt(buf[:n], off); err != nil {
				return err
			}
			written, err := disk.WriteAt(buf[:n], off)
			if err != nil {
				return err
			}
			if int64(written) != n {
				return io.ErrShortWrite
			}
			off += n
		}
		pos = end
	}
	return nil
}
