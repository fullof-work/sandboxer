package sandbox

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kuasar-sandbox/sandboxer/internal/diffstore"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
)

type CheckpointDiskBinding struct{ DiffID, BaseID, Path string }
type CheckpointDiskPlan struct {
	Directory string
	Disks     []CheckpointDiskBinding
}

// PrepareCheckpointDiskPlan fixes the paths before capacity and ext4 preflight.
// bases may be bound after cold C0 projection, without changing any paths.
// reopen is populated only after pinning and checking every source version.
func PrepareCheckpointDiskPlan(cfg *config.SandboxConfig, bases *config.PortableSandboxConfig, directory string, reopen []string) (*CheckpointDiskPlan, error) {
	directory, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	roots := []*config.RootConfig{&cfg.Boot.Root}
	for i := range cfg.Boot.Disks {
		roots = append(roots, &cfg.Boot.Disks[i].RootConfig)
	}
	if len(reopen) != 0 && len(reopen) != len(roots) {
		return nil, errors.New("checkpoint reopen topology mismatch")
	}
	plan := &CheckpointDiskPlan{Directory: directory, Disks: make([]CheckpointDiskBinding, len(roots))}
	for i, root := range roots {
		target := &root.Diff
		if root.Overlay != nil {
			target = &root.Overlay.Diff
		}
		if *target != "" {
			scheme, path, ok := config.SchemeAndPath(*target)
			if !ok || scheme != "file" {
				return nil, errors.New("checkpoint requires a local writable DIFF")
			}
			abs, err := filepath.Abs(path)
			if err != nil {
				return nil, err
			}
			if filepath.Dir(abs) != filepath.Join(directory, "diffs") {
				return nil, fmt.Errorf("checkpoint writable DIFF %q is outside its derived directory", path)
			}
			if len(reopen) == 0 {
				if _, err := os.Lstat(abs); err == nil {
					return nil, errors.New("checkpoint refuses implicit writable DIFF takeover; import it as a template")
				} else if !os.IsNotExist(err) {
					return nil, err
				}
			}
		}
		id := ""
		if len(reopen) > 0 {
			id = reopen[i]
		} else {
			var b [16]byte
			if _, err := rand.Read(b[:]); err != nil {
				return nil, err
			}
			id = hex.EncodeToString(b[:])
		}
		if id == "" || strings.ContainsAny(id, "/\\.") {
			return nil, errors.New("invalid checkpoint DiffID")
		}
		path := filepath.Join(directory, "diffs", id+".diff")
		plan.Disks[i] = CheckpointDiskBinding{DiffID: id, Path: path}
		*target = "file://" + path
	}
	if bases != nil {
		if err := plan.Bind(bases); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func (p *CheckpointDiskPlan) Bind(bases *config.PortableSandboxConfig) error {
	if bases == nil || !bases.IsCheckpoint() || len(p.Disks) != len(bases.Boot.Disks)+1 {
		return errors.New("checkpoint base graph mismatch")
	}
	p.Disks[0].BaseID = artifact.DiffCheckpointBaseID(bases.Boot.Root)
	for i, d := range bases.Boot.Disks {
		p.Disks[i+1].BaseID = artifact.DiffCheckpointBaseID(d.PortableRootConfig)
	}
	return nil
}

func prepareNamedCapture(req ctl.Request, opts RunOptions, disks []SnapDiskRef) (ctl.Request, error) {
	if req.CheckpointID == "" {
		if req.Type == ctl.TypeCheckpointSnapshotRequest || req.Type == ctl.TypeCheckpointExportRequest {
			return req, errors.New("checkpoint capture requires a nonempty ID")
		}
		return req, nil
	}
	if !opts.EnableCheckpoint {
		return req, ctl.ErrCheckpointUnsupported
	}
	if err := sandboxfile.ValidateCheckpointID(req.CheckpointID); err != nil {
		return req, err
	}
	if req.Upload {
		return req, errors.New("checkpoint cannot be combined with upload")
	}
	for _, d := range disks {
		if d.DiffID == "" || d.Checkpoint == nil {
			return req, ctl.ErrCheckpointUnsupported
		}
	}
	want, err := filepath.Abs(opts.checkpointDir)
	if err != nil {
		return req, err
	}
	if req.OutDir == "" {
		req.OutDir = want
		return req, nil
	}
	got, err := filepath.Abs(req.OutDir)
	if err != nil {
		return req, err
	}
	if got != want {
		return req, errors.New("checkpoint output must be the owner's derived CheckpointDir")
	}
	req.OutDir = want
	return req, nil
}

func (h *SnapshotHandler) listDiffCheckpoints(ctx context.Context) ([]artifact.DiffCheckpointRef, error) {
	if !h.EnableCheckpoint {
		return nil, ctl.ErrCheckpointUnsupported
	}
	var refs []artifact.DiffCheckpointRef
	entries, err := os.ReadDir(filepath.Join(h.CheckpointDir, "diffs"))
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".diff") {
			continue
		}
		id := strings.TrimSuffix(e.Name(), ".diff")
		live := false
		for _, d := range h.Disks {
			if d.DiffID == id {
				ids, err := d.ListCheckpoints(ctx)
				if err != nil {
					return nil, err
				}
				for _, cp := range ids {
					refs = append(refs, artifact.DiffCheckpointRef{DiffID: id, ID: cp})
				}
				live = true
				break
			}
		}
		if live {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, errors.New("checkpoint DIFF is not a regular file")
		}
		path := filepath.Join(h.CheckpointDir, "diffs", e.Name())
		meta, err := diffstore.OpenMetadata(path)
		if err != nil {
			return nil, err
		}
		if meta.Info().DiffID != id {
			return nil, errors.Join(errors.New("checkpoint DIFF filename/header identity mismatch"), meta.Close())
		}
		ids, err := meta.ListCheckpoints(ctx)
		closeErr := meta.Close()
		if err = errors.Join(err, closeErr); err != nil {
			return nil, err
		}
		for _, id := range ids {
			refs = append(refs, artifact.DiffCheckpointRef{DiffID: strings.TrimSuffix(e.Name(), ".diff"), ID: id})
		}
	}
	return refs, nil
}

func (h *SnapshotHandler) deleteDiffCheckpoints(ctx context.Context, refs []ctl.DiffCheckpointRef) error {
	if !h.EnableCheckpoint {
		return ctl.ErrCheckpointUnsupported
	}
	if err := h.capture.begin(); err != nil {
		return err
	}
	defer h.capture.finish(false)
	groups := map[string][]string{}
	for _, r := range refs {
		if r.DiffID == "" || strings.ContainsAny(r.DiffID, "/\\.") {
			return errors.New("invalid DiffID")
		}
		if err := sandboxfile.ValidateCheckpointID(r.ID); err != nil {
			return err
		}
		groups[r.DiffID] = append(groups[r.DiffID], r.ID)
	}
	for id, ids := range groups {
		live := false
		for _, d := range h.Disks {
			if d.DiffID == id {
				if d.DeleteCheckpoints == nil {
					return ctl.ErrCheckpointUnsupported
				}
				if err := d.DeleteCheckpoints(ctx, ids); err != nil {
					return err
				}
				live = true
				break
			}
		}
		if live {
			continue
		}
		meta, err := diffstore.OpenMetadata(filepath.Join(h.CheckpointDir, "diffs", id+".diff"))
		if err != nil {
			return err
		}
		if meta.Info().DiffID != id {
			return errors.Join(errors.New("checkpoint DIFF filename/header identity mismatch"), meta.Close())
		}
		err = meta.DeleteCheckpoints(ctx, ids)
		if err = errors.Join(err, meta.Close()); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) ListCheckpoints(ctx context.Context) ([]artifact.DiffCheckpointRef, error) {
	if err := r.beginOperation(); err != nil {
		return nil, err
	}
	defer r.opsWG.Done()
	r.mu.Lock()
	fn := r.listCheckpoints
	r.mu.Unlock()
	if fn == nil {
		return nil, ctl.ErrCheckpointUnsupported
	}
	return fn(ctx)
}
func (r *Runtime) DeleteCheckpoint(ctx context.Context, ref artifact.DiffCheckpointRef) error {
	if err := r.beginOperation(); err != nil {
		return err
	}
	defer r.opsWG.Done()
	r.mu.Lock()
	fn := r.deleteCheckpoint
	r.mu.Unlock()
	if fn == nil {
		return ctl.ErrCheckpointUnsupported
	}
	return fn(ctx, ref)
}

func validateNamedCaptureGraph(id string, p *config.PortableSandboxConfig, parent string, merged []bool) error {
	if id != "" {
		return p.Validate()
	}
	return snapshot.ValidateExportGraph(p, parent, merged)
}
