package diffstore

import (
	"context"
	"fmt"
)

// CheckpointManager is an optional live-disk capability. Its operations retain
// Disk's admission, materialization and cache-drain boundaries. A caller must
// never substitute a separately opened physical Store for this manager.
type CheckpointManager interface {
	CheckpointInfo() (diffID, baseID string, enabled bool)
	CanResumeCheckpoint(CheckpointID) bool
	Checkpoint(context.Context, CheckpointID) error
	ListCheckpoints(context.Context) ([]CheckpointID, error)
	DeleteCheckpoint(context.Context, CheckpointID) error
	DeleteCheckpoints(context.Context, []CheckpointID) error
	OpenCheckpoint(context.Context, CheckpointID) (ExportView, error)
}

// Checkpoints reports the format's capability once; Single has no manager.
// The returned manager shares the Disk's lifetime and must not outlive it.
func (c *Disk) Checkpoints() (CheckpointManager, bool) {
	if c.versions == nil {
		return nil, false
	}
	return c, true
}

// OpenActiveView exports upper data including accepted cache writes. The caller
// must keep this Disk open and quiescent until the view closes. It is not a
// persistent checkpoint and never falls through to the immutable base.
func (c *Disk) OpenActiveView(ctx context.Context) (ExportView, error) {
	return c.Export(ctx, ActiveDiff)
}

// OpenCheckpoint opens a named immutable view with an independent reader pin.
// The view remains valid after this Disk closes or resumes writing. Close the
// view to release its pin; an empty ID cannot select an active view here.
func (c *Disk) OpenCheckpoint(ctx context.Context, id CheckpointID) (ExportView, error) {
	if id == ActiveDiff {
		return nil, fmt.Errorf("diffstore: empty checkpoint ID")
	}
	return c.Export(ctx, id)
}
