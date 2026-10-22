package sandbox

import (
	"context"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"github.com/kuasar-sandbox/sandboxer/pkg/vhost"
)

func bindCheckpointDisks(refs []SnapDiskRef, disks []DiskBackend) []SnapDiskRef {
	for i := range refs {
		if i >= len(disks) || disks[i].Cow == nil {
			continue
		}
		cow := disks[i].Cow
		refs[i].Export = func(ctx context.Context) (snapshot.ExportView, error) { return cow.Export(ctx, vhost.ActiveDiff) }
		refs[i].WaitMaterialized = cow.WaitMaterialized
		id, _, enabled := cow.CheckpointInfo()
		if !enabled {
			continue
		}
		refs[i].DiffID = id
		refs[i].Checkpoint = func(ctx context.Context, id string) error { return cow.Checkpoint(ctx, vhost.CheckpointID(id)) }
		refs[i].DeleteCheckpoint = func(ctx context.Context, id string) error { return cow.DeleteCheckpoint(ctx, vhost.CheckpointID(id)) }
		refs[i].DeleteCheckpoints = func(ctx context.Context, ids []string) error {
			versions := make([]vhost.CheckpointID, len(ids))
			for j, id := range ids {
				versions[j] = vhost.CheckpointID(id)
			}
			return cow.DeleteCheckpoints(ctx, versions)
		}
		refs[i].ListCheckpoints = func(ctx context.Context) ([]string, error) {
			ids, err := cow.ListCheckpoints(ctx)
			if err != nil {
				return nil, err
			}
			out := make([]string, len(ids))
			for j, id := range ids {
				out[j] = string(id)
			}
			return out, nil
		}
	}
	return refs
}
