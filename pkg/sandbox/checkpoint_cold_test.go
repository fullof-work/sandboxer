package sandbox

import (
	"bytes"
	"context"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/sandboxer/internal/diffstore"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMaterializedColdGraphKeepsEffectiveOverridesAndRewrittenDependencies(t *testing.T) {
	hash := strings.Repeat("a", 64)
	old := "file://source.overlay@digest:" + hash
	relocated := "file://materialized.overlay@digest:" + hash
	ordinary := &config.PortableSandboxConfig{Version: 1, Resources: config.PortableResourcesConfig{Capacity: config.CapacityConfig{CPU: 2, Memory: "1GiB"}, Allocatable: config.AllocatableConfig{CPU: 1, Memory: "1GiB"}}, Boot: config.PortableBootConfig{Kernel: "file://vmlinux@digest:" + hash, Runtime: "file://runtime.bundle@digest:" + hash, Root: config.PortableRootConfig{Base: "self", BaseFromRefs: []string{old}}}, Launch: config.PortableLaunchConfig{Exec: "/bin/user-override", Workdir: "/", Restart: "never"}, Metadata: map[string]string{"caller": "override"}}
	effective, err := ordinary.Checkpointed("")
	if err != nil {
		t.Fatal(err)
	}
	materialized, err := effective.MaterializedCheckpoint(nil)
	if err != nil {
		t.Fatal(err)
	}
	materialized.Boot.Root.BaseFromRefs = []string{relocated}
	materialized.Launch.Exec = "/bin/original-artifact"
	runtime := &config.SandboxConfig{Boot: config.BootConfig{Kernel: "/host/kernel", Runtime: "/host/runtime", Root: config.RootConfig{Base: old, Diff: "file:///target/new.diff"}}, Launch: config.LaunchConfig{Exec: "/bin/user-override", EphemeralEnv: map[string]string{"PRIVATE": "yes"}}}
	got, err := materializedColdGraph(runtime, effective, materialized)
	if err != nil {
		t.Fatal(err)
	}
	if got.IsCheckpoint() || got.Boot.Root.Base != "self" || got.Boot.Root.BaseFromRefs[0] != relocated || got.Launch.Exec != "/bin/user-override" || got.Metadata["caller"] != "override" || got.Resources.Capacity.CPU != 2 {
		t.Fatalf("effective C0 changed: %+v", got)
	}
	if runtime.Boot.Root.BaseFromRefs[0] != relocated || runtime.Boot.Root.Diff != "file:///target/new.diff" || runtime.Boot.Kernel != "/host/kernel" || runtime.Launch.EphemeralEnv["PRIVATE"] != "yes" {
		t.Fatalf("runtime override lost: %+v", runtime)
	}
}

func TestNamedCaptureCollectsMaterializedBasesWithoutChangingIdentity(t *testing.T) {
	for _, bundle := range []bool{false, true} {
		t.Run(map[bool]string{false: "local", true: "bundle"}[bundle], func(t *testing.T) {
			ctx := context.Background()
			temporary := t.TempDir()
			sink := snapshot.NewFileSink(temporary, "source", nil, false, nil)
			lower, _, err := sink.AbsorbOverlaySource(ctx, sparse.Dense(bytes.NewReader(bytes.Repeat([]byte{0x11}, 8192)), 8192))
			if err != nil {
				t.Fatal(err)
			}
			portable := snapshotTestPortable(t)
			portable.Boot.Root = config.PortableRootConfig{Base: "self", BaseFromRefs: []string{lower}}
			runtime, err := config.MarshalPortableSandboxConfig(portable)
			if err != nil {
				t.Fatal(err)
			}
			src, err := sandboxfile.BuildSourceContext(ctx, sparse.Dense(bytes.NewReader(bytes.Repeat([]byte{0x22}, 8192)), 8192), nil, runtime)
			if err != nil {
				t.Fatal(err)
			}
			parent, parentPath, err := sink.AbsorbSandbox(ctx, src)
			if err != nil {
				t.Fatal(err)
			}
			cp, err := portable.Checkpointed(parent)
			if err != nil {
				t.Fatal(err)
			}
			before := artifact.DiffCheckpointBaseID(cp.Boot.Root)
			output := t.TempDir()
			opts := RunOptions{SandboxID: "capture", namedCheckpoint: true, checkpointDir: output, PortableConfig: cp, SourceBinding: &RunSourceBinding{SandboxRef: parent, RuntimeRef: "file://" + parentPath, RelativeDir: temporary}}
			plan, err := prepareSnapshotDependencyPlan(ctx, opts, nil, []bool{false}, output, store.WriteAdmission{}, bundle)
			if err != nil {
				t.Fatal(err)
			}
			replacements, err := plan.Emit(ctx, snapshot.NewFileSink(output, "captured", nil, false, nil), nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := plan.Close(); err != nil {
				t.Fatal(err)
			}
			after, err := cp.RewriteDiskArtifactRefs(replacements)
			if err != nil {
				t.Fatal(err)
			}
			if artifact.DiffCheckpointBaseID(after.Boot.Root) != before {
				t.Fatal("capture changed immutable base identity")
			}
			if err := os.RemoveAll(temporary); err != nil {
				t.Fatal(err)
			}
			for _, raw := range append([]string{after.Boot.Root.Base}, after.Boot.Root.BaseFromRefs...) {
				ref, err := manifest.ParseRef(raw)
				if err != nil {
					t.Fatal(err)
				}
				stream, err := artifact.OpenFile(ctx, filepath.Join(output, ref.Path), ref, nil, nil, nil, nil, false)
				if err != nil {
					t.Fatalf("base disappeared with temporary runner: %v", err)
				}
				payload, _, err := sandboxfile.PayloadIfSandbox(ctx, stream, false)
				if err != nil {
					t.Fatal(err)
				}
				b := make([]byte, 8192)
				if _, err := payload.ReadAt(ctx, b, 0); err != nil {
					t.Fatal(err)
				}
				if len(b) != 8192 || b[0] != 0x11 && b[0] != 0x22 {
					t.Fatal("wrong retained dependency")
				}
				if err := payload.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestColdEncryptedCheckpointWithoutManifestConfig(t *testing.T) {
	for _, cross := range []bool{false, true} {
		t.Run(map[bool]string{false: "same-directory", true: "cross-directory"}[cross], func(t *testing.T) {
			ctx := context.Background()
			base := t.TempDir()
			dir := filepath.Join(base, "path", "checkpoint")
			if err := os.MkdirAll(filepath.Join(dir, "diffs"), 0700); err != nil {
				t.Fatal(err)
			}
			key := [32]byte{0x39}
			codec, err := manifestcrypto.NewTarStreamCodec(key)
			if err != nil {
				t.Fatal(err)
			}
			portable, err := snapshotTestLivePortable(t).Checkpointed("")
			if err != nil {
				t.Fatal(err)
			}
			store, err := diffstore.Create(filepath.Join(dir, "diffs", "root.diff"), diffstore.Options{DiffID: "root", BaseID: artifact.DiffCheckpointBaseID(portable.Boot.Root), LogicalSize: 8192, Key: &key, RequireEncryption: true})
			if err != nil {
				t.Fatal(err)
			}
			want := bytes.Repeat([]byte{0x45}, 8192)
			if _, err := store.WriteAt(want, 0); err != nil {
				t.Fatal(err)
			}
			if err := store.Checkpoint(ctx, "cp"); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			cp := &sandboxfile.Checkpoint{Version: 1, CheckpointID: "cp", Root: sandboxfile.CheckpointDisk{DiffID: "root"}, Disks: []sandboxfile.CheckpointNamedDisk{}}
			source, err := sandboxfile.BuildCheckpointSource(ctx, portable, cp)
			if err != nil {
				t.Fatal(err)
			}
			ref, path, err := snapshot.NewFileSink(dir, "test", codec, true, nil).AbsorbSandbox(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			opts := RunOptions{Cfg: &config.SandboxConfig{}, PortableConfig: portable, BaseRoot: base, PathID: "path", SandboxID: "test", EnableCheckpoint: true, LocalCodec: codec, LocalRequired: true, CustomerKeyFn: func() ([32]byte, error) { return key, nil }, SourceBinding: &RunSourceBinding{SandboxRef: ref, RuntimeRef: "file://" + path, RelativeDir: dir, Checkpoint: cp}}
			if cross {
				opts.BaseRoot = t.TempDir()
			}
			opts, release, err := prepareColdCheckpoint(ctx, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			if opts.ManifestCfg != nil || opts.LocalCodec == nil || !opts.LocalRequired {
				t.Fatal("local policy changed")
			}
			if !cross {
				if opts.checkpointPlan.Disks[0].DiffID != "root" {
					t.Fatal("encrypted same-directory source did not reopen")
				}
				return
			}
			if opts.SourceBinding.Checkpoint != nil || opts.PortableConfig.IsCheckpoint() {
				t.Fatal("cross-directory source was not materialized")
			}
			materialRef, err := manifest.ParseRef(opts.SourceBinding.RuntimeRef)
			if err != nil {
				t.Fatal(err)
			}
			opened, err := artifact.OpenFile(ctx, materialRef.Path, materialRef, nil, opts.CustomerKeyFn, nil, codec, true)
			if err != nil {
				t.Fatal(err)
			}
			root, err := sandboxfile.Open(ctx, opened)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			got := make([]byte, len(want))
			if _, err := root.Payload.ReadAt(ctx, got, 0); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatal("encrypted materialized payload changed")
			}
			plain, err := artifact.OpenFile(ctx, materialRef.Path, materialRef, nil, nil, nil, nil, false)
			if err == nil {
				plain.Close()
				t.Fatal("materialization emitted plaintext despite explicit local policy")
			}
		})
	}
}
