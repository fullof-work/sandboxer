package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/guestlink"
	"github.com/kuasar-sandbox/sandboxer/pkg/proto"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
)

// Opt-in real Guest test. No child-process substitute is used here. The config
// must use prepared local artifacts and launch.placeholder=true; its TAP, if
// any, must be private to this validation run and already configured.
func TestKVMRuntimeLaunch(t *testing.T) {
	path := os.Getenv("SANDBOX_SDK_KVM_CONFIG")
	if path == "" {
		t.Skip("real KVM: set SANDBOX_SDK_KVM_CONFIG and SANDBOX_SDK_KVM_CH")
	}
	if _, err := os.Stat("/dev/kvm"); err != nil {
		t.Fatalf("real KVM prerequisite: %v", err)
	}
	ch := os.Getenv("SANDBOX_SDK_KVM_CH")
	if ch == "" {
		t.Fatal("SANDBOX_SDK_KVM_CH must name the prepared patched Cloud Hypervisor")
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Launch.Placeholder {
		t.Fatal("KVM SDK fixture requires launch.placeholder=true so it stays alive until Close")
	}
	storage, err := artifact.NewProcessStorageWithCustomerKey(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	shape, err := sandbox.DeriveRuntimeSpec(ctx, cfg, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp("/tmp", "sdk-kvm-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	shape.SandboxID = "sdk-kvm"
	shape.CHBinary = ch
	shape.RuntimeRoot = filepath.Join(root, "run")
	shape.BaseRoot = filepath.Join(root, "base")
	boot, cancelBoot := context.WithCancel(ctx)
	owner, err := StartRuntime(boot, shape)
	cancelBoot()
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if owner.State() != "runtime_ready" {
		t.Fatal(owner.State())
	}
	host := &guestlink.HostClient{BasePath: filepath.Join(shape.RuntimeRoot, shape.SandboxID, "vsock.sock")}
	for range 2 {
		reply, err := host.RoundTripContext(ctx, &proto.Message{Type: proto.TypePing, ID: 17}, 5*time.Second)
		if err != nil || reply.Type != proto.TypePong {
			t.Fatalf("base Guest ping=%+v/%v", reply, err)
		}
		if reply.InitialLaunchState != nil {
			t.Fatalf("base runtime fabricated initial launch: %+v", reply.InitialLaunchState)
		}
		if _, err := owner.Stats(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if _, _, err := owner.Exec(ctx, proto.ExecSpec{Argv: []string{"/bin/true"}}); err == nil {
		t.Fatal("base runtime accepted workload exec")
	}
	// Prewarm memory capture/restore is not a supported SDK lifecycle.
	// These operations must reject without consuming the later live Launch.
	for name, capture := range map[string]func(context.Context, ctl.Request) (ctl.Response, error){
		"snapshot": owner.Snapshot, "export": owner.Export,
	} {
		if _, err := capture(ctx, ctl.Request{}); err == nil || !strings.Contains(err.Error(), "runtime_ready") {
			t.Fatalf("base %s=%v", name, err)
		}
	}
	for _, kind := range []string{proto.TypeRestore, proto.TypeAttach, proto.TypeQuiesce} {
		reply, err := host.RoundTripContext(ctx, &proto.Message{Type: kind, ID: 18}, 5*time.Second)
		if err != nil || reply.Type != proto.TypeError || !strings.Contains(reply.Msg, "workload not launched") {
			t.Fatalf("base %s=%+v/%v", kind, reply, err)
		}
	}
	if owner.State() != "runtime_ready" {
		t.Fatal(owner.State())
	}
	launch := sandbox.LaunchSpecFromConfig(cfg)
	launch.Storage = storage
	op, cancelLaunch := context.WithCancel(ctx)
	err = owner.Launch(op, launch)
	cancelLaunch()
	if err != nil {
		t.Fatal(err)
	}
	if owner.State() != "running" {
		t.Fatal(owner.State())
	}
	if _, err := owner.Stats(ctx); err != nil {
		t.Fatal(err)
	}
	reply, err := host.RoundTripContext(ctx, &proto.Message{Type: proto.TypePing, ID: 19}, 5*time.Second)
	if err != nil || reply.Type != proto.TypePong || reply.InitialLaunchState == nil ||
		reply.InitialLaunchState.Token == "" || reply.InitialLaunchState.State != proto.InitialLaunchStarted {
		t.Fatalf("launched Guest fact=%+v/%v", reply, err)
	}
	if err := owner.Launch(ctx, launch); err == nil {
		t.Fatal("running runtime accepted a second initial Launch")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}
