package sandboxfile

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func checkpointConfig(t *testing.T) (*config.PortableSandboxConfig, *Checkpoint) {
	t.Helper()
	p, err := config.ParsePortableSandboxConfig(livePortableBytes(t))
	if err != nil {
		t.Fatal(err)
	}
	p.Boot.Disks = []config.PortableDiskConfig{{Name: "workspace"}, {Name: "scratch"}}
	for _, disk := range p.Boot.Disks {
		p.Mounts = append(p.Mounts, config.MountConfig{Type: "disk", Source: disk.Name, Target: "/" + disk.Name})
	}
	p, err = p.Checkpointed("")
	if err != nil {
		t.Fatal(err)
	}
	return p, &Checkpoint{Version: 1, CheckpointID: "cp-001", Root: CheckpointDisk{DiffID: "root-1"}, Disks: []CheckpointNamedDisk{{Name: "workspace", DiffID: "data-1"}, {Name: "scratch", DiffID: "data-2"}}}
}

func TestCheckpointLayoutRoundTripHasNoPayloadOrSelf(t *testing.T) {
	p, cp := checkpointConfig(t)
	source, err := BuildCheckpointSource(context.Background(), p, cp)
	if err != nil {
		t.Fatal(err)
	}
	stream := &closeStream{Source: source}
	root, err := Open(context.Background(), stream)
	if err != nil {
		t.Fatal(err)
	}
	if root.ArchiveBase != 0 || root.Payload.Size() != 0 || root.Checkpoint == nil || !root.Portable.IsCheckpoint() {
		t.Fatalf("wrong checkpoint layout: %+v", root)
	}
	if _, err := config.ParsePortableSandboxConfig(root.RuntimeConfig); err == nil {
		t.Fatal("ordinary parser accepted zero-self checkpoint config")
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
	if stream.closes != 1 {
		t.Fatalf("close count %d", stream.closes)
	}
}

func TestCheckpointLayoutRejectsPayloadAndMalformedTopology(t *testing.T) {
	p, cp := checkpointConfig(t)
	for _, mutate := range []func(*Checkpoint){func(c *Checkpoint) { c.Disks = c.Disks[:1] }, func(c *Checkpoint) { c.Disks[0].DiffID = c.Root.DiffID }, func(c *Checkpoint) { c.Disks[0].Name = "missing" }, func(c *Checkpoint) { c.Root.DiffID = "../escape" }, func(c *Checkpoint) { c.CheckpointID = "bad\nname" }} {
		body, _ := json.Marshal(cp)
		var bad Checkpoint
		_ = json.Unmarshal(body, &bad)
		mutate(&bad)
		if _, err := BuildCheckpointSource(context.Background(), p, &bad); err == nil {
			t.Fatalf("accepted malformed checkpoint %+v", bad)
		}
	}
	raw, _ := json.Marshal(cp)
	duplicate := bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
	if _, err := ParseCheckpoint(duplicate, p); err == nil {
		t.Fatal("accepted duplicate JSON key")
	}
	source, err := BuildCheckpointSource(context.Background(), p, cp)
	if err != nil {
		t.Fatal(err)
	}
	bytesWithPayload := append(make([]byte, 4096), sourceBytes(t, source)...)
	if _, err := Open(context.Background(), &closeStream{Source: dataSource(t, bytesWithPayload)}); err == nil {
		t.Fatal("accepted checkpoint with root payload")
	}
}

func TestCheckpointMaterializationRestoresOrdinaryGraph(t *testing.T) {
	p, _ := checkpointConfig(t)
	rootBase := p.Boot.Root.Base
	refs := []string{"file://a.overlay@digest:" + sandboxTestSHA, "file://b.overlay@digest:" + sandboxTestSHA2}
	ordinary, err := p.MaterializedCheckpoint(refs)
	if err != nil {
		t.Fatal(err)
	}
	if ordinary.IsCheckpoint() || ordinary.Boot.Root.Overlay.Base != "self" || ordinary.Boot.Root.Base != rootBase {
		t.Fatal("materialization lost base or self")
	}
	raw, err := config.MarshalPortableSandboxConfig(ordinary)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := config.ParsePortableSandboxConfig(raw); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "checkpoint") {
		t.Fatal("layout leaked into portable runtime config")
	}
	if p.Boot.Root.Overlay.Base != "" {
		t.Fatal("mutated original checkpoint graph")
	}
}
