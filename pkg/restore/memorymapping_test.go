package restore

import (
	"encoding/json"
	"testing"
)

func TestRestoreMemoryMappingLegacyOffsets(t *testing.T) {
	config := []byte(`{"memory":{"size":0,"zones":[{"id":"ram0","size":8192,"shared":true}]}}`)
	for _, name := range []string{"valid-gap", "reordered", "missing-offset", "overlap", "short", "other-zone", "virtio-mem", "payload"} {
		t.Run(name, func(t *testing.T) {
			regions := []map[string]any{
				{"slot": 0, "gpa": 0, "size": 4096, "file_offset": 0, "zone_id": "ram0"},
				{"slot": 1, "gpa": 1 << 32, "size": 4096, "file_offset": 4096, "zone_id": "ram0"},
			}
			ranges := []any{}
			switch name {
			case "reordered":
				regions[0], regions[1] = regions[1], regions[0]
			case "missing-offset":
				delete(regions[0], "file_offset")
			case "overlap":
				regions[1]["gpa"] = 0
			case "short":
				regions = regions[:1]
			case "other-zone":
				regions[1]["zone_id"] = "ram1"
			case "virtio-mem":
				regions[0]["virtio_mem"] = true
			case "payload":
				ranges = append(ranges, map[string]any{"gpa": 0, "length": 4096})
			}
			inner, _ := json.Marshal(map[string]any{"guest_ram_mappings": regions, "memory_ranges": map[string]any{"data": ranges}})
			tree := chSnapshotTree{Snapshots: map[string]*chSnapshotTree{"memory-manager": {SnapshotData: &chSnapshotData{State: string(inner)}}}}
			state, _ := json.Marshal(tree)
			err := validateMemoryMapping(config, state, 8192)
			if (err == nil) != (name == "valid-gap") {
				t.Fatalf("validation: %v", err)
			}
		})
	}
}
