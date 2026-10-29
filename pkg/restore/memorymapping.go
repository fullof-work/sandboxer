package restore

import (
	"encoding/json"
	"fmt"
)

// validateMemoryMapping establishes the contiguous single-memfd contract used
// by legacy CH's serial, offset-less va_report. Validate before starting CH;
// otherwise a reordered restored mapping could populate the wrong baseline.
func validateMemoryMapping(configJSON, stateJSON []byte, capacity uint64) error {
	var cfg struct {
		Memory struct {
			Size  uint64 `json:"size"`
			Zones []struct {
				ID        string `json:"id"`
				Size      uint64 `json:"size"`
				Shared    bool   `json:"shared"`
				Prefault  bool   `json:"prefault"`
				Hugepages bool   `json:"hugepages"`
			} `json:"zones"`
		} `json:"memory"`
	}
	if err := json.Unmarshal(configJSON, &cfg); err != nil {
		return err
	}
	if cfg.Memory.Size != 0 || len(cfg.Memory.Zones) != 1 {
		return fmt.Errorf("restore memory: expected single external ram0 zone")
	}
	z := cfg.Memory.Zones[0]
	if z.ID != "ram0" || z.Size != capacity || !z.Shared || z.Prefault || z.Hugepages {
		return fmt.Errorf("restore memory: unsupported external zone configuration")
	}
	var tree chSnapshotTree
	if err := json.Unmarshal(stateJSON, &tree); err != nil {
		return err
	}
	mm := tree.Snapshots["memory-manager"]
	if mm == nil || mm.SnapshotData == nil {
		return fmt.Errorf("restore memory: missing memory-manager state")
	}
	var state struct {
		Ranges *struct {
			Data []json.RawMessage `json:"data"`
		} `json:"memory_ranges"`
		Regions []struct {
			Slot      uint32  `json:"slot"`
			GPA       uint64  `json:"gpa"`
			Size      uint64  `json:"size"`
			Offset    *uint64 `json:"file_offset"`
			Zone      string  `json:"zone_id"`
			VirtioMem bool    `json:"virtio_mem"`
		} `json:"guest_ram_mappings"`
	}
	if err := json.Unmarshal([]byte(mm.SnapshotData.State), &state); err != nil {
		return fmt.Errorf("restore memory state: %w", err)
	}
	if state.Ranges == nil || len(state.Ranges.Data) != 0 || len(state.Regions) == 0 {
		return fmt.Errorf("restore memory: non-external or empty RAM layout")
	}
	seen := map[uint32]bool{}
	var offset, gpaEnd uint64
	for _, region := range state.Regions {
		if seen[region.Slot] || region.Zone != "ram0" || region.VirtioMem || region.Offset == nil || *region.Offset != offset || region.Size == 0 || (region.GPA|region.Size|offset)%4096 != 0 || region.GPA < gpaEnd || region.GPA > ^uint64(0)-region.Size || offset > capacity || region.Size > capacity-offset {
			return fmt.Errorf("restore memory: invalid ordered memfd mapping at slot %d", region.Slot)
		}
		seen[region.Slot] = true
		offset += region.Size
		gpaEnd = region.GPA + region.Size
	}
	if offset != capacity {
		return fmt.Errorf("restore memory: incomplete RAM mapping")
	}
	return nil
}
