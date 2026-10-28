#!/usr/bin/env python3
"""Fail closed on same-state, full-residency size experiment associations."""
import json
from pathlib import Path
import sys


def validate(directory, output):
    directory, output = Path(directory), Path(output)
    observations = [json.loads(line) for line in (directory / "observations.jsonl").read_text().splitlines()]
    captures = [json.loads(line) for line in (directory / "capture-residency.jsonl").read_text().splitlines()]
    assert len(observations) == 3 and len(captures) == 2, "expected restore then exactly two captures"
    run = observations[0]["run_id"]
    pid = observations[0]["pid"]
    assert run and pid > 0
    size = 256 << 20
    for sequence, record in enumerate(observations, 1):
        assert (record["run_id"], record["pid"], record["sequence"], record["bytes"]) == (run, pid, sequence, size), record
        assert Path(record["path"]).parent == directory, "observation outside this run"
        assert Path(record["path"]).stat().st_size == size, "missing/truncated RAM observation"
    for number, (capture, name, observation) in enumerate(zip(captures, ("h", "c2"), observations[1:]), 1):
        assert (capture["run_id"], capture["pid"], capture["capture_sequence"], capture["resume_sequence"]) == (run, pid, number, number+1), capture
        assert capture["size"] == size and capture["holes"] == [], "dense baseline requires capture-time full M"
        compared = json.loads((output / (name + "-comparison.json")).read_text())
        result = json.loads((output / (name + ".json")).read_text())
        assert compared["complete_comparison"] == "equal" and compared["logical_bytes"] == size
        assert compared["sha256"] == observation["sha256"], "wrong frozen round"
        assert result["snapshotRef"].rsplit("@", 1)[1] == compared["layers"][0]["Identity"], "wrong snapshot identity"
        if name == "h":
            top = compared["layers"][0]
            assert top["Data"] == size and top["Zero"] == 0 and top["DirtyPresent"], "false top must preserve full W and its clean metadata"
            assert len(compared["layers"]) == 2, "expected exactly one owned H and no external suffix"
    original = json.loads((output / "w2-comparison.json").read_text())
    assert original["layers"][0]["Data"] == size and original["layers"][0]["Zero"] == 0, "legacy H input must fully cover the owned history"
    return {"run_id": run, "pid": pid, "captures": 2, "memory_bytes": size, "observer_materializes_memory": True, "codec": "plaintext"}


if __name__ == "__main__":
    print(json.dumps(validate(*sys.argv[1:])))
