#!/usr/bin/env python3
"""The size harness must reject missing, sparse, stale and mismatched evidence."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("evidence", Path(__file__).resolve().parents[1] / "test/e2e/lib/dirty_memory_evidence.py")
evidence = importlib.util.module_from_spec(spec)
spec.loader.exec_module(evidence)


class EvidenceTest(unittest.TestCase):
    def test_associations_fail_closed(self):
        for change in ("valid", "empty", "missing", "extra", "sparse", "wrong-pid", "wrong-round", "stale-run", "wrong-hash", "wrong-identity", "thin-false", "missing-bitmap"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                out = Path(directory)
                ram = out / "ram"
                ram.mkdir()
                observations = []
                captures = []
                for seq in (1, 2, 3):
                    path = ram / f"{seq}.bin"
                    with path.open("wb") as file:
                        file.truncate(256 << 20)
                    observations.append(dict(run_id="fresh", pid=42, sequence=seq, bytes=256 << 20, path=str(path), sha256=f"hash-{seq}"))
                for seq, name in enumerate(("h", "c2"), 1):
                    captures.append(dict(run_id="fresh", pid=42, capture_sequence=seq, resume_sequence=seq+1, size=256 << 20, holes=[]))
                    top = dict(Data=256 << 20, Zero=0, DirtyPresent=True, Identity=f"digest:{name}")
                    compared = dict(complete_comparison="equal", logical_bytes=256 << 20, sha256=f"hash-{seq+1}", layers=[top, copy.copy(top)])
                    if name == "h":
                        if change == "wrong-hash": compared["sha256"] = "wrong"
                        if change == "thin-false": top["Data"] -= 4096
                        if change == "missing-bitmap": top["DirtyPresent"] = False
                        if change == "wrong-identity": top["Identity"] = "digest:wrong"
                    (out / (name + "-comparison.json")).write_text(json.dumps(compared))
                    (out / (name + ".json")).write_text(json.dumps(dict(snapshotRef=f"file://{name}@digest:{name}")))
                (out / "w2-comparison.json").write_text(json.dumps(dict(layers=[dict(Data=256 << 20, Zero=0)])))
                if change == "empty": captures = []
                if change == "missing": captures.pop()
                if change == "extra": captures.append(captures[-1])
                if change == "sparse": captures[0]["holes"] = [[0,4096]]
                if change == "wrong-pid": captures[0]["pid"] += 1
                if change == "wrong-round": captures[0]["resume_sequence"] += 1
                if change == "stale-run": observations[0]["run_id"] = "stale"
                (ram / "observations.jsonl").write_text("\n".join(map(json.dumps, observations)))
                (ram / "capture-residency.jsonl").write_text("\n".join(map(json.dumps, captures)))
                if change == "valid":
                    self.assertEqual(evidence.validate(ram,out)["captures"],2)
                else:
                    with self.assertRaises(AssertionError): evidence.validate(ram,out)


if __name__ == "__main__":
    unittest.main()
