#!/usr/bin/env bash
# e2e-requires: ["kvm","userfaultfd"]
# Compare every RAM byte at real capture/restore barriers, and same-state sizes.
set -euo pipefail
source "${E2E_LIB:?}/common.sh"
export SANDBOXER_LIB="$E2E_LIB/sandboxer"
source "$SANDBOXER_LIB/tarstream.sh"
: "${WORK:?}" "${OUT:?}" "${DIRTY_MEMORY_GUEST_BIN:?}" "${MEMORY_COMPARE_BIN:?}" "${MEMORY_BASELINE_BIN:?}"
require_root
require_kvm
for cmd in python3 mkfs.ext4 truncate timeout;do require_command "$cmd";done
for binary in sandbox-ctl cloud-hypervisor flatten-ctl;do require_binary "$binary";done
mkdir -p "$WORK/root/bin" "$WORK/root/tmp" "$WORK/root/etc" "$WORK/root/proc" "$WORK/root/sys" "$WORK/root/dev" "$WORK/runtime" "$WORK/base" "$OUT"
cp "$DIRTY_MEMORY_GUEST_BIN" "$WORK/root/bin/probe"
cp "$SANDBOXER_LIB/dirty_memory_ch.py" "$WORK/ch-observer"
chmod +x "$WORK/ch-observer"
export DIRTY_REAL_CH="$BIN/cloud-hypervisor"
"$BIN/flatten-ctl" export --output "$WORK/root.img" --no-progress "$WORK/root"
ROOT_REF=$(plaintext_tarstream_ref "$WORK/root.img")
truncate -s 256M "$WORK/root-template.ext4"
mkfs.ext4 -q -F -O ^has_journal "$WORK/root-template.ext4"
SID="dirty-$BASHPID"
CP="$WORK/base/$SID/checkpoint"
mkdir -p "$CP"
PID=''
declare -A CAPTURE_RAM
cleanup(){ local status=$?;if [ -n "$PID" ];then kill -TERM "$PID" 2>/dev/null || true;wait "$PID" || true;fi;exit "$status"; }
trap cleanup EXIT
stop_vm(){ kill -TERM "$PID";wait "$PID";PID=''; }
ref_path(){ python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["snapshotRef"].split("file://")[1].split("@")[0])' "$1"; }
ram_path(){ python3 -c 'import json,sys;print(json.loads(open(sys.argv[1]).readlines()[int(sys.argv[2])])["path"])' "$DIRTY_OBSERVATIONS/observations.jsonl" "$1"; }
guest(){ "$BIN/sandbox-ctl" exec --sandbox-id "$SID" --run-root "$WORK/runtime" -- /bin/probe "$1"; }
wait_guest(){
 for i in $(seq 1 90);do
  if timeout 5 "$BIN/sandbox-ctl" exec --sandbox-id "$SID" --run-root "$WORK/runtime" -- /bin/probe check >"$OUT/guest-check.json" 2>"$OUT/guest-check.err";then return;fi
  kill -0 "$PID" || return 1;sleep 1
 done
 return 1
}
capture(){
 local mode=$1 name=$2
 "$BIN/sandbox-ctl" snapshot --sandbox-id "$SID" --run-root "$WORK/runtime" --output "$CP" --resume --merge-ref="$mode" --json >"$OUT/$name.json"
 "$MEMORY_COMPARE_BIN" "$CP/$(ref_path "$OUT/$name.json")" "$(ram_path -1)" >"$OUT/$name-comparison.json"
 CAPTURE_RAM["$CP/$(ref_path "$OUT/$name.json")"]=$(ram_path -1)
}
launch_restore(){
 local source=$1 name=$2
 export DIRTY_OBSERVATIONS="$OUT/ram-$name"
 cat >"$WORK/$name.yaml" <<CFG
resources: { capacity: { cpu: 1, memory: 256MiB }, allocatable: { cpu: 1, memory: 192MiB } }
boot:
  kernel: file://$BIN/vmlinux
  runtime: file://$BIN/sandbox-runtime.bundle
  root: { overlay: { diff: file://$WORK/$name.ext4 } }
CFG
 "$BIN/sandbox-ctl" run --restore "$source" --config "$WORK/$name.yaml" --sandbox-id "$SID" --ch-binary "$WORK/ch-observer" --run-root "$WORK/runtime" --base-root "$WORK/base" >"$OUT/$name.log" 2>&1 &
 PID=$!
 wait_guest || { tail -80 "$OUT/$name.log";e2e_fail 'restore failed'; }
}
restore(){
 local source=$1 name=$2 baseline_ram=''
 if [ "${DIRTY_MEMORY_RESTORE_BASELINE:-0}" = 1 ];then
  # Both independent restores use identical CH config/state and logical RAM.
  # Host device reconstruction may write RAM; compare every byte after it in
  # both paths, with no masks/transforms. Captures always compare source bytes.
  local baseline="$CP/$name-full-baseline.snapshot"
  "$MEMORY_BASELINE_BIN" dense "$source" "${CAPTURE_RAM[$source]}" "$baseline" >"$OUT/$name-baseline-size.json"
  "$MEMORY_COMPARE_BIN" "$baseline" "${CAPTURE_RAM[$source]}" >"$OUT/$name-baseline-input.json"
  launch_restore "$baseline" "$name-baseline"
  baseline_ram=$(ram_path 0)
  stop_vm
 fi
 launch_restore "$source" "$name"
 if [ -n "$baseline_ram" ];then
  cmp "$baseline_ram" "$(ram_path 0)"
  python3 - "$baseline_ram" "$(ram_path 0)" "$source" >"$OUT/$name-comparison.json" <<'PYRESTORE'
import hashlib,json,pathlib,sys
paths=list(map(pathlib.Path,sys.argv[1:3]));sizes=[p.stat().st_size for p in paths]
assert sizes==[256<<20,256<<20],sizes
hashes=[hashlib.file_digest(p.open('rb'),'sha256').hexdigest() for p in paths]
assert hashes[0]==hashes[1]
print(json.dumps(dict(complete_comparison='equal-independent-restores',logical_bytes=sizes[0],sha256=hashes[0],baseline_ram=str(paths[0]),dirty_ram=str(paths[1]),source=sys.argv[3])))
PYRESTORE
 else
  "$MEMORY_COMPARE_BIN" "$source" "$(ram_path 0)" >"$OUT/$name-comparison.json"
 fi
}
# x86 KVM restores rewrite the wall-clock MSR's 12-byte guest structure before
# resume. Disable kvmclock in this fixture to compare ALL bytes without masks.
cat >"$WORK/cold.yaml" <<CFG
resources: { capacity: { cpu: 1, memory: 256MiB }, allocatable: { cpu: 1, memory: 192MiB } }
boot:
  kernel: file://$BIN/vmlinux
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1 no-kvmclock"
  root:
    base: $ROOT_REF
    overlay: { diff: file://$WORK/cold.ext4, diff_template: file://$WORK/root-template.ext4 }
launch: { exec: /bin/probe, args: [serve], restart: never }
CFG
export DIRTY_OBSERVATIONS="$OUT/ram-cold"
"$BIN/sandbox-ctl" run --config "$WORK/cold.yaml" --sandbox-id "$SID" --ch-binary "$WORK/ch-observer" --run-root "$WORK/runtime" --base-root "$WORK/base" >"$OUT/cold.log" 2>&1 &
PID=$!
wait_guest || { tail -80 "$OUT/cold.log";e2e_fail 'cold boot failed'; }
capture true cold
S0="$CP/$(ref_path "$OUT/cold.json")"
stop_vm
restore "$S0" restore1
for pair in 'false w1' 'true c1' 'false w2';do
 read -r mode name <<<"$pair"
 guest mutate >"$OUT/$name-guest.json"
 capture "$mode" "$name"
done
W2="$CP/$(ref_path "$OUT/w2.json")"
stop_vm
restore "$W2" restore2
guest mutate >"$OUT/h-guest.json"
capture false h
H_RAM=$(ram_path -1)
guest mutate >"$OUT/c2-guest.json"
capture true c2
C_RAM=$(ram_path -1)
# A full map before CaptureView cannot grow through clean tails. This proves
# that the dense baseline has exactly the old current-working-set coverage.
python3 "$SANDBOXER_LIB/dirty_memory_evidence.py" "$DIRTY_OBSERVATIONS" "$OUT" >"$OUT/size-association.json"
H_PATH=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["layers"][1]["Path"])' "$OUT/h-comparison.json")
"$MEMORY_BASELINE_BIN" history "$H_PATH" "$W2" "$OUT/legacy-H.snapshot" >"$OUT/legacy-H-size.json"
H_ID=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["identity"].split(":")[1])' "$OUT/legacy-H-size.json")
cp "$OUT/legacy-H.snapshot" "$OUT/$H_ID.snapshot"
"$MEMORY_BASELINE_BIN" dense "$CP/$(ref_path "$OUT/h.json")" "$H_RAM" "$OUT/legacy-W.snapshot" "file://$H_ID.snapshot@digest:$H_ID" >"$OUT/legacy-W-size.json"
"$MEMORY_COMPARE_BIN" "$OUT/legacy-W.snapshot" "$H_RAM" >"$OUT/legacy-W-comparison.json"
"$MEMORY_BASELINE_BIN" dense "$CP/$(ref_path "$OUT/c2.json")" "$C_RAM" "$OUT/legacy-C.snapshot" >"$OUT/legacy-C-size.json"
"$MEMORY_COMPARE_BIN" "$OUT/legacy-C.snapshot" "$C_RAM" >"$OUT/legacy-C-comparison.json"
# Consume real KVM/CH logs, then lose the response and recover the failed capture.
guest mutate-fresh >"$OUT/loss-guest.json"
touch "$DIRTY_OBSERVATIONS/drop-dirty-once"
if "$BIN/sandbox-ctl" snapshot --sandbox-id "$SID" --run-root "$WORK/runtime" --output "$CP" --resume --json >"$OUT/lost-response.json" 2>"$OUT/lost-response.err";then e2e_fail 'expected response-loss capture failure';fi
guest check >"$OUT/recovered-guest.json"
capture true recovered
python3 - "$DIRTY_OBSERVATIONS" "$OUT" <<'PY'
import json,pathlib,sys
p=pathlib.Path(sys.argv[1]);out=pathlib.Path(sys.argv[2])
sys.path.insert(0,str(pathlib.Path(__import__('os').environ['SANDBOXER_LIB'])))
from dirty_memory_evidence import validate_loss_records
reports=[json.loads(line) for line in (p/'capture-residency.jsonl').read_text().splitlines()]
print(json.dumps(validate_loss_records(reports,json.loads((p/'lost-dirty-response.json').read_text()))))
for new,old in [('c2','legacy-C'),('h','legacy-W')]:
 a=json.loads((out/(new+'-comparison.json')).read_text());b=json.loads((out/(old+'-comparison.json')).read_text())
 assert a['sha256']==b['sha256']
 assert a['identity_deduplicated_local_bytes']<b['identity_deduplicated_local_bytes'],(a,b)
print('complete RAM equivalence, cumulative failure recovery and same-state net-size reduction passed')
PY
stop_vm

# Exercise real CH MADV/REMOVE/PUNCH, then restore released Zero and reuse RAM.
restore "$CP/$(ref_path "$OUT/recovered.json")" release
 guest release >"$OUT/balloon-guest-release.json"
python3 "$SANDBOXER_LIB/dirty_memory_balloon.py" "$WORK/runtime/$SID/ch.sock" 100663296 >"$OUT/balloon-inflate.jsonl"
capture false balloon
python3 - "$OUT/balloon-comparison.json" <<'PYTEST'
import json,sys
r=json.load(open(sys.argv[1]));assert r['layers'][0]['Zero']>0,r
PYTEST
BALLOON="$CP/$(ref_path "$OUT/balloon.json")"
stop_vm
restore "$BALLOON" reuse
python3 "$SANDBOXER_LIB/dirty_memory_balloon.py" "$WORK/runtime/$SID/ch.sock" 0 >"$OUT/balloon-deflate.jsonl"
guest reuse >"$OUT/balloon-guest-reuse.json"
capture true reuse
guest mutate >"$OUT/balloon-guest-mutate.json"
capture false reused-w
stop_vm
