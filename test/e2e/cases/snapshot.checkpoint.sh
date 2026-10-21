#!/usr/bin/env bash
# e2e-requires: ["docker","kvm","userfaultfd"]
# Three writable disks: local versions, resume, concurrent publication, deletion,
# historical/cross-directory materialization, and source-independent restoration.
set -euo pipefail

source "${E2E_LIB:?E2E_LIB is required}/common.sh"
source "$E2E_LIB/sandboxer/tarstream.sh"
: "${E2E_WORKSPACE:?E2E_WORKSPACE is required}"
: "${WORK:?WORK is required}"
: "${OUT:?OUT is required}"
: "${E2E_IMAGE:?E2E_IMAGE is required}"
require_root
require_kvm
for command in cmp docker mkfs.ext4 python3 readlink sha256sum timeout truncate; do
    require_command "$command"
done
for binary in sandbox-ctl flatten-ctl cloud-hypervisor; do require_binary "$binary"; done
for file in sandbox-init sandbox-runtime.bundle vmlinux; do
    [ -f "$BIN/$file" ] || e2e_fail "missing prepared product: $file"
done
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || e2e_fail "prepared E2E_IMAGE is not loaded"
mkdir -p "$WORK/run" "$WORK/base" "$OUT"
RUN_ROOT="$WORK/run"
BASE_ROOT="$WORK/base"
SID="checkpoint-$BASHPID"
PATH_ID="slot-$BASHPID"
CP_DIR="$BASE_ROOT/$PATH_ID/checkpoint"
RUN_PID=""
PUBLISH_PIDS=()
cleanup() {
    local status=$?
    set +e
    for pid in "${PUBLISH_PIDS[@]}"; do
        kill -TERM "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null
    done
    if [ -n "$RUN_PID" ]; then
        kill -TERM "$RUN_PID" 2>/dev/null
        wait "$RUN_PID" 2>/dev/null
    fi
    [ ! -d "$BASE_ROOT/source-hidden" ] || mv "$BASE_ROOT/source-hidden" "$BASE_ROOT/$PATH_ID"
    [ ! -f "$WORK/root.img.hidden" ] || mv "$WORK/root.img.hidden" "$WORK/root.img"
    exit "$status"
}
trap cleanup EXIT

docker save "$E2E_IMAGE" | "$BIN/flatten-ctl" export --output "$WORK/root.img" --no-progress
ROOT_REF="$(plaintext_tarstream_ref "$WORK/root.img")"
for disk in root scratch dataset; do
    truncate -s 256M "$WORK/$disk.ext4"
    mkfs.ext4 -q -F -O ^has_journal "$WORK/$disk.ext4"
done
cat >"$WORK/cold.yaml" <<EOF
resources: { capacity: { cpu: 1, memory: 512MiB }, allocatable: { cpu: 1, memory: 512MiB } }
boot:
  kernel: file://$BIN/vmlinux
  runtime: file://$BIN/sandbox-runtime.bundle
  root:
    base: $ROOT_REF
    overlay: { diff_template: file://$WORK/root.ext4 }
  disks:
    - { name: scratch, diff_template: file://$WORK/scratch.ext4 }
    - { name: dataset, diff_template: file://$WORK/dataset.ext4 }
mounts:
  - { target: /scratch, type: disk, source: scratch }
  - { target: /data, type: disk, source: dataset }
launch: { exec: /bin/sleep, args: ["3600"], restart: never }
EOF
cat >"$WORK/restore.yaml" <<EOF
resources: { capacity: { cpu: 1, memory: 512MiB }, allocatable: { cpu: 1, memory: 512MiB } }
boot:
  kernel: file://$BIN/vmlinux
  runtime: file://$BIN/sandbox-runtime.bundle
EOF

guest() {
    "$BIN/sandbox-ctl" exec --path-id "$CURRENT_PATH" --run-root "$RUN_ROOT" -- "$@"
}
start() {
    local label=$1 path=$2
    shift 2
    CURRENT_PATH=$path
    timeout -k 10s 600 "$BIN/sandbox-ctl" run --sandbox-id "$SID" --path-id "$path" \
        --ch-binary "$BIN/cloud-hypervisor" --run-root "$RUN_ROOT" --base-root "$BASE_ROOT" \
        "$@" >"$OUT/$label.log" 2>&1 &
    RUN_PID=$!
    for _ in $(seq 1 120); do
        if timeout -k 2s 6 "$BIN/sandbox-ctl" exec --path-id "$path" --run-root "$RUN_ROOT" -- /bin/true >/dev/null 2>&1; then
            return
        fi
        if ! kill -0 "$RUN_PID" 2>/dev/null; then
            tail -100 "$OUT/$label.log" >&2 || true
            e2e_fail "$label exited before readiness"
        fi
        sleep 1
    done
    e2e_fail "$label did not become executable"
}
stop_run() {
    kill -TERM "$RUN_PID" 2>/dev/null || true
    wait "$RUN_PID" || true
    RUN_PID=""
}
finish_capture() {
    wait "$RUN_PID" || true
    RUN_PID=""
}
capture() {
    local kind=$1 id=$2
    shift 2
    "$BIN/sandbox-ctl" "$kind" --path-id "$CURRENT_PATH" --run-root "$RUN_ROOT" \
        --checkpoint "$id" --json "$@" >"$OUT/$id.json" 2>"$OUT/$id.log"
}
capture_path() {
    local id=$1 role=$2 directory=$3
    python3 - "$OUT/$id.json" "$role" "$directory" <<'PY'
import json, os, sys
r = json.load(open(sys.argv[1]))
assert isinstance(r['removedRefs'], list) and not r['removedRefs'], r
raw = r[sys.argv[2] + 'Ref']
assert raw.startswith('file://'), raw
name = raw.removeprefix('file://').split('@', 1)[0]
assert name == os.path.basename(name) and name not in ('.', '..'), raw
path = os.path.join(sys.argv[3], name)
assert os.path.isfile(path), path
print(path)
PY
}
check_hashes() {
    guest /bin/sh -c 'sha256sum /checkpoint-payload /scratch/checkpoint-payload /data/checkpoint-payload' >"$OUT/$1.hashes"
    cmp "$OUT/original.hashes" "$OUT/$1.hashes"
}
assert_list() {
    local path=$1 expected=$2
    "$BIN/sandbox-ctl" checkpoint list --path-id "$path" --run-root "$RUN_ROOT" --json >"$OUT/list.json"
    python3 - "$OUT/list.json" "$expected" <<'PY'
import collections, json, sys
refs = json.load(open(sys.argv[1]))
expected = {x: 3 for x in sys.argv[2].split(',') if x}
assert dict(collections.Counter(r['id'] for r in refs)) == expected, refs
assert len({(r['diff_id'], r['id']) for r in refs}) == len(refs)
PY
}
assert_checkpoint() {
    python3 - "$1" "$2" <<'PY'
import json, sys, tarfile, zipfile
with tarfile.open(sys.argv[1]) as carrier:
    payloads = [m for m in carrier if m.isfile() and not m.name.startswith('.kuasar.')]
    assert len(payloads) == 1, payloads
    payloads[0].size = int(payloads[0].pax_headers.get('GNU.sparse.realsize', payloads[0].size))
    z = zipfile.ZipFile(carrier.extractfile(payloads[0]))
    names = z.namelist()
    assert len(names) == len(set(names)), names
    assert set(names) == {'sandbox.runtime.cfg', 'checkpoint.json'}, names
    c = json.loads(z.read('checkpoint.json'))
    assert c['version'] == 1 and c['checkpoint_id'] == sys.argv[2], c
    assert [d['name'] for d in c['disks']] == ['scratch', 'dataset'], c
    ids = [c['root']['diff_id']] + [d['diff_id'] for d in c['disks']]
    assert len(set(ids)) == 3, c
    assert all('/' not in x and '\\' not in x for x in ids), c
PY
}
disk_inodes() {
    python3 - "$CP_DIR/diffs" <<'PY'
import pathlib, sys
for p in sorted(pathlib.Path(sys.argv[1]).glob('*.diff')):
    print(p.name, p.stat().st_ino)
PY
}

start cold "$PATH_ID" --config "$WORK/cold.yaml" --enable-checkpoint
guest /bin/sh -c 'set -e; for f in /checkpoint-payload /scratch/checkpoint-payload /data/checkpoint-payload; do dd if=/dev/urandom of="$f" bs=1048576 count=16 2>/dev/null; done; sync'
guest /bin/sh -c 'sha256sum /checkpoint-payload /scratch/checkpoint-payload /data/checkpoint-payload' >"$OUT/original.hashes"

# Enabling the backend does not turn an ordinary export into a local version.
"$BIN/sandbox-ctl" export --path-id "$PATH_ID" --run-root "$RUN_ROOT" --resume \
    --output "$WORK/ordinary" >"$OUT/ordinary.log" 2>&1
python3 - "$WORK/ordinary/$SID.sandbox" <<'PY'
import sys, tarfile, zipfile
with tarfile.open(sys.argv[1]) as carrier:
    payloads = [m for m in carrier if m.isfile() and not m.name.startswith('.kuasar.')]
    assert len(payloads) == 1, payloads
    # Python tarfile may reapply packed PAX size after decoding the GNU sparse map.
    payloads[0].size = int(payloads[0].pax_headers.get('GNU.sparse.realsize', payloads[0].size))
    with zipfile.ZipFile(carrier.extractfile(payloads[0])) as z:
        assert 'checkpoint.json' not in z.namelist()
PY
assert_list "$PATH_ID" ""

# Validation must fail before suspending the guest or publishing a version.
if "$BIN/sandbox-ctl" snapshot --path-id "$PATH_ID" --run-root "$RUN_ROOT" \
    --checkpoint rejected --output "$WORK/wrong-directory" >"$OUT/rejected.log" 2>&1; then
    e2e_fail "checkpoint capture accepted an unrelated output directory"
fi
guest /bin/true
assert_list "$PATH_ID" ""

capture export export-a --resume
E_A="$(capture_path export-a sandbox "$CP_DIR")"
assert_checkpoint "$E_A" export-a
capture snapshot snapshot-b
finish_capture
S_B="$(capture_path snapshot-b snapshot "$CP_DIR")"
E_B="$(capture_path snapshot-b sandbox "$CP_DIR")"
assert_checkpoint "$E_B" snapshot-b
sha256sum "$E_A" "$E_B" "$S_B" >"$OUT/source-artifacts.hashes"
disk_inodes >"$OUT/before-resume.inodes"
[ "$(wc -l <"$OUT/before-resume.inodes")" -eq 3 ] || e2e_fail "expected exactly three DIFF files"

# A standalone publisher may pin first, then the owner resumes the latest version.
"$BIN/sandbox-ctl" publish --json --to-ref-location "portable=file://$WORK/published-b" "$S_B" >"$OUT/published-b.json" 2>"$OUT/published-b.log" &
PUB_B=$!
PUBLISH_PIDS+=("$PUB_B")
start resumed "$PATH_ID" --config "$WORK/restore.yaml" --restore "$S_B" --enable-checkpoint
disk_inodes >"$OUT/after-resume.inodes"
cmp "$OUT/before-resume.inodes" "$OUT/after-resume.inodes"
check_hashes resumed
assert_list "$PATH_ID" export-a,snapshot-b
cp "$OUT/list.json" "$OUT/initial-version-refs.json"
if "$BIN/sandbox-ctl" checkpoint list --checkpoint-dir "$CP_DIR" --json >"$OUT/offline-busy.log" 2>&1; then
    e2e_fail "offline checkpoint management bypassed a live directory owner"
fi

# Reverse order: owner is live before publication pins; guest overwrites proceed.
# Successful publish routes exact deletion back through the live owner's gate.
"$BIN/sandbox-ctl" publish --json --delete-checkpoint \
    --to-ref-location "portable=file://$WORK/published-a" "$E_A" >"$OUT/published-a.json" 2>"$OUT/published-a.log" &
PUB_A=$!
PUBLISH_PIDS+=("$PUB_A")
guest /bin/sh -c 'set -e; for f in /checkpoint-payload /scratch/checkpoint-payload /data/checkpoint-payload; do dd if=/dev/zero of="$f" bs=1048576 count=16 2>/dev/null; done; sync'
wait "$PUB_B"
wait "$PUB_A"
PUBLISH_PIDS=()
assert_list "$PATH_ID" snapshot-b
# Retry the original exact set after publication has already tombstoned it.
DELETE_ARGS=()
while IFS= read -r id; do DELETE_ARGS+=(--diff-id "$id"); done < <(python3 - "$OUT/initial-version-refs.json" <<'PY'
import json, sys
for r in json.load(open(sys.argv[1])):
    if r['id'] == 'export-a':
        print(r['diff_id'])
PY
)
[ "${#DELETE_ARGS[@]}" -eq 6 ] || e2e_fail "expected exact references for three deleted disks"
"$BIN/sandbox-ctl" checkpoint delete --path-id "$PATH_ID" --run-root "$RUN_ROOT"     --checkpoint export-a "${DELETE_ARGS[@]}" --json >"$OUT/delete-retry.json"
assert_list "$PATH_ID" snapshot-b
sha256sum -c "$OUT/source-artifacts.hashes"
stop_run

# The active epoch changed, so a historical restore must allocate new identities.
start historical "$PATH_ID" --config "$WORK/restore.yaml" --restore "$S_B" --enable-checkpoint
check_hashes historical
capture snapshot historical-c
finish_capture
E_C="$(capture_path historical-c sandbox "$CP_DIR")"
assert_checkpoint "$E_C" historical-c
python3 - "$E_B" "$E_C" <<'PY'
import json, sys, tarfile, zipfile
def ids(p):
    with tarfile.open(p) as carrier:
        payloads = [m for m in carrier if m.isfile() and not m.name.startswith('.kuasar.')]
        assert len(payloads) == 1, payloads
        payloads[0].size = int(payloads[0].pax_headers.get('GNU.sparse.realsize', payloads[0].size))
        with zipfile.ZipFile(carrier.extractfile(payloads[0])) as z:
            c = json.loads(z.read('checkpoint.json'))
    return {c['root']['diff_id'], *(d['diff_id'] for d in c['disks'])}
assert ids(sys.argv[1]).isdisjoint(ids(sys.argv[2]))
PY

# Capturing after materialization must keep its immutable lower dependencies
# after the producing runtime releases all temporary materialization handles.
S_C="$(capture_path historical-c snapshot "$CP_DIR")"
start historical-recapture historical-recapture --config "$WORK/restore.yaml" --restore "$S_C" --enable-checkpoint
check_hashes historical-recapture
stop_run

# A different PathID also materializes versions, with checkpoint mode on or off.
start cross-directory cross-slot --config "$WORK/restore.yaml" --restore "$S_B" --enable-checkpoint
check_hashes cross-directory
capture snapshot cross-d
finish_capture
E_D="$(capture_path cross-d sandbox "$BASE_ROOT/cross-slot/checkpoint")"
assert_checkpoint "$E_D" cross-d
S_D="$(capture_path cross-d snapshot "$BASE_ROOT/cross-slot/checkpoint")"
start cross-recapture cross-recapture --config "$WORK/restore.yaml" --restore "$S_D" --enable-checkpoint
check_hashes cross-recapture
stop_run
start mode-disabled legacy-slot --config "$WORK/restore.yaml" --restore "$S_B"
check_hashes mode-disabled
set +e
"$BIN/sandbox-ctl" export --path-id "$CURRENT_PATH" --run-root "$RUN_ROOT"     --checkpoint unsupported --resume >"$OUT/unsupported.log" 2>&1
UNSUPPORTED_STATUS=$?
set -e
[ "$UNSUPPORTED_STATUS" -eq 78 ] || e2e_fail "legacy checkpoint exit=$UNSUPPORTED_STATUS, want 78"
guest /bin/true
stop_run

# Published roots contain portable data. Hide the original version directory and
# immutable root source before restoring both the published E and S artifacts.
mv "$BASE_ROOT/$PATH_ID" "$BASE_ROOT/source-hidden"
mv "$WORK/root.img" "$WORK/root.img.hidden"
for kind in a b; do
    read -r REF < <(python3 - "$OUT/published-$kind.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
assert isinstance(r['removedRefs'], list)
print(r.get('snapshotRef') or r['sandboxRef'])
PY
)
    if [ "$kind" = a ]; then MODE=--from; else MODE=--restore; fi
    start "portable-$kind" "portable-$kind" --config "$WORK/restore.yaml" "$MODE" "$REF" \
        --ref-location "portable=file://$WORK/published-$kind"
    check_hashes "portable-$kind"
    stop_run
done
mv "$BASE_ROOT/source-hidden" "$BASE_ROOT/$PATH_ID"
mv "$WORK/root.img.hidden" "$WORK/root.img"
echo "PASS snapshot.checkpoint.sh"
