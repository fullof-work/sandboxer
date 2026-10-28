#!/usr/bin/env bash
# e2e-requires: ["docker","kvm","userfaultfd"]
# Initial Launch and ordinary required reads survive actual dependency failures.
set -euo pipefail

source "${E2E_LIB:?E2E_LIB is required}/common.sh"
SANDBOXER_LIB="$E2E_LIB/sandboxer"
source "$SANDBOXER_LIB/readiness_helpers.sh"

: "${E2E_WORKSPACE:?E2E_WORKSPACE is required}"
: "${WORK:?WORK is required}"
: "${OUT:?OUT is required}"
: "${E2E_IMAGE:?E2E_IMAGE is required}"

require_root
: "${LAUNCH_RECOVERY_BIN:?prepared launch-recovery fixture required}"
require_kvm
for command in docker debugfs grep mkfs.ext4 mv openssl pgrep python3 truncate wc; do
    require_command "$command"
done
for binary in sandbox-ctl cloud-hypervisor flatten-ctl manifest-ctl store-ctl cache-ctl; do
    require_binary "$binary"
done
for file in sandbox-runtime.bundle vmlinux; do
    [ -f "$BIN/$file" ] || e2e_fail "missing prepared product: $file"
done
[ -f "$SANDBOXER_LIB/read_fault_proxy.py" ] || e2e_fail "missing prepared read_fault_proxy.py"
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || e2e_fail "prepared E2E_IMAGE is not loaded: $E2E_IMAGE"

mkdir -p "$WORK" "$OUT" "$WORK/run" "$WORK/base"
SESSIONS=()
cleanup() {
    local status=$?
    set +e
    if [ "$status" -ne 0 ]; then
        python3 - "$WORK" <<'RECOVERY_DIAGNOSTICS'
import pathlib, re, sys
work = pathlib.Path(sys.argv[1])
# Only bounded tails of owned runtime/error logs; never read config or key files.
# Snapshot/source errors can contain manifest keys, so redact 256-bit hex values.
for name in ('launch.log', 'launch.app.log', 'proxy.log', 'cache.log', 'store.log'):
    try:
        with (work / name).open('rb') as source:
            source.seek(0, 2)
            size = source.tell()
            offset = max(0, size - 4096)
            source.seek(offset)
            tail = source.read(4096).decode(errors='replace')
        if offset:
            tail = tail.partition('\n')[2]  # Discard a possibly partial key/line.
        tail = re.sub(r'(?i)[0-9a-f]{64}', '<redacted>', tail)
        print(f'read-recovery diagnostic {name} bytes={size}', file=sys.stderr)
        print('\n'.join(tail.splitlines()[-12:]), file=sys.stderr)
    except OSError:
        print(f'read-recovery diagnostic {name}: unavailable', file=sys.stderr)
RECOVERY_DIAGNOSTICS
    fi
    for sid in "${SESSIONS[@]}"; do readiness_kill_session TERM "$sid"; done
    for pid in $(jobs -pr); do kill -TERM "$pid" 2>/dev/null || true; done
    sleep 1
    for sid in "${SESSIONS[@]}"; do readiness_kill_session KILL "$sid"; done
    for pid in $(jobs -pr); do kill -KILL "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; done
    exit "$status"
}
trap cleanup EXIT

free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}
wait_file() {
    local path=$1 pattern=$2 pid=$3
    for _ in $(seq 1 1200); do
        grep -qE "$pattern" "$path" 2>/dev/null && return
        kill -0 "$pid" 2>/dev/null || { cat "$path" >&2; return 1; }
        sleep .05
    done
    echo "timed out: $pattern in $path" >&2
    cat "$path" >&2
    return 1
}
fault_count() {
    if [ -f "$WORK/faults.jsonl" ]; then wc -l <"$WORK/faults.jsonl"; else printf '0\n'; fi
}
wait_fault_count() {
    local before=$1 count=$2 pid=$3 target; target=$((before + count))
    for _ in $(seq 1 400); do
        [ "$(fault_count)" -ge "$target" ] && return
        kill -0 "$pid" 2>/dev/null || return 1
        sleep .05
    done
    echo "source failures did not reach $target records (started at $before)" >&2
    return 1
}
wait_new_fault() { wait_fault_count "$1" 1 "$2"; }
wait_file_count() {
    local path=$1 pattern=$2 before=$3 pid=$4 count
    for _ in $(seq 1 200); do
        count=$(grep -cE "$pattern" "$path" 2>/dev/null || true)
        [ "$count" -gt "$before" ] && return
        kill -0 "$pid" 2>/dev/null || { cat "$path" >&2; return 1; }
        sleep .05
    done
    echo "timed out waiting for a new $pattern in $path" >&2
    cat "$path" >&2
    return 1
}
wait_cache_serving() {
    local pid=$1
    for _ in $(seq 1 1200); do
        if "$BIN/cache-ctl" ping --endpoint "127.0.0.1:$HEALTH_PORT" >"$WORK/cache-health" 2>&1 \
            && grep -q SERVING "$WORK/cache-health"; then
            return
        fi
        kill -0 "$pid" 2>/dev/null || {
            cat "$WORK/cache.log" >&2
            return 1
        }
        sleep .05
    done
    cat "$WORK/cache-health" "$WORK/cache.log" >&2
    return 1
}
connect_failure_count() { grep -c '"event": "connect-failure"' "$WORK/faults.jsonl" 2>/dev/null || true; }
wait_connect_failures() {
    local before=$1 count=$2 pid=$3 target; target=$((before + count))
    for _ in $(seq 1 400); do
        [ "$(connect_failure_count)" -ge "$target" ] && return
        kill -0 "$pid" 2>/dev/null || return 1
        sleep .05
    done
    echo "cache connect failures did not reach $target records (started at $before)" >&2
    return 1
}
mode() {
    printf '%s\n' "$1" >"$WORK/mode.next"
    mv "$WORK/mode.next" "$WORK/mode"
}

STORE_PORT=$(free_port)
CACHE_PORT=$(free_port)
HEALTH_PORT=$(free_port)
cat >"$WORK/store.yaml" <<YAML
listen: 127.0.0.1:$STORE_PORT
backend: fs
fs: {root: $WORK/store, verify_content_key: true}
YAML
"$BIN/store-ctl" init --config "$WORK/store.yaml" --generation G1
"$BIN/store-ctl" serve --config "$WORK/store.yaml" >"$WORK/store.log" 2>&1 &
STORE_PID=$!

cat >"$WORK/cache.yaml" <<YAML
mode: tiered
listen: 127.0.0.1:$CACHE_PORT
health_listen: 127.0.0.1:$HEALTH_PORT
rpc_timeout: 2s
freq: {counters: 1M, reset_after: 100K}
tiers:
  - type: embedded
    rocks: {path: $WORK/cache, disk_bytes: 1GiB, mem_ratio: 0.1, direct_reads: false}
origin:
  type: store
  store: {endpoint: 127.0.0.1:$STORE_PORT, pool: 2, timeout: 2s}
YAML
"$BIN/cache-ctl" serve --config "$WORK/cache.yaml" >"$WORK/cache.log" 2>&1 &
CACHE_PID=$!
wait_cache_serving "$CACHE_PID"
mode healthy
python3 "$SANDBOXER_LIB/read_fault_proxy.py" "$WORK/proxy.sock" "$CACHE_PORT" "$WORK/mode" "$WORK/faults.jsonl" >"$WORK/proxy.log" 2>&1 &
PROXY_PID=$!
for _ in $(seq 1 100); do [ -S "$WORK/proxy.sock" ] && break; sleep .05; done
[ -S "$WORK/proxy.sock" ] || e2e_fail "read-fault proxy did not create its socket"

KEY=$(openssl rand -hex 32)
cat >"$WORK/manifest.yaml" <<YAML
manifest: {key: "$KEY"}
store: {endpoint: 127.0.0.1:$STORE_PORT, pool: 4, timeout: 10s}
cache: {endpoint: $WORK/proxy.sock, pool: 4, timeout: 1s}
chunker: {mode: fixed, fixed: {size: 64KiB}}
crypto: {chunk: aes, manifest: aes}
YAML

docker save "$E2E_IMAGE" | "$BIN/flatten-ctl" export --output "$WORK/root.img" --no-progress
ROOT=$("$BIN/manifest-ctl" store --manifest-config "$WORK/manifest.yaml" --no-progress "$WORK/root.img")
[ "${#ROOT}" -eq 64 ] || e2e_fail "root manifest key is not 64 hex characters"

mkdir "$WORK/data"
cp "$LAUNCH_RECOVERY_BIN" "$WORK/data/entry"
chmod 0755 "$WORK/data/entry"
python3 - "$WORK/data/payload" <<'PY'
import sys
with open(sys.argv[1], 'wb') as f:
    for i in range(512):
        f.write(bytes([i % 251]) * 4096)
PY
truncate -s 64M "$WORK/data.raw"
mkfs.ext4 -q -F -O ^has_journal -b 4096 -d "$WORK/data" "$WORK/data.raw"
"$BIN/flatten-ctl" tar stream -f "$WORK/data.img" "$WORK/data.raw"
DATA=$("$BIN/manifest-ctl" store --manifest-config "$WORK/manifest.yaml" --no-progress "$WORK/data.img")
"$BIN/manifest-ctl" get-manifest --manifest-config "$WORK/manifest.yaml" "$DATA" >"$WORK/data.manifest"
# debugfs resolves actual inode blocks; the fixture uses the canonical carrier
# parser to validate logical geometry and the manifest table to select requests.
# Manifest offsets address decoded disk bytes, not tarstream carrier positions.
ENTRY_BLOCK=$(debugfs -R 'blocks /entry' "$WORK/data.raw" 2>/dev/null | awk '{print $1}')
PAYLOAD_BLOCK=$(debugfs -R 'blocks /payload' "$WORK/data.raw" 2>/dev/null | awk '{print $257}')
[ -n "$ENTRY_BLOCK" ] && [ -n "$PAYLOAD_BLOCK" ] || e2e_fail "missing physical file blocks"
APP_KEYS=$("$LAUNCH_RECOVERY_BIN" keys "$WORK/data.manifest" "$WORK/data.img" "$((ENTRY_BLOCK*4096))" 4096)
READ_KEYS=$("$LAUNCH_RECOVERY_BIN" keys "$WORK/data.manifest" "$WORK/data.img" "$((PAYLOAD_BLOCK*4096))" 4096)
python3 - "$APP_KEYS" "$READ_KEYS" <<'PY'
import sys
assert not set(sys.argv[1].split(',')) & set(sys.argv[2].split(',')), 'startup/read fault chunks overlap'
PY
truncate -s 256M "$WORK/seed.diff"
mkfs.ext4 -q -F -O ^has_journal "$WORK/seed.diff"
cat >"$WORK/sandbox.yaml" <<YAML
resources:
  capacity: {cpu: 1, memory: 512MiB}
  allocatable: {cpu: 1, memory: 512MiB}
boot:
  kernel: file://$BIN/vmlinux
  runtime: file://$BIN/sandbox-runtime.bundle
  root:
    base: manifest://$ROOT
    overlay: {diff_template: file://$WORK/seed.diff}
  disks:
    - {name: data, base: manifest://$DATA}
mounts:
  - {target: /data, type: disk, source: data}
launch:
  exec: /data/entry
  args: [guest]
  restart: never
YAML

vm_identity() {
    python3 - "$RUN_PID" <<'PYVM'
import pathlib,sys
parent=int(sys.argv[1])
# Linux lists children under the spawning OS thread, which need not be
# the Go process's main thread.
children=set()
for task in pathlib.Path(f"/proc/{parent}/task").iterdir():
    try: children.update((task/"children").read_text().split())
    except FileNotFoundError: pass
matches=[]
for child in children:
    p=pathlib.Path("/proc")/child
    if p.joinpath("exe").resolve().name == "cloud-hypervisor":
        fields=p.joinpath("stat").read_text().rpartition(")")[2].split()
        assert int(fields[1])==parent
        matches.append((int(child),int(fields[19])))
assert len(matches)==1, matches
print("%s:%s" % matches[0])
PYVM
}

mode "stall:$APP_KEYS"
readiness_begin_capture "$WORK/launch.ready"
READY_READER=$READY_READER_PID
readiness_exec_in_new_session "$BIN/sandbox-ctl" run --ready-fd="$READY_WRITE_FD"     --stdout-to "$WORK/launch.app.log" --config "$WORK/sandbox.yaml"     --manifest-config "$WORK/manifest.yaml" --ch-binary "$BIN/cloud-hypervisor"     --sandbox-id launch --run-root "$WORK/run" --base-root "$WORK/base"     --stats-json "$WORK/launch.stats.json" >"$WORK/launch.log" 2>&1 &
RUN_PID=$!
SESSIONS+=("$RUN_PID")
readiness_close_parent_writer
readiness_wait_event "$WORK/launch.ready" 1 control_ready "$RUN_PID"
wait_file "$WORK/launch.log" 'launch_ack received' "$RUN_PID"
wait_fault_count 0 1 "$RUN_PID"
CH_ID=$(vm_identity) || e2e_fail "no original VM"
# Observe retries of the selected exec read beyond the old post-ACK budget.
# Each record comes from a received request; elapsed time alone is insufficient.
wait_fault_count 0 4 "$RUN_PID"
python3 - "$WORK/faults.jsonl" <<'PY'
import json,sys
rows=[json.loads(s) for s in open(sys.argv[1]) if '"monotonic_ns"' in s]
assert rows[-1]['monotonic_ns']-rows[0]['monotonic_ns'] > 2_000_000_000
PY
! grep -q '^LAUNCH-APP-STARTED$' "$WORK/launch.app.log" || e2e_fail "startup bypassed fault"
BEFORE=$(fault_count)
mode "half:$APP_KEYS"
wait_fault_count "$BEFORE" 2 "$RUN_PID"
# A killed Cache has no retained listener. New connection attempts must observe
# refusal, then the same original request/VM must continue after restart.
mode offline
kill -KILL "$CACHE_PID"
wait "$CACHE_PID" 2>/dev/null || true
BEFORE=$(connect_failure_count)
wait_connect_failures "$BEFORE" 2 "$RUN_PID"
"$BIN/cache-ctl" serve --config "$WORK/cache.yaml" >>"$WORK/cache.log" 2>&1 &
CACHE_PID=$!
wait_cache_serving "$CACHE_PID"
mode healthy
readiness_wait_event "$WORK/launch.ready" 2 ready "$RUN_PID"
readiness_assert_wire "$WORK/launch.ready" "$READY_READER" $'control_ready\nready\n'
wait_file "$WORK/launch.app.log" '^LAUNCH-APP-STARTED$' "$RUN_PID"
[ "$(vm_identity)" = "$CH_ID" ] || e2e_fail "VM was replaced"
[ "$(grep -c 'CH started pid=' "$WORK/launch.log")" = 1 ] || e2e_fail "launch was replayed"

BEFORE=$(fault_count)
mode "disk-read:$READ_KEYS"
"$BIN/sandbox-ctl" exec --sandbox-id launch --run-root "$WORK/run" -- touch /read.begin
wait_fault_count "$BEFORE" 4 "$RUN_PID"
! grep -q '^LAUNCH-READ-RECOVERED$' "$WORK/launch.app.log" || e2e_fail "ordinary read bypassed fault"
mode healthy
wait_file "$WORK/launch.app.log" '^LAUNCH-READ-RECOVERED$' "$RUN_PID"
[ "$(vm_identity)" = "$CH_ID" ] || e2e_fail "ordinary read replaced VM"
kill -TERM "$RUN_PID"
wait "$RUN_PID"
SESSIONS=()
python3 - "$WORK/launch.stats.json" "$WORK/faults.jsonl" <<'PY'
import json,sys
stats=json.load(open(sys.argv[1]))
for b in stats['backends']:
    for op in ('read','write','flush'):
        assert b[op]['err_count']==0, (b['name'],op)
rows=[json.loads(s) for s in open(sys.argv[2])]
assert {'stall','half','offline','disk-read'} <= {r['mode'] for r in rows}
assert any(r.get('event')=='connect-failure' for r in rows)
PY
cp "$WORK/faults.jsonl" "$WORK/launch.stats.json" "$OUT/"
e2e_pass "same Launch and VM recovered post-ACK exec and ordinary mandatory reads"
