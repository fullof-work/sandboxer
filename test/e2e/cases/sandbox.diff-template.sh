#!/usr/bin/env bash
# e2e-requires: ["docker","kvm","tun"]
# Auto-default overlay diff seeded from a prepared ext4 template.
set -euo pipefail

source "${E2E_LIB:?E2E_LIB is required}/common.sh"
SANDBOXER_LIB="$E2E_LIB/sandboxer"
source "$SANDBOXER_LIB/tarstream.sh"

: "${E2E_WORKSPACE:?E2E_WORKSPACE is required}"
: "${WORK:?WORK is required}"
: "${OUT:?OUT is required}"
: "${E2E_IMAGE:?E2E_IMAGE is required}"

require_root
require_kvm
for command in docker ip mkfs.ext4 python3 timeout; do
    require_command "$command"
done
for binary in sandbox-ctl flatten-ctl cloud-hypervisor; do
    require_binary "$binary"
done
for file in sandbox-init sandbox-runtime.bundle vmlinux; do
    [ -f "$BIN/$file" ] || e2e_fail "missing prepared product: $file"
done
[ -r "$SANDBOXER_LIB/tarstream.sh" ] || e2e_fail "missing prepared tarstream helpers"
docker image inspect "$E2E_IMAGE" >/dev/null 2>&1 || e2e_fail "prepared E2E_IMAGE is not loaded: $E2E_IMAGE"

mkdir -p "$WORK" "$OUT" "$WORK/run" "$WORK/base"
TAP_NAME="dt$((BASHPID % 1000000))"
TAP_CREATED=0
cleanup() {
    local status=$?
    set +e
    [ "$TAP_CREATED" = 1 ] && ip link del "$TAP_NAME" 2>/dev/null
    exit "$status"
}
trap cleanup EXIT

ip tuntap add dev "$TAP_NAME" mode tap
TAP_CREATED=1
ip addr add 169.254.1.0/31 dev "$TAP_NAME"
ip link set "$TAP_NAME" up

ROOT_IMAGE="$WORK/root.img"
docker save "$E2E_IMAGE" | "$BIN/flatten-ctl" export --output "$ROOT_IMAGE" --no-progress
[ -s "$ROOT_IMAGE" ] || e2e_fail "flatten produced an empty root artifact"
ROOT_REF="$(plaintext_tarstream_ref "$ROOT_IMAGE")"

TEMPLATE="$WORK/basic.ext4"
"$BIN/sandbox-ctl" diff create --output "$TEMPLATE" --size 512M >/dev/null
# The provisioning template is a v3 DIFF; ext4 starts inside its data region.
# Inspect the actual superblock without copying the whole sparse disk to raw.
python3 - "$TEMPLATE" <<'PY'
import struct, sys
with open(sys.argv[1], 'rb') as f:
    header = f.read(4096)
    assert header[:8] == b'\x89KDXTS1\n', 'invalid DIFF magic'
    assert struct.unpack_from('>H', header, 8)[0] == 3, 'not a v3 template'
    assert struct.unpack_from('>I', header, 12)[0] == 0, 'template is not plaintext'
    body = struct.unpack_from('>Q', header, 32)[0]
    f.seek(body + 1024)
    superblock = f.read(1024)
assert len(superblock) == 1024 and struct.unpack_from('<H', superblock, 0x38)[0] == 0xef53, 'invalid ext4 superblock'
assert not struct.unpack_from('<I', superblock, 0x5c)[0] & 4, 'diff template unexpectedly contains an ext4 journal'
PY

cat >"$WORK/sandbox.yaml" <<EOF
resources:
  capacity: { cpu: 1, memory: 512MiB }
  allocatable: { cpu: 1, memory: 512MiB }
network: { tap: $TAP_NAME, interface: eth0, ip: 169.254.1.1/31, hostname: e2e-dt }
boot:
  kernel: file://$BIN/vmlinux
  runtime: file://$BIN/sandbox-runtime.bundle
  cmdline: "console=hvc0 printk.time=1"
  root:
    base: $ROOT_REF
    overlay:
      diff_template: file://$TEMPLATE
launch:
  args: ["-c", "import sys; print('PYBOOT-OK', sys.version_info.major*100+sys.version_info.minor, flush=True)"]
  restart: never
EOF

LOG="$OUT/diff-template.log"
timeout -k 10s 90 "$BIN/sandbox-ctl" run \
    --config "$WORK/sandbox.yaml" \
    --ch-binary "$BIN/cloud-hypervisor" \
    --run-root "$WORK/run" \
    --base-root "$WORK/base" >"$LOG" 2>&1

grep -qE '^PYBOOT-OK [0-9]+$' "$LOG" || {
    tail -60 "$LOG" >&2 || true
    e2e_fail "app did not run on template-seeded auto-default diff"
}
echo "PASS sandbox.diff-template.sh"
