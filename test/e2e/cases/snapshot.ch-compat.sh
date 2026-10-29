#!/usr/bin/env bash
# e2e-requires: ["kvm","userfaultfd"]
# Actual release-fixed CH, built from the formal first eleven patches.
set -euo pipefail
: "${COMPAT_CH_BIN:?}" "${WORK:?}" "${OUT:?}"
case_script="$(dirname "${BASH_SOURCE[0]}")/snapshot.dirty-memory.sh"
base_work=$WORK
base_out=$OUT
mkdir -p "$base_out"
sha256sum "$COMPAT_CH_BIN" > "$base_out/compat-ch.sha256"
"$COMPAT_CH_BIN" --version > "$base_out/compat-ch.version"
# Old CH cold capture, both merge modes, repeated saves, release and reuse.
WORK="$base_work/legacy" OUT="$base_out/legacy" \
 DIRTY_MEMORY_COMPAT=1 DIRTY_MEMORY_CH="$COMPAT_CH_BIN" \
 bash "$case_script"
# Four-entry snapshots made by native logging must restore with that same
# unextended CH, using the new host's full Memory reader and original ACK.
WORK="$base_work/cross" OUT="$base_out/cross" \
 DIRTY_MEMORY_COMPAT=0 DIRTY_MEMORY_CH="$BIN/cloud-hypervisor" \
 DIRTY_MEMORY_COMPAT_AFTER_W2_CH="$COMPAT_CH_BIN" \
 bash "$case_script"
