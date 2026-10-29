#!/usr/bin/env bash
# e2e-requires: ["kvm","userfaultfd"]
# Regular CI exercises endpoint absence without a second native artifact.
# Release acceptance also supplies DIRTY_MEMORY_CH with a rebuilt #379 CH;
# that run leaves DIRTY_MEMORY_HIDE_LOG=0 and observes its real 404 response.
set -euo pipefail
export DIRTY_MEMORY_COMPAT=1
: "${DIRTY_MEMORY_HIDE_LOG:=1}"
export DIRTY_MEMORY_HIDE_LOG
source "$(dirname "${BASH_SOURCE[0]}")/snapshot.dirty-memory.sh"
