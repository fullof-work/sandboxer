#!/usr/bin/env bash
# Test-only release-fixed CH without the new cumulative-log owner.
set -euo pipefail
[ "$#" = 2 ] || { echo 'usage: build-compat-ch.sh ARCH OUTPUT' >&2; exit 2; }
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
case "$1" in x86_64|aarch64) ;; *) exit 2 ;; esac
arch=$1
out=$(realpath -m "$2")
shopt -s nullglob
patches=("$ROOT"/native-deps/deps/ch-patches/000*.patch "$ROOT"/native-deps/deps/ch-patches/0010-*.patch "$ROOT"/native-deps/deps/ch-patches/0011-*.patch)
[ "${#patches[@]}" = 11 ] || { echo 'compat CH requires the complete eleven-owner baseline' >&2; exit 1; }
# The key prevents the native builder's skip-existing-output behavior from
# reusing a binary after an owner, build recipe or compiler has changed.
key=$({ sha256sum "${patches[@]}" "$ROOT/native-deps/Makefile" "$ROOT/native-deps/deps/build-cloud-hypervisor.sh" "$ROOT/native-deps/deps/common.sh"; rustc --version; cargo --version; } | sha256sum | cut -d' ' -f1)
# Source validation checks even ignored files. Keep every native output
# outside the admitted source tree, including the tarball cache and symlink.
cache=$(realpath -m "${TMPDIR:-/tmp}/sandboxer-compat-ch/$arch/$key")
mkdir -p "$cache/patches" "$out"
cp "${patches[@]}" "$cache/patches/"
make -C "$ROOT/native-deps" "$cache/bin/cloud-hypervisor" TARGET_ARCH="$arch" \
 BINDIR="$cache/bin" BUILD_DIR="$cache/build" TARBALL_DIR="$cache/tarball" \
 CLOUD_HYPERVISOR_SRC="$cache/src" CLOUD_HYPERVISOR_BUILD_OUT="$cache/target" \
 CH_PATCHES_DIR="$cache/patches"
cp "$cache/bin/cloud-hypervisor" "$out/cloud-hypervisor-compat"
printf 'Compatibility CH applied source tree: '
git -C "$cache/src" rev-parse HEAD^{tree}
sha256sum "${patches[@]}" "$out/cloud-hypervisor-compat"
