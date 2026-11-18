#!/usr/bin/env bash
# Release build of SprigDB for Linux on the host's architecture (aarch64 on an
# Apple-silicon laptop, which is also what OCI A1 VMs run), in a container, so
# the benchmark runs a Linux binary inside Linux. Built on Debian 12 (glibc
# 2.36): the binary runs in the Debian 13 runtime image and on Ubuntu 24.04.
#
#   e0/build-linux.sh <name> [-p crate ...]     default packages: -p sprigdb -p sprigctl
#
# Steps:
#   1. snapshot the working tree (tracked and new, not ignored, files; no
#      studio/, site/ or brand/) into $E0_WORK/src/sprigdb, copying only files
#      whose content changed, with fresh modification times, so cargo never
#      reuses a stale artifact;
#   2. `cargo build --release --locked` in $IMG_BUILD, with the target and cargo
#      home under $E0_WORK (CARGO_BUILD_JOBS, default 6);
#   3. copy the binaries to $E0_WORK/bin/<name>/ (old files deleted first) with
#      a STAMP file: commit, dirty or not, date, and the image digest.
# E0_SRC=<dir> builds that tree as it is instead (no snapshot), e.g. an agent's
# staging tree; E0_WORK=<dir> moves the target dir, cargo home and bin/ there.
# Run it through the heavy-slot queue: target/heavy-lock.sh run <agent> -- e0/build-linux.sh <name>
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/env.sh"
NAME=${1:?name, e.g. before or e0}; shift
PKGS=("$@"); [[ ${#PKGS[@]} -gt 0 ]] || PKGS=(-p sprigdb -p sprigctl)
SRC=${E0_SRC:-$E0_WORK/src/sprigdb}; TGT=$E0_WORK/linux-target; CH=$E0_WORK/cargo-home; OUT=$E0_WORK/bin/$NAME
mkdir -p "$SRC" "$TGT" "$CH" "$OUT" "$E0_WORK/logs"
LOG=$E0_WORK/logs/build-$NAME-$(date +%Y%m%d-%H%M%S).log
cd "$E0_ROOT"
if [[ -z ${E0_SRC:-} ]]; then
    git ls-files -co --exclude-standard | grep -vE '^(studio/|site/|brand/)' | grep -v node_modules/ |
        while read -r f; do [[ -f $f ]] && echo "$f"; done >"$E0_WORK/src-files.txt"
    rsync -a --checksum --no-times --delete --files-from="$E0_WORK/src-files.txt" "$E0_ROOT/" "$SRC/"
    # --files-from with --delete removes nothing outside the list's directories; prune vanished files by hand
    (cd "$SRC" && find . -type f ! -path './.git/*' | sed 's#^\./##' | sort) >"$E0_WORK/src-have.txt"
    sort "$E0_WORK/src-files.txt" | comm -23 "$E0_WORK/src-have.txt" - | while read -r f; do rm -f "$SRC/$f"; done
fi
STAMP="$(git rev-parse --short HEAD)$([[ -n $(git status --porcelain | head -1) ]] && echo ' dirty')${E0_SRC:+ (tree $E0_SRC)}"
e0_say "building $NAME ($STAMP): ${PKGS[*]} (log $LOG)"
$E0_DOCKER image inspect "$IMG_BUILD" >/dev/null 2>&1 || "$E0_DIR/stack.sh" images
t0=$(date +%s)
$E0_DOCKER run --rm --name "e0bench-build-$$" -m "${E0_BUILD_MEM:-14g}" \
    -e CARGO_TARGET_DIR=/target -e CARGO_HOME=/cargo -e RUSTUP_TOOLCHAIN=1.94.0 \
    -e CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-6}" -e CARGO_INCREMENTAL=0 -e CARGO_PROFILE_RELEASE_DEBUG="${E0_RELEASE_DEBUG:-false}" \
    -v "$SRC:/src" -v "$TGT:/target" -v "$CH:/cargo" -w /src "$IMG_BUILD" \
    bash -c 'mkdir -p /cargo/bin && cargo build --release --locked "$@"' _ "${PKGS[@]}" >"$LOG" 2>&1 ||
    { tail -40 "$LOG"; e0_say "BUILD FAILED ($LOG)"; exit 1; }
for b in $(cd "$TGT/release" && ls); do
    [[ -f $TGT/release/$b && -x $TGT/release/$b ]] || continue
    case $b in *.d | *.rlib | *.so) continue ;; esac
    rm -f "$OUT/$b"; cp "$TGT/release/$b" "$OUT/$b"
done
{ echo "stamp: $STAMP"; echo "built: $(date '+%F %T %Z') in $(( $(date +%s) - t0 )) s"
  echo "image: $IMG_BUILD ($($E0_DOCKER image inspect "$IMG_BUILD" --format '{{.Id}}'))"
  echo "packages: ${PKGS[*]}"; (cd "$OUT" && shasum -a 256 $(ls | grep -v STAMP)); } >"$OUT/STAMP"
cat "$OUT/STAMP"
