#!/bin/sh
# Build the current platform's `ocg` release artifact and its SHA256SUMS.
#
# This mirrors what the release workflow publishes. It is intentionally local
# and deterministic: no uploads, no tags, no network.
#
# Environment:
#   OCG_DIST   output directory (default <repo>/dist)
#   OCG_BIN    use this binary instead of building one
#   CARGO                cargo binary (default `cargo`)

set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${OCG_DIST:-$ROOT/dist}"
CARGO="${CARGO:-cargo}"

case "$(uname -s)" in
    Darwin) os=darwin ;;
    Linux) os=linux ;;
    *)
        printf 'package-release: unsupported operating system: %s\n' "$(uname -s)" >&2
        exit 1
        ;;
esac

case "$(uname -m)" in
    arm64|aarch64) arch=arm64 ;;
    x86_64|amd64) arch=x86_64 ;;
    *)
        printf 'package-release: unsupported CPU architecture: %s\n' "$(uname -m)" >&2
        exit 1
        ;;
esac

artifact="ocg-$os-$arch"
mkdir -p "$OUT_DIR"

if [ -n "${OCG_BIN:-}" ]; then
    source_bin="$OCG_BIN"
else
    "$CARGO" build --release --locked --manifest-path "$ROOT/Cargo.toml"
    source_bin="$ROOT/target/release/ocg"
fi

if [ ! -f "$source_bin" ]; then
    printf 'package-release: binary not found: %s\n' "$source_bin" >&2
    exit 1
fi

cp "$source_bin" "$OUT_DIR/$artifact"
chmod +x "$OUT_DIR/$artifact"

ocg_sha256() {
    if command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    elif command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        printf 'package-release: need shasum or sha256sum\n' >&2
        return 1
    fi
}

: > "$OUT_DIR/SHA256SUMS"
for file in "$OUT_DIR"/ocg-*; do
    [ -f "$file" ] || continue
    name="$(basename "$file")"
    printf '%s  %s\n' "$(ocg_sha256 "$file")" "$name" >> "$OUT_DIR/SHA256SUMS"
done

printf 'packaged %s in %s\n' "$artifact" "$OUT_DIR"
