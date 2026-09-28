#!/bin/sh
# OCG installer.
#
# One-line install:
#   curl -fsSL https://raw.githubusercontent.com/18601673727/ocg/main/install.sh | sh
#
# Downloads the platform `ocg` binary and verifies it against the release
# SHA256SUMS before atomically installing it to ~/.local/bin/ocg. It uses only
# curl, shasum/sha256sum and core POSIX utilities: no sudo, git, node, Homebrew
# or shell startup files are touched.
#
# Environment:
#   OCG_VERSION      pin an exact release tag (for example v0.3.0)
#   OCG_INSTALL_DIR  install directory (default ~/.local/bin)
#   OCG_REPO         owner/name to download from
#   OCG_BASE_URL     override the GitHub base URL (tests, mirrors)
#   OCG_INSTALLER_TEST  set to 1 to source functions without installing

set -eu

OCG_REPO="${OCG_REPO:-18601673727/ocg}"
OCG_INSTALL_DIR="${OCG_INSTALL_DIR:-$HOME/.local/bin}"
OCG_BASE_URL="${OCG_BASE_URL:-https://github.com/$OCG_REPO}"
OCG_VERSION="${OCG_VERSION:-}"

# Detect the operating system as `darwin` or `linux`.
ocg_detect_os() {
    case "$(uname -s)" in
        Darwin) printf 'darwin\n' ;;
        Linux) printf 'linux\n' ;;
        *)
            printf 'ocg: unsupported operating system: %s\n' "$(uname -s)" >&2
            return 1
            ;;
    esac
}

# Normalize the CPU architecture to `arm64` or `x86_64`.
ocg_detect_arch() {
    case "$(uname -m)" in
        arm64|aarch64) printf 'arm64\n' ;;
        x86_64|amd64) printf 'x86_64\n' ;;
        *)
            printf 'ocg: unsupported CPU architecture: %s\n' "$(uname -m)" >&2
            return 1
            ;;
    esac
}

# Exact release artifact name for an OS/arch pair.
ocg_artifact_for() {
    printf 'ocg-%s-%s\n' "$1" "$2"
}

# Download a URL to a destination file over HTTPS.
ocg_download() {
    _url="$1"
    _dest="$2"
    curl --proto '=https' --tlsv1.2 -fsSL "$_url" -o "$_dest"
}

# The release tag to install, from the pin or the latest release redirect.
ocg_resolve_tag() {
    if [ -n "$OCG_VERSION" ]; then
        _tag="$OCG_VERSION"
    else
        _tag="$(curl --proto '=https' --tlsv1.2 -fsSL -o /dev/null -w '%{url_effective}' "$OCG_BASE_URL/releases/latest")"
        _tag="${_tag##*/tag/}"
    fi
    case "$_tag" in
        v*) printf '%s\n' "$_tag" ;;
        *) printf 'v%s\n' "$_tag" ;;
    esac
}

# Print the lowercase SHA-256 of a file using whichever tool is available.
ocg_checksum() {
    _file="$1"
    if command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$_file" | awk '{print $1}'
    elif command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$_file" | awk '{print $1}'
    else
        printf 'ocg: need shasum or sha256sum to verify the download\n' >&2
        return 1
    fi
}

# Verify a file against an expected lowercase hex digest.
ocg_verify() {
    _file="$1"
    _expected="$2"
    _actual="$(ocg_checksum "$_file")"
    if [ "$_actual" != "$_expected" ]; then
        printf 'ocg: checksum mismatch for %s (expected %s, got %s)\n' \
            "$_file" "$_expected" "$_actual" >&2
        return 1
    fi
}

# Look up an artifact digest in a SHA256SUMS file.
ocg_sums_lookup() {
    _sums="$1"
    _artifact="$2"
    awk -v name="$_artifact" '
        {
            file = $2
            sub(/^\*/, "", file)
            if (file == name) {
                print $1
                exit
            }
        }
    ' "$_sums"
}

# A unique temporary path next to the final binary so the final move is
# atomic. `mktemp` avoids predictable names that a local attacker could
# pre-create.
ocg_tmp_path() {
    mktemp "$1/.ocg.tmp.XXXXXX"
}

# Tell the user how to put the install directory on PATH.
ocg_path_guidance() {
    printf 'Add the install directory to your PATH if it is not there already:\n'
    printf '  export PATH="%s:$PATH"\n' "$OCG_INSTALL_DIR"
}

# Remove temporary files. Safe to call more than once.
ocg_cleanup() {
    if [ -n "${OCG_TMP:-}" ]; then
        rm -f "$OCG_TMP"
    fi
    if [ -n "${OCG_SUMS_TMP:-}" ]; then
        rm -f "$OCG_SUMS_TMP"
    fi
}

ocg_main() {
    case "$OCG_BASE_URL" in
        https://*) ;;
        *)
            printf 'ocg: refusing non-HTTPS release base URL: %s\n' "$OCG_BASE_URL" >&2
            return 1
            ;;
    esac
    _os="$(ocg_detect_os)"
    _arch="$(ocg_detect_arch)"
    _artifact="$(ocg_artifact_for "$_os" "$_arch")"
    _tag="$(ocg_resolve_tag)"

    mkdir -p "$OCG_INSTALL_DIR"
    OCG_TMP="$(ocg_tmp_path "$OCG_INSTALL_DIR")"
    OCG_SUMS_TMP="$OCG_TMP.sums"
    trap 'ocg_cleanup' 0 1 2 15

    _release_url="$OCG_BASE_URL/releases/download/$_tag"
    if ! ocg_download "$_release_url/SHA256SUMS" "$OCG_SUMS_TMP"; then
        printf 'ocg: failed to download SHA256SUMS for %s\n' "$_tag" >&2
        return 1
    fi
    if ! ocg_download "$_release_url/$_artifact" "$OCG_TMP"; then
        printf 'ocg: failed to download %s\n' "$_artifact" >&2
        return 1
    fi

    _expected="$(ocg_sums_lookup "$OCG_SUMS_TMP" "$_artifact")"
    if [ -z "$_expected" ]; then
        printf 'ocg: SHA256SUMS has no entry for %s\n' "$_artifact" >&2
        return 1
    fi
    if ! ocg_verify "$OCG_TMP" "$_expected"; then
        return 1
    fi
    chmod +x "$OCG_TMP"

    # Validate the staged binary before touching any existing installation.
    if ! "$OCG_TMP" version >/dev/null 2>&1; then
        printf 'ocg: staged binary failed its version check; keeping any existing install\n' >&2
        return 1
    fi

    mv -f "$OCG_TMP" "$OCG_INSTALL_DIR/ocg"

    printf 'Installed OCG %s to %s/ocg\n' "$_tag" "$OCG_INSTALL_DIR"
    ocg_path_guidance
}

# When sourced by the test harness, do not run the installer.
if [ "${OCG_INSTALLER_TEST:-0}" != "1" ]; then
    ocg_main "$@"
fi
