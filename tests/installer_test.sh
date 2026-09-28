#!/bin/sh
# Offline tests for install.sh.
#
# Sources the installer with OCG_INSTALLER_TEST=1 and exercises the
# pure functions plus a complete install flow against local fixtures. No
# network, no writes outside the temporary directory, no shell startup files.

set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIXTURES="$ROOT/tests/fixtures"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' 0 1 2 15

FAILURES=0
pass() {
    printf 'ok   %s\n' "$1"
}
fail() {
    printf 'FAIL %s\n' "$1" >&2
    FAILURES=$((FAILURES + 1))
}
check_eq() {
    if [ "$2" = "$3" ]; then
        pass "$1"
    else
        fail "$1 (expected [$3], got [$2])"
    fi
}
check_cmd() {
    _desc="$1"
    shift
    if "$@"; then
        pass "$_desc"
    else
        fail "$_desc"
    fi
}

check_cmd "install.sh passes a syntax check" sh -n "$ROOT/install.sh"

OCG_INSTALLER_TEST=1
OCG_INSTALL_DIR="$TMP/bin"
export OCG_INSTALLER_TEST OCG_INSTALL_DIR
. "$ROOT/install.sh"

# --- artifact and platform mapping -----------------------------------------
check_eq "artifact darwin/arm64" "$(ocg_artifact_for darwin arm64)" "ocg-darwin-arm64"
check_eq "artifact darwin/x86_64" "$(ocg_artifact_for darwin x86_64)" "ocg-darwin-x86_64"
check_eq "artifact linux/arm64" "$(ocg_artifact_for linux arm64)" "ocg-linux-arm64"
check_eq "artifact linux/x86_64" "$(ocg_artifact_for linux x86_64)" "ocg-linux-x86_64"

# --- explicit prerelease pin ------------------------------------------------
# `OCG_VERSION` may pin a prerelease tag verbatim; a bare version gets
# a leading `v`. The Darwin arm64 artifact mapping must stay exact.
OCG_VERSION="v0.2.0-rc.2"
check_eq "explicit prerelease tag is honored" "$(ocg_resolve_tag)" "v0.2.0-rc.2"
OCG_VERSION="0.2.0-rc.2"
check_eq "bare prerelease version gains a v" "$(ocg_resolve_tag)" "v0.2.0-rc.2"
check_eq "Darwin arm64 prerelease artifact" "$(ocg_artifact_for darwin arm64)" "ocg-darwin-arm64"
OCG_VERSION=""

uname() {
    case "$1" in
        -s) printf 'Darwin\n' ;;
        -m) printf 'aarch64\n' ;;
        *) command uname "$@" ;;
    esac
}
check_eq "detect Darwin" "$(ocg_detect_os)" "darwin"
check_eq "detect aarch64 -> arm64" "$(ocg_detect_arch)" "arm64"
unset -f uname

uname() {
    case "$1" in
        -s) printf 'Linux\n' ;;
        -m) printf 'x86_64\n' ;;
        *) command uname "$@" ;;
    esac
}
check_eq "detect Linux" "$(ocg_detect_os)" "linux"
check_eq "detect x86_64" "$(ocg_detect_arch)" "x86_64"
unset -f uname

uname() {
    case "$1" in
        -s) printf 'SunOS\n' ;;
        -m) printf 'sparc\n' ;;
        *) command uname "$@" ;;
    esac
}
if ocg_detect_os >/dev/null 2>&1; then
    fail "unsupported OS is rejected"
else
    pass "unsupported OS is rejected"
fi
unset -f uname

# --- checksum verification --------------------------------------------------
printf 'hello\n' > "$TMP/hello"
check_eq "sha256 of fixture" \
    "$(ocg_checksum "$TMP/hello")" \
    "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03"
check_cmd "verify accepts a matching digest" \
    ocg_verify "$TMP/hello" "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03"
if ocg_verify "$TMP/hello" "00" 2>/dev/null; then
    fail "verify rejects a mismatched digest"
else
    pass "verify rejects a mismatched digest"
fi

# --- temp path and PATH guidance -------------------------------------------
mkdir -p "$TMP/tmpdir"
tmp_path="$(ocg_tmp_path "$TMP/tmpdir")"
case "$tmp_path" in
    "$TMP/tmpdir"/.ocg.tmp.*)
        pass "temp path is inside the install directory"
        ;;
    *)
        fail "temp path is inside the install directory (got $tmp_path)"
        ;;
esac
if [ -f "$tmp_path" ]; then
    pass "mktemp created a unique temp file"
else
    fail "mktemp created a unique temp file"
fi
rm -f "$tmp_path"
if printf '%s\n' "$(ocg_path_guidance)" | grep -q "$TMP/bin"; then
    pass "PATH guidance mentions the install directory"
else
    fail "PATH guidance mentions the install directory"
fi

# --- SHA256SUMS lookup ------------------------------------------------------
check_eq "sums lookup finds an artifact" \
    "$(ocg_sums_lookup "$FIXTURES/SHA256SUMS" ocg-linux-x86_64)" \
    "922ed3d96abd21845f45d595ddeda3549d5cbb15be57d6daef90263fc3c21956"
check_eq "sums lookup ignores unknown artifacts" \
    "$(ocg_sums_lookup "$FIXTURES/SHA256SUMS" ocg-plan9-sparc)" ""

# --- complete offline install ----------------------------------------------
ocg_download() {
    case "$1" in
        */SHA256SUMS) cp "$FIXTURES/SHA256SUMS" "$2" ;;
        *) cp "$FIXTURES/fake-ocg" "$2" ;;
    esac
}
ocg_resolve_tag() {
    printf 'v9.9.9\n'
}

if ( ocg_main ); then
    pass "offline install succeeds"
else
    fail "offline install succeeds"
fi
check_cmd "installed binary is executable" test -x "$TMP/bin/ocg"
check_cmd "installed binary runs" "$TMP/bin/ocg" version
if ls "$TMP/bin"/.ocg.tmp.* >/dev/null 2>&1; then
    fail "temporary files are cleaned up"
else
    pass "temporary files are cleaned up"
fi

# --- HTTPS is mandatory -----------------------------------------------------
OCG_BASE_URL="http://example.invalid/repository"
if ( ocg_main 2>/dev/null ); then
    fail "non-HTTPS release base is rejected"
else
    pass "non-HTTPS release base is rejected"
fi
OCG_BASE_URL="https://github.com/$OCG_REPO"

# --- failed download leaves nothing behind ---------------------------------
rm -f "$TMP/bin/ocg"
ocg_download() {
    return 1
}
if ( ocg_main 2>/dev/null ); then
    fail "a failed download aborts the install"
else
    pass "a failed download aborts the install"
fi
if [ -e "$TMP/bin/ocg" ]; then
    fail "no binary is left after a failed download"
else
    pass "no binary is left after a failed download"
fi

# --- checksum mismatch leaves nothing behind -------------------------------
ocg_download() {
    case "$1" in
        */SHA256SUMS)
            sed 's/^[0-9a-f][0-9a-f]*/0000000000000000000000000000000000000000000000000000000000000000/' \
                "$FIXTURES/SHA256SUMS" > "$2"
            ;;
        *)
            cp "$FIXTURES/fake-ocg" "$2"
            ;;
    esac
}
if ( ocg_main 2>/dev/null ); then
    fail "a checksum mismatch aborts the install"
else
    pass "a checksum mismatch aborts the install"
fi
if [ -e "$TMP/bin/ocg" ]; then
    fail "no binary is left after a checksum mismatch"
else
    pass "no binary is left after a checksum mismatch"
fi

# --- staged validation failure preserves the existing install --------------
printf '#!/bin/sh\nprintf "existing install\\n"\n' > "$TMP/bin/ocg"
chmod +x "$TMP/bin/ocg"
existing="$(cat "$TMP/bin/ocg")"
ocg_download() {
    case "$1" in
        */SHA256SUMS) cp "$FIXTURES/SHA256SUMS" "$2" ;;
        *) cp "$FIXTURES/fake-ocg-bad" "$2" ;;
    esac
}
ocg_verify() {
    return 0
}
if ( ocg_main 2>/dev/null ); then
    fail "a staged version failure aborts the install"
else
    pass "a staged version failure aborts the install"
fi
check_eq "the existing install is preserved" "$(cat "$TMP/bin/ocg")" "$existing"

if [ "$FAILURES" -eq 0 ]; then
    printf 'installer tests: all passed\n'
    exit 0
fi
printf 'installer tests: %s failed\n' "$FAILURES" >&2
exit 1
