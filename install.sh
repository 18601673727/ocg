#!/bin/sh
set -eu

stop_launchd() {
    if [ -n "$domain" ]; then
        stop_pid=$(launchctl print "$domain/$uid/$label" </dev/null 2>/dev/null | awk '$1 == "pid" && $2 == "=" { print $3; exit }' || true)
        launchctl kill SIGTERM "$domain/$uid/$label" </dev/null >/dev/null 2>&1 || true
        case $stop_pid in
            ''|*[!0-9]*) ;;
            *)
                stop_count=0
                while kill -0 "$stop_pid" 2>/dev/null; do
                    [ "$stop_count" -lt 30 ] || break
                    sleep 1 </dev/null || true
                    stop_count=$((stop_count + 1))
                done
                ;;
        esac
    fi
    launchctl bootout "gui/$uid" "$plist" </dev/null >/dev/null 2>&1 || true
    launchctl bootout "user/$uid" "$plist" </dev/null >/dev/null 2>&1 || true
    stop_count=0
    while launchctl print "gui/$uid/$label" </dev/null >/dev/null 2>&1 || launchctl print "user/$uid/$label" </dev/null >/dev/null 2>&1; do
        [ "$stop_count" -lt 30 ] || return 1
        sleep 1 </dev/null || true
        stop_count=$((stop_count + 1))
    done
    return 0
}

systemd_path() {
    systemd_value=$(printf '%s' "$1" | sed 's/%/%%/g')
    if [ "${2:-literal}" = exec ]; then
        printf '%s' "$systemd_value" | sed 's/[\\"]/\\&/g'
    else
        printf '%s' "$systemd_value"
    fi
}

replace_link() {
    link_target=$1
    link_path=$2
    link_stage=$(mktemp -d "$link_path.XXXXXX" </dev/null) || return 1
    if ! ln -s "$link_target" "$link_stage/link" </dev/null; then
        rm -rf "$link_stage" </dev/null
        return 1
    fi
    link_status=0
    if [ "$os" = Linux ]; then
        mv -fT "$link_stage/link" "$link_path" </dev/null || link_status=$?
    else
        mv -fh "$link_stage/link" "$link_path" </dev/null || link_status=$?
    fi
    rm -rf "$link_stage" </dev/null
    return "$link_status"
}

wait_health() {
    health_version=$1
    health_count=0
    while [ "$health_count" -lt 30 ]; do
        if body=$(printf 'header = "Authorization: Bearer %s"\n' "$escaped_token" | curl -sS -o - -w '\n%{http_code}' --max-time 2 --noproxy '*' -K - "http://127.0.0.1:$port/api/v1/health" 2>/dev/null); then
            case $body in
                *"\"version\":\"$health_version\""*'
200') return 0 ;;
            esac
        fi
        sleep 1 </dev/null || true
        health_count=$((health_count + 1))
    done
    return 1
}

rollback_install() {
    rollback_ok=true
    if [ "$service_touched" = true ]; then
        if [ "$os" = Linux ]; then
            systemctl --user stop ocg </dev/null || rollback_ok=false
        else
            stop_launchd || rollback_ok=false
        fi
    fi
    if [ "$current_changed" = true ]; then
        if [ -n "$previous_target" ]; then
            replace_link "$previous_target" "$prefix/current" || rollback_ok=false
        else
            rm -f "$prefix/current" </dev/null || rollback_ok=false
        fi
    fi
    if [ "$command_changed" = true ]; then
        if [ -n "$previous_command" ]; then
            replace_link "$previous_command" "$HOME/.local/bin/ocg" || rollback_ok=false
        else
            rm -f "$HOME/.local/bin/ocg" </dev/null || rollback_ok=false
        fi
    fi
    if [ "$service_touched" = true ]; then
        if [ "$managed" = true ]; then
            cp -p "$tmp/service" "$service_file" </dev/null || rollback_ok=false
        else
            if [ "$os" = Linux ]; then systemctl --user disable ocg </dev/null || true; fi
            rm -f "$service_file" </dev/null || rollback_ok=false
        fi
        if [ "$os" = Linux ]; then
            systemctl --user daemon-reload </dev/null || rollback_ok=false
        fi
    fi
    if [ "$settings_created" = true ]; then rm -f "$settings" </dev/null || rollback_ok=false; fi
    if [ "$stopped" = true ]; then
        if [ "$os" = Linux ]; then
            systemctl --user reset-failed ocg </dev/null || true
            systemctl --user start ocg </dev/null || rollback_ok=false
        else
            launchctl bootstrap "$previous_domain/$uid" "$plist" </dev/null || rollback_ok=false
        fi
        if ! wait_health "$previous_version"; then rollback_ok=false; fi
    fi
    if [ "$rollback_ok" = true ]; then
        printf 'Update to %s failed; previous installation restored. Logs: %s/service.log\n' "$version" "$prefix" >&2
    else
        printf 'Update to %s failed; previous service could not be fully restored. Logs: %s/service.log\n' "$version" "$prefix" >&2
    fi
    [ "$rollback_ok" = true ]
}

finish_install() {
    install_status=$?
    trap - 0
    set +e
    if [ "$rollback" = true ]; then
        if ! rollback_install; then keep_staged=true; fi
        install_status=1
    fi
    if [ -n "$staged" ] && [ "$keep_staged" = false ]; then
        rm -rf "$staged" </dev/null
    fi
    rm -rf "$tmp" </dev/null
    exit "$install_status"
}

main() {
    usage='Usage: install.sh [--version X.Y.Z] [--prefix DIR] [--port N] [--no-service] [--uninstall] [--help]
Install a specific version: curl -fsSL https://github.com/18601673727/ocg/releases/latest/download/install.sh | sh -s -- --version X.Y.Z'
    version=
    version_set=false
    prefix=${HOME:?HOME must be set}/.local/share/ocg
    port=7840
    port_set=false
    service=true
    uninstall=false
    while [ "$#" -gt 0 ]; do
        case $1 in
            --version|--prefix|--port)
                option=$1
                [ "$#" -ge 2 ] || { printf '%s\n' "$usage" >&2; exit 1; }
                case $option in
                    --version) version=$2; version_set=true ;;
                    --prefix) prefix=$2 ;;
                    --port) port=$2; port_set=true ;;
                esac
                shift 2
                ;;
            --no-service) service=false; shift ;;
            --uninstall) uninstall=true; shift ;;
            --help) printf '%s\n' "$usage"; exit 0 ;;
            *) printf 'Unknown option: %s\n%s\n' "$1" "$usage" >&2; exit 1 ;;
        esac
    done

    for tool in curl tar uname mktemp od tr sed id awk cp mkdir rm ln sleep readlink chmod date mv; do
        command -v "$tool" >/dev/null 2>&1 || { printf 'Missing required tool: %s\n' "$tool" >&2; exit 1; }
    done
    if command -v sha256sum >/dev/null 2>&1; then
        hash_tool=sha256sum
    elif command -v shasum >/dev/null 2>&1; then
        hash_tool=shasum
    else
        printf '%s\n' 'Missing required tool: sha256sum or shasum' >&2
        exit 1
    fi
    os=$(uname -s </dev/null)
    cpu=$(uname -m </dev/null)
    case $os in
        Linux)
            case $cpu in
                x86_64) target=x86_64-unknown-linux-gnu ;;
                aarch64|arm64) target=aarch64-unknown-linux-gnu ;;
                *) printf 'Unsupported platform: %s %s\n' "$os" "$cpu" >&2; exit 1 ;;
            esac
            ;;
        Darwin)
            for tool in sysctl launchctl; do
                command -v "$tool" >/dev/null 2>&1 || { printf 'Missing required tool: %s\n' "$tool" >&2; exit 1; }
            done
            case $cpu in
                arm64) target=aarch64-apple-darwin ;;
                x86_64)
                    if [ "$(sysctl -n hw.optional.arm64 </dev/null 2>/dev/null || true)" = 1 ]; then
                        target=aarch64-apple-darwin
                    else
                        target=x86_64-apple-darwin
                    fi
                    ;;
                *) printf 'Unsupported platform: %s %s\n' "$os" "$cpu" >&2; exit 1 ;;
            esac
            ;;
        *) printf 'Unsupported platform: %s %s\n' "$os" "$cpu" >&2; exit 1 ;;
    esac
    case $prefix in
        /*) ;;
        *) printf '%s\n' 'The prefix must be an absolute path.' >&2; exit 1 ;;
    esac
    case $prefix in
        *"'"*|*'
'*|*/../*|*/..|*/./*|*/.|/) printf '%s\n' 'Unsupported prefix path.' >&2; exit 1 ;;
    esac
    while [ "${prefix%/}" != "$prefix" ]; do prefix=${prefix%/}; done
    case $port in ''|*[!0-9]*) printf '%s\n' 'Invalid port.' >&2; exit 1 ;; esac
    if [ "${#port}" -gt 5 ] || [ "$port" -lt 1 ] || [ "$port" -gt 65535 ]; then
        printf '%s\n' 'Invalid port.' >&2
        exit 1
    fi
    if [ "$version_set" = true ]; then
        valid=$(printf '%s\n' "$version" | sed -n '/^[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\(-[0-9A-Za-z][0-9A-Za-z.-]*\)\{0,1\}$/p')
        [ -n "$valid" ] && [ "$valid" = "$version" ] || { printf 'Invalid version: %s\n' "$version" >&2; exit 1; }
    fi

    settings=$prefix/ocg.env
    data=$prefix/data
    label=io.github.18601673727.ocg
    unit=$HOME/.config/systemd/user/ocg.service
    plist=$HOME/Library/LaunchAgents/$label.plist
    uid=$(id -u </dev/null)
    if [ "$uninstall" = true ]; then
        if [ -f "$settings" ]; then
            data=$(sed -n "s/^OCG_DATA_DIR='\(.*\)'$/\1/p" "$settings" </dev/null)
            [ -n "$data" ] || { printf 'Cannot read OCG_DATA_DIR from %s\n' "$settings" >&2; exit 1; }
        fi
        case $data in "$prefix/versions"|"$prefix/versions/"*|"$prefix/current"|"$prefix/current/"*|"$settings"|"$prefix/service.log")
            printf 'Refusing to remove installation files containing data: %s\n' "$data" >&2; exit 1 ;;
        esac
        if [ -d "$data" ] && [ -d "$prefix" ]; then
            data_real=$(cd -P "$data" && pwd -P)
            prefix_real=$(cd -P "$prefix" && pwd -P)
            case $data_real/ in "$prefix_real/versions/"*) printf 'Data is inside the versions directory: %s\n' "$data" >&2; exit 1 ;; esac
            case $prefix_real/ in "$data_real/"*) printf 'Installation is inside the data directory: %s\n' "$data" >&2; exit 1 ;; esac
        fi
        [ ! -L "$prefix/versions" ] || { printf '%s\n' 'Refusing to remove a symlinked versions directory.' >&2; exit 1; }
        if [ "$os" = Linux ] && [ -f "$unit" ]; then
            service_path=$(systemd_path "$prefix" exec)
            OCG_SERVICE_PATH="$service_path/current/ocg" awk '$0 == "ExecStart=\"" ENVIRON["OCG_SERVICE_PATH"] "\"" || $0 == "ExecStart=:\"" ENVIRON["OCG_SERVICE_PATH"] "\"" { found=1 } END { exit !found }' "$unit" </dev/null || { printf 'The existing service belongs to another prefix: %s\n' "$unit" >&2; exit 1; }
        elif [ "$os" = Darwin ] && [ -f "$plist" ]; then
            service_path=$(printf '%s\n' "$prefix" | sed 's/\&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g; s/"/\&quot;/g; s/'"'"'/\&apos;/g')
            awk -v path="$service_path/current/ocg" 'index($0,path) { found=1 } END { exit !found }' "$plist" </dev/null || { printf 'The existing service belongs to another prefix: %s\n' "$plist" >&2; exit 1; }
        fi
        if [ "$os" = Linux ] && [ -f "$unit" ]; then
            systemctl --user stop ocg </dev/null
            systemctl --user disable ocg </dev/null
            rm -f "$unit" </dev/null
            systemctl --user daemon-reload </dev/null
        elif [ "$os" = Darwin ] && [ -f "$plist" ]; then
            launchctl bootout "gui/$uid" "$plist" </dev/null >/dev/null 2>&1 || true
            launchctl bootout "user/$uid" "$plist" </dev/null >/dev/null 2>&1 || true
            rm -f "$plist" </dev/null
        fi
        remove_command=false
        if [ -L "$HOME/.local/bin/ocg" ]; then
            link=$(readlink "$HOME/.local/bin/ocg" </dev/null)
            case $link in
                "$prefix/"*) remove_command=true ;;
                /*) ;;
                *)
                    link_parent=${link%/*}
                    [ "$link_parent" != "$link" ] || link_parent=.
                    if link_real=$(cd -P "$HOME/.local/bin/$link_parent" 2>/dev/null && pwd -P) && prefix_real=$(cd -P "$prefix" 2>/dev/null && pwd -P); then
                        case $link_real/ in "$prefix_real/"*) remove_command=true ;; esac
                    fi
                    ;;
            esac
        fi
        rm -rf "$prefix/versions" </dev/null
        rm -f "$prefix/current" "$settings" "$prefix/service.log" </dev/null
        if [ "$remove_command" = true ]; then rm -f "$HOME/.local/bin/ocg" </dev/null; fi
        printf 'Uninstalled OCG. Data kept at: %s\n' "$data"
        exit 0
    fi

    tmp=$(mktemp -d "${TMPDIR:-/tmp}/ocg-install.XXXXXX" </dev/null)
    rollback=false
    staged=
    keep_staged=false
    trap finish_install 0
    trap 'exit 1' HUP INT TERM
    release_root=${OCG_RELEASE_URL:-https://github.com/18601673727/ocg/releases/download}
    while [ "${release_root%/}" != "$release_root" ]; do release_root=${release_root%/}; done
    if [ "$version_set" = true ]; then
        base=$release_root/v$version
    else
        base=${release_root%/download}/latest/download
    fi
    if ! curl -fsSL "$base/SHA256SUMS" -o "$tmp/SHA256SUMS" </dev/null; then
        printf 'Failed to download release checksums from %s\n' "$base" >&2
        exit 1
    fi
    if [ "$version_set" = false ]; then
        version=$(awk -v target="$target" '
            { name=$2; sub(/^\*/, "", name); suffix="-" target ".tar.gz";
              if (substr(name,1,5)=="ocg-v" && substr(name,length(name)-length(suffix)+1)==suffix) {
                  print substr(name,6,length(name)-5-length(suffix)); count++
              } }
            END { if (count != 1) exit 1 }
        ' "$tmp/SHA256SUMS" </dev/null) || { printf 'No unique archive for %s in SHA256SUMS.\n' "$target" >&2; exit 1; }
        valid=$(printf '%s\n' "$version" | sed -n '/^[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\(-[0-9A-Za-z][0-9A-Za-z.-]*\)\{0,1\}$/p')
        [ "$valid" = "$version" ] || { printf 'Invalid release version: %s\n' "$version" >&2; exit 1; }
    fi
    name=ocg-v$version-$target
    archive=$name.tar.gz
    expected=$(awk -v name="$archive" '{ file=$2; sub(/^\*/, "", file); if (file==name) { print $1; count++ } } END { if (count != 1) exit 1 }' "$tmp/SHA256SUMS" </dev/null) || { printf 'Missing or duplicate checksum for %s\n' "$archive" >&2; exit 1; }
    if ! curl -fsSL "$base/$archive" -o "$tmp/$archive" </dev/null; then
        printf 'Failed to download %s\n' "$archive" >&2
        exit 1
    fi
    if [ "$hash_tool" = sha256sum ]; then
        actual=$(sha256sum "$tmp/$archive" </dev/null | awk '{print $1}')
    else
        actual=$(shasum -a 256 "$tmp/$archive" </dev/null | awk '{print $1}')
    fi
    [ "$actual" = "$expected" ] || { printf 'Checksum mismatch for %s\n' "$archive" >&2; exit 1; }
    tar -xzf "$tmp/$archive" -C "$tmp" </dev/null
    if ! binary_version=$("$tmp/$name/ocg" --version </dev/null 2>&1); then
        if [ "$os" = Linux ]; then printf '%s\n' 'Unable to run OCG; this build needs glibc 2.34 or newer.' >&2; else printf '%s\n' 'Unable to run OCG.' >&2; fi
        exit 1
    fi
    [ "$binary_version" = "ocg $version" ] || { printf 'Unexpected binary version: %s\n' "$binary_version" >&2; exit 1; }

    if [ -f "$settings" ]; then
        data=$(sed -n "s/^OCG_DATA_DIR='\(.*\)'$/\1/p" "$settings" </dev/null)
        port=$(sed -n "s/^OCG_CONTROL_PORT='\(.*\)'$/\1/p" "$settings" </dev/null)
        [ -n "$data" ] && [ -n "$port" ] || { printf 'Cannot read settings from %s\n' "$settings" >&2; exit 1; }
        if [ "$port_set" = true ]; then printf 'Ignoring --port; existing settings are in %s\n' "$settings"; fi
    fi
    if [ -d "$data" ] && [ -d "$prefix" ]; then
        data_real=$(cd -P "$data" && pwd -P)
        prefix_real=$(cd -P "$prefix" && pwd -P)
        case $data_real/ in "$prefix_real/versions/"*) printf 'Data is inside the versions directory: %s\n' "$data" >&2; exit 1 ;; esac
        case $prefix_real/ in "$data_real/"*) printf 'Installation is inside the data directory: %s\n' "$data" >&2; exit 1 ;; esac
    fi
    if [ "$os" = Linux ] && [ -f "$unit" ]; then
        service_path=$(systemd_path "$prefix" exec)
        OCG_SERVICE_PATH="$service_path/current/ocg" awk '$0 == "ExecStart=\"" ENVIRON["OCG_SERVICE_PATH"] "\"" || $0 == "ExecStart=:\"" ENVIRON["OCG_SERVICE_PATH"] "\"" { found=1 } END { exit !found }' "$unit" </dev/null || { printf 'The existing service belongs to another prefix: %s\n' "$unit" >&2; exit 1; }
    elif [ "$os" = Darwin ] && [ -f "$plist" ]; then
        service_path=$(printf '%s\n' "$prefix" | sed 's/\&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g; s/"/\&quot;/g; s/'"'"'/\&apos;/g')
        awk -v path="$service_path/current/ocg" 'index($0,path) { found=1 } END { exit !found }' "$plist" </dev/null || { printf 'The existing service belongs to another prefix: %s\n' "$plist" >&2; exit 1; }
    fi
    previous_target=
    previous_version=
    if [ -L "$prefix/current" ]; then
        previous_target=$(readlink "$prefix/current" </dev/null)
        case $previous_target in
            versions/*) previous=${previous_target#versions/} ;;
            *) printf '%s\n' 'Refusing an unmanaged current link.' >&2; exit 1 ;;
        esac
        case $previous in *[!0-9A-Za-z.-]*|''|.|..) printf '%s\n' 'Invalid current version directory.' >&2; exit 1 ;; esac
        previous_version=$("$prefix/current/ocg" --version </dev/null)
        previous_version=${previous_version#ocg }
    elif [ -e "$prefix/current" ]; then
        printf '%s\n' 'Refusing to replace an unmanaged current directory.' >&2
        exit 1
    fi
    [ ! -L "$prefix/versions" ] || { printf '%s\n' 'Refusing a symlinked versions directory.' >&2; exit 1; }
    mkdir -p "$prefix/versions" </dev/null
    staged=$(mktemp -d "$prefix/versions/$version-XXXXXX" </dev/null)
    cp -R "$tmp/$name/." "$staged/" </dev/null
    staged_version=$("$staged/ocg" --version </dev/null)
    [ "$staged_version" = "ocg $version" ] || { printf '%s\n' 'Staged binary has the wrong version.' >&2; exit 1; }
    next_target=versions/${staged##*/}
    managed=false
    stopped=false
    service_touched=false
    current_changed=false
    command_changed=false
    settings_created=false
    previous_command=
    escaped_token=
    domain=
    previous_domain=
    if [ "$os" = Linux ]; then service_file=$unit; else service_file=$plist; fi
    if [ -f "$service_file" ]; then
        managed=true
        service=true
        cp -p "$service_file" "$tmp/service" </dev/null
    fi
    if [ -f "$settings" ]; then
        token=$(sed -n "s/^OCG_CONTROL_TOKEN='\(.*\)'$/\1/p" "$settings" </dev/null)
        [ -n "$token" ] || { printf 'Cannot read control token from %s\n' "$settings" >&2; exit 1; }
        escaped_token=$(printf '%s\n' "$token" | sed 's/\\/\\\\/g; s/"/\\"/g')
    fi
    rollback=true
    if [ "$managed" = true ] && [ "$os" = Linux ]; then
        stopped=true
        service_touched=true
        systemctl --user stop ocg </dev/null
    elif [ "$managed" = true ]; then
        if launchctl print "gui/$uid/$label" </dev/null >/dev/null 2>&1; then domain=gui; stopped=true
        elif launchctl print "user/$uid/$label" </dev/null >/dev/null 2>&1; then domain=user; stopped=true
        fi
        previous_domain=${domain:-gui}
        stopped=true
        service_touched=true
        stop_launchd || { printf '%s\n' 'Timed out stopping OCG.' >&2; exit 1; }
    fi
    if [ -f "$data/ocg.redb" ]; then
        check_ok=false
        if result=$(OCG_DATA_DIR="$data" "$tmp/$name/ocg" migrate --check </dev/null 2>&1); then check_ok=true; fi
        case $result in
            *'no migration required'*) if [ "$check_ok" = true ]; then result=; fi ;;
        esac
        if [ "$check_ok" != true ] || [ -n "$result" ]; then
            printf '%s\n' "$result" >&2
            case $result in
                *'Database already open'*) printf '%s\n' 'OCG is running outside the service; stop it and rerun the installer.' >&2 ;;
                *'Database repair aborted'*) printf '%s\n' 'Start the old version once, stop it, and rerun the installer.' >&2 ;;
                *'plan: '*migrat*)
                    keep_staged=true
                    quoted_data=$(printf '%s' "$data" | sed "s/'/'\\\\''/g")
                    printf "OCG_DATA_DIR='%s' '%s/ocg' migrate --to <new dir>\n" "$quoted_data" "$staged" >&2
                    printf '%s\n' 'Set OCG_DATA_DIR in ocg.env to the new directory and rerun the installer.' >&2
                    ;;
                *) printf '%s\n' 'Unable to verify database compatibility; installation stopped.' >&2 ;;
            esac
            exit 1
        fi
    fi

    current_changed=true
    replace_link "$next_target" "$prefix/current"
    mkdir -p "$HOME/.local/bin" </dev/null
    if [ -e "$HOME/.local/bin/ocg" ] && [ ! -L "$HOME/.local/bin/ocg" ]; then
        printf 'Not replacing existing file: %s\n' "$HOME/.local/bin/ocg"
    else
        if [ -L "$HOME/.local/bin/ocg" ]; then previous_command=$(readlink "$HOME/.local/bin/ocg" </dev/null); fi
        command_changed=true
        replace_link "$prefix/current/ocg" "$HOME/.local/bin/ocg"
    fi
    case :${PATH:-}: in *:"$HOME/.local/bin":*) ;; *) printf '%s is not on PATH.\n' "$HOME/.local/bin" ;; esac
    if [ ! -f "$settings" ]; then
        umask 077
        token=$(od -An -v -N32 -tx1 /dev/urandom </dev/null | tr -dc '0-9a-f')
        [ "${#token}" -eq 64 ] || { printf '%s\n' 'Failed to generate a control token.' >&2; exit 1; }
        settings_created=true
        printf "OCG_DATA_DIR='%s'\nOCG_UI_DIR='%s'\nOCG_CONTROL_PORT='%s'\nOCG_CONTROL_TOKEN='%s'\n" "$data" "$prefix/current/ui" "$port" "$token" > "$settings"
    fi
    manual="sh -c 'set -a; . \"\$1\"; exec \"\$2\"' sh '$settings' '$prefix/current/ocg'"
    if [ "$service" = false ]; then rollback=false; keep_staged=true; printf 'Start OCG manually:\n%s\n' "$manual"; exit 0; fi
    if [ "$os" = Linux ]; then
        if ! command -v systemctl >/dev/null 2>&1 || ! systemctl --user show-environment </dev/null >/dev/null 2>&1; then
            if [ "$managed" = true ]; then printf '%s\n' 'Cannot restart the existing managed service.' >&2; exit 1; fi
            rollback=false
            keep_staged=true
            printf 'User services are unavailable. Start OCG manually:\n%s\n' "$manual"
            exit 0
        fi
        mkdir -p "$HOME/.config/systemd/user" </dev/null
        unit_prefix=$(systemd_path "$prefix" exec)
        unit_path=$(systemd_path "$prefix")
        service_touched=true
        printf '[Unit]\nDescription=OCG\n[Service]\nEnvironmentFile=%s/ocg.env\nExecStart=:"%s/current/ocg"\nRestart=on-failure\nStandardOutput=append:%s/service.log\nStandardError=append:%s/service.log\n[Install]\nWantedBy=default.target\n' "$unit_path" "$unit_prefix" "$unit_path" "$unit_path" > "$unit"
        systemctl --user daemon-reload </dev/null
        systemctl --user enable ocg </dev/null
        systemctl --user start ocg </dev/null
        printf 'The service stops at logout unless you run: loginctl enable-linger "%s"\n' "${USER:-$(id -un </dev/null)}"
        stop='systemctl --user stop ocg'
    else
        mkdir -p "$HOME/Library/LaunchAgents" </dev/null
        umask 077
        : >> "$prefix/service.log"
        xml_prefix=$(printf '%s\n' "$prefix" | sed 's/\&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g; s/"/\&quot;/g; s/'"'"'/\&apos;/g')
        service_touched=true
        printf '%s\n' '<?xml version="1.0" encoding="UTF-8"?>' '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">' '<plist version="1.0"><dict>' "<key>Label</key><string>$label</string>" '<key>ProgramArguments</key><array><string>/bin/sh</string><string>-c</string><string>set -a; . "$0"; exec "$1"</string>' "<string>$xml_prefix/ocg.env</string><string>$xml_prefix/current/ocg</string></array>" '<key>RunAtLoad</key><true/>' '<key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>' "<key>StandardOutPath</key><string>$xml_prefix/service.log</string>" "<key>StandardErrorPath</key><string>$xml_prefix/service.log</string>" '</dict></plist>' > "$plist"
        if launchctl bootstrap "gui/$uid" "$plist" </dev/null; then domain=gui
        elif launchctl bootstrap "user/$uid" "$plist" </dev/null; then domain=user
        else
            if [ "$managed" = true ]; then printf '%s\n' 'Cannot restart the existing managed service.' >&2; exit 1; fi
            rollback=false
            keep_staged=true
            printf 'User services are unavailable. Start OCG manually:\n%s\n' "$manual"
            exit 0
        fi
        stop="launchctl bootout $domain/$uid '$plist'"
    fi
    token=$(sed -n "s/^OCG_CONTROL_TOKEN='\(.*\)'$/\1/p" "$settings" </dev/null)
    [ -n "$token" ] || { printf 'Cannot read control token from %s\n' "$settings" >&2; exit 1; }
    escaped_token=$(printf '%s\n' "$token" | sed 's/\\/\\\\/g; s/"/\\"/g')
    if wait_health "$version"; then
        rollback=false
        keep_staged=true
        printf 'OCG %s is available at http://127.0.0.1:%s\nData: %s\nSettings: %s\nStop: %s\nUninstall: curl -fsSL https://github.com/18601673727/ocg/releases/latest/download/install.sh | sh -s -- --prefix '\''%s'\'' --uninstall\n' "$version" "$port" "$data" "$settings" "$stop" "$prefix"
        exit 0
    fi
    printf 'OCG did not become healthy. Logs: %s/service.log and %s/ocg_logs\n' "$prefix" "$data" >&2
    exit 1
}

main "$@"
