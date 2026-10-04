use super::{relative_display, ProjectRoot, ToolError, ToolErrorKind, ToolResult};
use crate::orchestration::domain::DomainRepository;
use crate::process::{ProcessHost, SystemProcessHost};
use fs2::FileExt;
use ring::digest::{Context, SHA256};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

type VResult<T> = Result<T, Box<dyn std::error::Error>>;
const EXCLUDED: &[&str] = &[
    ".git",
    ".ocg",
    ".codegraph",
    "target",
    "node_modules",
    ".cache",
    ".next",
];
const MAX_FILES: usize = 20_000;
const MAX_BYTES: u64 = 256 * 1024 * 1024;
const MAX_FILE: u64 = 64 * 1024 * 1024;
const ENVIRONMENT: &[&str] = &[
    "PATH",
    "HOME",
    "CARGO_HOME",
    "RUSTUP_HOME",
    "RUSTUP_TOOLCHAIN",
    "RUSTFLAGS",
    "CARGO_ENCODED_RUSTFLAGS",
    "RUSTDOCFLAGS",
    "RUSTC",
    "RUSTC_WRAPPER",
    "RUSTC_WORKSPACE_WRAPPER",
    "CARGO_BUILD_TARGET",
    "CARGO_TARGET_DIR",
    "CC",
    "CXX",
    "AR",
    "CFLAGS",
    "CXXFLAGS",
    "LDFLAGS",
    "MACOSX_DEPLOYMENT_TARGET",
    "SDKROOT",
    "LANG",
    "LC_ALL",
    "TZ",
    "SOURCE_DATE_EPOCH",
    "XDG_CONFIG_HOME",
    "DEVELOPER_DIR",
];

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
fn digest(bytes: &[u8]) -> String {
    format!("sha256:{}", crate::hash::sha256_hex(bytes))
}
fn active(cancelled: &dyn Fn() -> bool) -> VResult<()> {
    if cancelled() {
        return Err("validation evidence cancelled".into());
    }
    Ok(())
}

#[derive(Clone)]
struct Snapshot {
    revision: String,
    stamps: BTreeMap<PathBuf, String>,
}
fn file_hash(path: &Path, cancelled: &dyn Fn() -> bool, remaining: &mut u64) -> VResult<String> {
    let before = fs::symlink_metadata(path)?;
    if !before.is_file() || before.len() > MAX_FILE || before.len() > *remaining {
        return Err(
            "workspace fingerprint unavailable: symlink, special file or size limit".into(),
        );
    }
    let mut file = File::open(path)?;
    if crate::context::fulltext::metadata_stamp(&before)
        != crate::context::fulltext::metadata_stamp(&file.metadata()?)
    {
        return Err("file replaced before fingerprint".into());
    }
    let mut hash = Context::new(&SHA256);
    let mut bytes = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        active(cancelled)?;
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        bytes += count as u64;
        if bytes > MAX_FILE || bytes > *remaining {
            return Err("fingerprint size limit".into());
        }
        hash.update(&buffer[..count]);
    }
    if crate::context::fulltext::metadata_stamp(&before)
        != crate::context::fulltext::metadata_stamp(&fs::symlink_metadata(path)?)
    {
        return Err("file changed during fingerprint".into());
    }
    *remaining -= bytes;
    Ok(format!(
        "sha256:{}",
        hash.finish()
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ))
}
fn snapshot(root: &ProjectRoot, cancelled: &dyn Fn() -> bool) -> VResult<Snapshot> {
    let mut pending = vec![root.path().to_path_buf()];
    let mut entries = BTreeMap::new();
    let mut stamps = BTreeMap::new();
    let mut remaining = MAX_BYTES;
    let mut visited = 0;
    while let Some(directory) = pending.pop() {
        active(cancelled)?;
        let metadata = fs::symlink_metadata(&directory)?;
        if !metadata.is_dir() {
            return Err("workspace directory changed".into());
        }
        stamps.insert(
            directory.clone(),
            crate::context::fulltext::metadata_stamp(&metadata),
        );
        for entry in fs::read_dir(&directory)? {
            active(cancelled)?;
            let entry = entry?;
            visited += 1;
            if visited > MAX_FILES {
                return Err("workspace fingerprint file limit".into());
            }
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.is_dir() && EXCLUDED.iter().any(|name| entry.file_name() == *name) {
                continue;
            }
            if metadata.is_dir() {
                pending.push(path);
            } else {
                let relative = path
                    .strip_prefix(root.path())?
                    .to_str()
                    .ok_or("non UTF-8 path")?;
                let hash = file_hash(&path, cancelled, &mut remaining)?;
                entries.insert(relative.to_string(), hash);
                stamps.insert(path, crate::context::fulltext::metadata_stamp(&metadata));
            }
        }
    }
    for (path, stamp) in &stamps {
        active(cancelled)?;
        if crate::context::fulltext::metadata_stamp(&fs::symlink_metadata(path)?) != *stamp {
            return Err("workspace changed during fingerprint".into());
        }
    }
    Ok(Snapshot {
        revision: digest(&serde_json::to_vec(&entries)?),
        stamps,
    })
}

fn command(root: &ProjectRoot, arguments: &Value) -> VResult<Value> {
    let program = arguments["program"].as_str().ok_or("program required")?;
    if !matches!(program, "cargo" | "git") {
        return Err("not a supported validation executable".into());
    }
    let args = arguments
        .get("args")
        .and_then(Value::as_array)
        .ok_or("args required")?;
    let args: Vec<&str> = args
        .iter()
        .map(|value| value.as_str().ok_or("string argv required"))
        .collect::<Result<_, _>>()?;
    let allowed = if program == "git" {
        args == ["diff", "--check"] || args == ["diff", "--cached", "--check"]
    } else if args
        .first()
        .is_some_and(|arg| matches!(*arg, "check" | "build"))
    {
        let mut position = 1;
        let mut valid = true;
        while position < args.len() {
            match args[position] {
                "--locked"
                | "--offline"
                | "--frozen"
                | "--workspace"
                | "--all-targets"
                | "--all-features"
                | "--no-default-features"
                | "--release"
                | "--quiet"
                | "-q" => {}
                "--package" | "-p" | "--features" | "--target" | "--profile" => {
                    position += 1;
                    if position >= args.len()
                        || args[position].starts_with('-')
                        || args[position].len() > 128
                        || !args[position]
                            .chars()
                            .all(|c| c.is_ascii_alphanumeric() || "_-., ".contains(c))
                    {
                        valid = false;
                        break;
                    }
                }
                _ => {
                    valid = false;
                    break;
                }
            }
            position += 1;
        }
        valid && args.len() <= 32
    } else {
        false
    };
    if !allowed {
        return Err(
            "unsupported validation argv; only cargo check/build and git diff --check".into(),
        );
    }
    let cwd = root
        .resolve_existing(arguments.get("cwd").and_then(Value::as_str).unwrap_or("."))
        .map_err(|error| error.message)?;
    if !cwd.is_dir()
        || cwd
            .strip_prefix(root.path())?
            .components()
            .any(|component| EXCLUDED.iter().any(|name| component.as_os_str() == *name))
    {
        return Err("validation cwd must be a source directory".into());
    }
    Ok(json!({"program":program,"args":args,"cwd":relative_display(root, &cwd)}))
}

fn configuration(path: &Path) -> VResult<String> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(64 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 64 * 1024 {
        return Err("external configuration exceeds evidence limit".into());
    }
    Ok(String::from_utf8(bytes)?)
}

fn quoted_setting(text: &str, key: &str) -> VResult<String> {
    let values: Vec<&str> = text
        .lines()
        .filter_map(|line| {
            let (name, value) = line.split_once('=')?;
            (name.trim() == key).then_some(value.trim())
        })
        .collect();
    if values.len() != 1 {
        return Err("unsupported toolchain configuration".into());
    }
    Ok(serde_json::from_str(values[0])?)
}

fn environment(
    command: &Value,
    root: &ProjectRoot,
    cancelled: &dyn Fn() -> bool,
) -> VResult<String> {
    // Only compiler/toolchain facts enter the digest; credentials and raw environment never enter storage.
    let mut facts = BTreeMap::from([(
        "platform".to_string(),
        Some(format!(
            "{}:{}",
            std::env::consts::OS,
            std::env::consts::ARCH
        )),
    )]);
    for name in ENVIRONMENT {
        facts.insert(
            name.to_string(),
            std::env::var_os(name).map(|value| digest(value.to_string_lossy().as_bytes())),
        );
    }
    for (name, _) in std::env::vars_os() {
        let name = name.to_string_lossy();
        if (name.starts_with("CARGO_") || name.starts_with("RUST") || name.starts_with("GIT_"))
            && !ENVIRONMENT.contains(&name.as_ref())
            && !["RUST_LOG", "GIT_PAGER", "CARGO_ZIGBUILD_CACHE_DIR"].contains(&name.as_ref())
        {
            return Err(
                "unsupported Cargo/Rust/Git environment override; evidence not reusable".into(),
            );
        }
    }
    let mut remaining = MAX_BYTES;
    let program = command["program"].as_str().ok_or("invalid command")?;
    let resolved = SystemProcessHost
        .find_in_path(program)
        .ok_or("executable unavailable")?
        .canonicalize()?;
    facts.insert(
        "executable".into(),
        Some(digest(resolved.to_string_lossy().as_bytes())),
    );
    facts.insert(
        "executable_content".into(),
        Some(file_hash(&resolved, cancelled, &mut remaining)?),
    );
    if program == "cargo" {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or("HOME unavailable")?;
        let cargo = std::env::var_os("CARGO_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".cargo"));
        let rustup = std::env::var_os("RUSTUP_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".rustup"));
        for (label, path) in [
            ("cargo_config", cargo.join("config")),
            ("cargo_config_toml", cargo.join("config.toml")),
            ("rustup_settings", rustup.join("settings.toml")),
        ] {
            match fs::symlink_metadata(&path) {
                Ok(_) => {
                    facts.insert(
                        label.into(),
                        Some(file_hash(&path, cancelled, &mut remaining)?),
                    );
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    facts.insert(label.into(), None);
                }
                Err(error) => return Err(error.into()),
            }
        }
        // Include toolchain binaries without executing commands at the query boundary.
        let settings = configuration(&rustup.join("settings.toml"))?;
        if settings.lines().any(|line| {
            line.split_once('=')
                .and_then(|(name, _)| serde_json::from_str::<String>(name.trim()).ok())
                .is_some_and(|directory| root.path().starts_with(directory))
        }) {
            return Err("rustup directory override unsupported".into());
        }
        let toolchain = std::env::var("RUSTUP_TOOLCHAIN")
            .ok()
            .or_else(|| quoted_setting(&settings, "default_toolchain").ok())
            .ok_or("active toolchain unavailable")?;
        // Local rust-toolchain overrides are source inputs but selection also changes the compiler.
        let mut selected = toolchain;
        for ancestor in root
            .path()
            .join(command["cwd"].as_str().unwrap_or("."))
            .ancestors()
        {
            let config = ancestor.join("rust-toolchain.toml");
            let legacy = ancestor.join("rust-toolchain");
            if config.exists() || legacy.exists() {
                let channel = if config.exists() {
                    quoted_setting(&configuration(&config)?, "channel")?
                } else {
                    configuration(&legacy)?.trim().to_string()
                };
                let host = selected
                    .split_once('-')
                    .map(|(_, host)| host.to_string())
                    .ok_or("toolchain host unknown")?;
                selected = if channel.contains(&host) {
                    channel
                } else {
                    format!("{channel}-{host}")
                };
                break;
            }
        }
        if !selected
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c))
        {
            return Err("unsupported toolchain name".into());
        }
        for name in ["cargo", "rustc"] {
            let path = rustup
                .join("toolchains")
                .join(&selected)
                .join("bin")
                .join(name);
            facts.insert(
                format!("toolchain:{name}"),
                Some(file_hash(&path, cancelled, &mut remaining)?),
            );
        }
        // Ancestor Cargo configuration is an external input; reject rather than silently omit it.
        for ancestor in root.path().ancestors().skip(1) {
            if ancestor.join(".cargo/config").exists()
                || ancestor.join(".cargo/config.toml").exists()
            {
                return Err("external ancestor Cargo configuration unsupported".into());
            }
        }
    } else {
        let git = root.path().join(".git");
        if !git.is_dir() {
            return Err("git evidence requires an in-project .git directory".into());
        }
        for name in ["HEAD", "index", "config", "packed-refs", "info/attributes"] {
            let path = git.join(name);
            facts.insert(
                format!("git:{name}"),
                if path.exists() {
                    Some(file_hash(&path, cancelled, &mut remaining)?)
                } else {
                    None
                },
            );
        }
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or("HOME unavailable")?;
        let xdg = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".config"));
        for (label, path) in [
            ("git_global", home.join(".gitconfig")),
            ("git_xdg", xdg.join("git/config")),
            ("git_system", PathBuf::from("/etc/gitconfig")),
        ] {
            if path.exists() {
                let text = configuration(&path)?;
                if text.lines().any(|line| {
                    line.trim_start().starts_with("[include")
                        || line.to_ascii_lowercase().contains("attributesfile")
                }) {
                    return Err("external Git configuration include/attributes unsupported".into());
                }
                facts.insert(
                    label.into(),
                    Some(file_hash(&path, cancelled, &mut remaining)?),
                );
            }
        }
        let config = configuration(&git.join("config"))?;
        if config.lines().any(|line| {
            line.trim_start().starts_with("[include")
                || line.to_ascii_lowercase().contains("attributesfile")
        }) {
            return Err("external Git configuration include/attributes unsupported".into());
        }
        let head = configuration(&git.join("HEAD"))?;
        if let Some(reference) = head.trim().strip_prefix("ref: ") {
            let path = root.resolve_existing(&format!(".git/{reference}"));
            if let Ok(path) = path {
                facts.insert(
                    "git:head_ref".into(),
                    Some(file_hash(&path, cancelled, &mut remaining)?),
                );
            }
        }
    }
    Ok(digest(&serde_json::to_vec(&facts)?))
}

pub(super) struct Observation {
    command: Value,
    snapshot: Snapshot,
    environment: String,
    started: i64,
}
impl Observation {
    pub(super) fn begin(
        root: &ProjectRoot,
        name: &str,
        arguments: &Value,
        cancelled: &dyn Fn() -> bool,
    ) -> Option<Self> {
        if name != "process.exec" {
            return None;
        }
        let capture = || -> VResult<Self> {
            let command = command(root, arguments)?;
            let environment = environment(&command, root, cancelled)?;
            let snapshot = snapshot(root, cancelled)?;
            Ok(Self {
                command,
                snapshot,
                environment,
                started: now(),
            })
        };
        match capture() {
            Ok(observation) => Some(observation),
            Err(error) => {
                tracing::debug!(%error, "validation evidence observation unavailable");
                None
            }
        }
    }
    pub(super) fn finish(
        self,
        root: &ProjectRoot,
        domain: &DomainRepository,
        call_id: &str,
        result: &ToolResult,
        cancelled: &dyn Fn() -> bool,
    ) {
        let project = || -> VResult<()> {
            active(cancelled)?;
            let Some(exit) = result.output.get("exit") else {
                return Ok(());
            };
            let finished = now();
            let after = snapshot(root, cancelled)?;
            let env = environment(&self.command, root, cancelled)?;
            let stable = self.snapshot.revision == after.revision
                && self.environment == env
                && self
                    .snapshot
                    .stamps
                    .iter()
                    .filter(|(path, _)| path.is_file())
                    .all(|(path, stamp)| after.stamps.get(path) == Some(stamp));
            let call = domain.call(call_id)?;
            if !matches!(call.state.as_str(), "succeeded" | "failed" | "completed") {
                return Ok(());
            }
            let attempt = domain.attempt(&call.attempt_id)?.ok_or("Attempt missing")?;
            let job = domain.job(&attempt.job_id)?.ok_or("Job missing")?;
            let identity = digest(&serde_json::to_vec(&json!([
                job.project_id,
                self.snapshot.revision,
                self.command,
                self.environment
            ]))?);
            let mut evidence = json!({"identity":identity,"project_id":job.project_id,"project_revision":self.snapshot.revision,
                "command":self.command,"environment_fingerprint":self.environment,"stable_during_execution":stable,
                "status":if result.success {"passed"} else {"failed"},"exit":exit,
                "started_at_ms":self.started,"finished_at_ms":finished,"duration_ms":result.metadata.get("durationMs"),
                "provenance":{"job_id":job.id,"attempt_id":attempt.id,"call_id":call.id,"generation":call.generation},
                "output_ref":if call.response.is_some() {json!({"call_id":call.id,"fields":["output.stdout","output.stderr"],"truncated":result.truncated})} else {json!({"call_id":call.id,"fields":["failure"]})}});
            evidence["record_fingerprint"] = json!(digest(evidence.to_string().as_bytes()));
            with_store(root, cancelled, |connection, _| {
                let transaction = connection.transaction()?;
                transaction.execute(
                    "INSERT OR REPLACE INTO evidence(call_id,finished,body) VALUES(?1,?2,?3)",
                    params![call.id, finished, evidence.to_string()],
                )?;
                transaction.execute("DELETE FROM evidence WHERE finished < ?1 OR call_id NOT IN (SELECT call_id FROM evidence ORDER BY finished DESC,call_id LIMIT 128)", params![now().saturating_sub(30 * 24 * 60 * 60 * 1000)])?;
                transaction.commit()?;
                Ok(())
            })
        };
        if let Err(error) = project() {
            tracing::warn!(%error, "derived validation evidence was not recorded");
        }
    }
}

fn with_store<T>(
    root: &ProjectRoot,
    cancelled: &dyn Fn() -> bool,
    action: impl FnOnce(&mut Connection, bool) -> VResult<T>,
) -> VResult<T> {
    for directory in [".ocg", ".ocg/index"] {
        let path = root
            .resolve_for_create(directory)
            .map_err(|error| error.message)?;
        if !path.exists() {
            fs::create_dir(&path)?;
        }
        root.resolve_existing(directory)
            .map_err(|error| error.message)?;
    }
    let database = root
        .resolve_for_create(".ocg/index/validation-evidence.sqlite3")
        .map_err(|error| error.message)?;
    let lock_path = root
        .resolve_for_create(".ocg/index/validation-evidence.lock")
        .map_err(|error| error.message)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)?;
    loop {
        active(cancelled)?;
        match lock.try_lock_exclusive() {
            Ok(()) => break,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10))
            }
            Err(error) => return Err(error.into()),
        }
    }
    let open = || -> rusqlite::Result<Connection> {
        let connection = Connection::open(&database)?;
        connection.busy_timeout(Duration::from_millis(100))?;
        let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if !matches!(version, 0 | 1) {
            return Err(rusqlite::Error::InvalidQuery);
        }
        connection.execute_batch("PRAGMA auto_vacuum=FULL; PRAGMA max_page_count=512; CREATE TABLE IF NOT EXISTS evidence(call_id TEXT PRIMARY KEY,finished INTEGER NOT NULL,body TEXT NOT NULL CHECK(length(body)<=8192)); PRAGMA user_version=1;")?;
        let check: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        if check != "ok" {
            return Err(rusqlite::Error::InvalidQuery);
        }
        Ok(connection)
    };
    let (mut connection, rebuilt) = match open() {
        Ok(connection) => (connection, false),
        Err(error)
            if matches!(&error, rusqlite::Error::InvalidQuery)
                || matches!(&error, rusqlite::Error::SqliteFailure(code, _) if matches!(code.code, rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase)) =>
        {
            fs::remove_file(&database)?;
            (open()?, true)
        }
        Err(error) => return Err(error.into()),
    };
    active(cancelled)?;
    action(&mut connection, rebuilt)
}

pub(super) fn query(
    root: &ProjectRoot,
    arguments: &Value,
    cancelled: &dyn Fn() -> bool,
) -> ToolResult {
    let started = Instant::now();
    let filter = if arguments.get("program").is_some() {
        match command(root, arguments) {
            Ok(command) => Some(command),
            Err(error) => {
                return ToolResult::failure(ToolError::new(
                    ToolErrorKind::InvalidInput,
                    error.to_string(),
                ))
            }
        }
    } else {
        if arguments.get("args").is_some() || arguments.get("cwd").is_some() {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::InvalidInput,
                "program is required with args/cwd",
            ));
        }
        None
    };
    let query = || -> VResult<ToolResult> {
        let project = DomainRepository::open_existing(root.path())?
            .project_at_root(root.path())?
            .ok_or("Project missing")?;
        let current = snapshot(root, cancelled)?;
        let mut environments = BTreeMap::new();
        let (rows, rebuilt) = with_store(root, cancelled, |connection, rebuilt| {
            let mut statement = connection.prepare("SELECT body FROM evidence WHERE finished>=?1 ORDER BY finished DESC,call_id LIMIT 128")?;
            let rows = statement
                .query_map(
                    params![now().saturating_sub(30 * 24 * 60 * 60 * 1000)],
                    |row| row.get::<_, String>(0),
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok((rows, rebuilt))
        })?;
        let limit = arguments.get("limit").and_then(Value::as_u64).unwrap_or(10) as usize;
        let mut entries = Vec::new();
        let mut budget = 0;
        let mut truncated = false;
        for row in rows {
            active(cancelled)?;
            let mut evidence: Value = match serde_json::from_str(&row) {
                Ok(value) => value,
                Err(_) => continue,
            };
            let checksum = evidence
                .as_object_mut()
                .and_then(|object| object.remove("record_fingerprint"));
            if checksum != Some(json!(digest(evidence.to_string().as_bytes()))) {
                tracing::warn!("ignoring damaged derived validation evidence record");
                continue;
            }
            if evidence["project_id"] != project.id
                || filter
                    .as_ref()
                    .is_some_and(|filter| evidence["command"] != *filter)
            {
                continue;
            }
            let command = evidence["command"].clone();
            let key = json!([command["program"], command["cwd"]]).to_string();
            if !environments.contains_key(&key) {
                environments.insert(key.clone(), environment(&command, root, cancelled).ok());
            }
            let applies = evidence["project_revision"] == current.revision
                && evidence["stable_during_execution"] == true
                && environments[&key]
                    .as_ref()
                    .is_some_and(|env| evidence["environment_fingerprint"] == *env);
            evidence["applicable"] = json!(applies);
            evidence["stale"] = json!(!applies);
            evidence["reason"] = json!(if applies {
                "source_command_environment_match"
            } else {
                "revision_environment_changed_or_execution_unstable"
            });
            budget += evidence.to_string().len();
            if entries.len() == limit || budget > super::TOOL_OUTPUT_CAP / 2 {
                truncated = true;
                break;
            }
            entries.push(evidence);
        }
        // Directory and file stamps protect the query from returning a known raced snapshot.
        for (path, stamp) in current.stamps {
            active(cancelled)?;
            if crate::context::fulltext::metadata_stamp(&fs::symlink_metadata(path)?) != stamp {
                return Err("workspace changed during evidence lookup; retry".into());
            }
        }
        Ok(ToolResult {
            success: true,
            output: json!({"project_id":project.id,"project_revision":current.revision,"evidence":entries,
            "scope":{"excluded_directories":EXCLUDED,"environment":"compiler/toolchain allowlist; external dependencies and arbitrary build-script inputs are not validated","reuse":"discovery_only; never automatically skip execution"},"rebuilt":rebuilt}),
            truncated,
            metadata: json!({"elapsed_ms":started.elapsed().as_millis()}),
            error: None,
        })
    };
    match query() {
        Ok(result) => result,
        Err(error) => ToolResult::failure(ToolError::new(
            if cancelled() {
                ToolErrorKind::Cancelled
            } else {
                ToolErrorKind::Unavailable
            },
            error.to_string(),
        )),
    }
}
