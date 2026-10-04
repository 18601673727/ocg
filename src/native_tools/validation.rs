use super::{relative_display, ProjectRoot, ToolError, ToolErrorKind, ToolResult};
use crate::orchestration::domain::DomainRepository;
use crate::process::{CaptureRunner, ProcessHost, SystemProcessHost};
use fs2::FileExt;
use ring::digest::{Context, SHA256};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub(super) type VResult<T> = Result<T, Box<dyn std::error::Error>>;
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
const CARGO_ENVIRONMENT: &[&str] = &[
    "PATH",
    "CARGO_HOME",
    "RUSTUP_HOME",
    "RUSTUP_TOOLCHAIN",
    "RUSTFLAGS",
    "CARGO_ENCODED_RUSTFLAGS",
    "RUSTC",
    "RUSTC_WRAPPER",
    "RUSTC_WORKSPACE_WRAPPER",
    "CARGO_BUILD_RUSTC",
    "CARGO_BUILD_RUSTC_WRAPPER",
    "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER",
    "CARGO_BUILD_RUSTFLAGS",
    "CARGO_BUILD_TARGET",
    "CARGO_TARGET_DIR",
    "CARGO_BUILD_TARGET_DIR",
    "CARGO_BUILD_INCREMENTAL",
    "CARGO_INCREMENTAL",
    "CARGO_BUILD_JOBS",
    "CARGO_BUILD_WARNINGS",
    "CARGO_BUILD_DEP_INFO_BASEDIR",
    "CC",
    "CXX",
    "AR",
    "CFLAGS",
    "CXXFLAGS",
    "LDFLAGS",
    "MACOSX_DEPLOYMENT_TARGET",
    "SDKROOT",
    "DEVELOPER_DIR",
    "SOURCE_DATE_EPOCH",
];
const GIT_ENVIRONMENT: &[&str] = &[
    "PATH",
    "HOME",
    "XDG_CONFIG_HOME",
    "LANG",
    "LC_ALL",
    "GIT_INDEX_FILE",
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_COMMON_DIR",
    "GIT_CONFIG_SYSTEM",
    "GIT_CONFIG_GLOBAL",
    "GIT_CONFIG_NOSYSTEM",
    "GIT_ATTR_NOSYSTEM",
    "GIT_NO_REPLACE_OBJECTS",
    "GIT_REPLACE_REF_BASE",
    "GIT_EXTERNAL_DIFF",
    "GIT_DIFF_OPTS",
    "GIT_EXEC_PATH",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_NAMESPACE",
];
const INPUT_MODEL_VERSION: u32 = 3;

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
pub(super) struct Snapshot {
    pub(super) revision: String,
    pub(super) revisions: BTreeMap<String, String>,
    stamps: BTreeMap<PathBuf, String>,
}

impl Snapshot {
    pub(super) fn verify(&self, cancelled: &dyn Fn() -> bool) -> VResult<()> {
        for (path, stamp) in &self.stamps {
            active(cancelled)?;
            if crate::context::fulltext::metadata_stamp(&fs::symlink_metadata(path)?) != *stamp {
                return Err("workspace changed during evidence lookup; retry".into());
            }
        }
        Ok(())
    }
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
pub(super) fn snapshot(root: &ProjectRoot, cancelled: &dyn Fn() -> bool) -> VResult<Snapshot> {
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
        revisions: entries,
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

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ValidationInputs {
    version: u32,
    toolchain: Option<String>,
    environment: String,
    config: Option<String>,
    git_state: Option<String>,
    dependency_boundary: Option<String>,
    unsupported_inputs: Vec<String>,
    unknown: bool,
    #[serde(skip)]
    toolchain_ms: u128,
    #[serde(skip)]
    git_state_ms: u128,
    #[serde(skip)]
    dependency_boundary_ms: u128,
    #[serde(skip)]
    git_index_stamp: Option<String>,
}
impl ValidationInputs {
    fn unavailable() -> Self {
        Self {
            version: INPUT_MODEL_VERSION,
            toolchain: None,
            environment: String::new(),
            config: None,
            git_state: None,
            dependency_boundary: None,
            unsupported_inputs: Vec::new(),
            unknown: true,
            toolchain_ms: 0,
            git_state_ms: 0,
            dependency_boundary_ms: 0,
            git_index_stamp: None,
        }
    }

    fn fingerprint(&self) -> String {
        digest(json!(self).to_string().as_bytes())
    }
}

fn validation_fingerprint(command: &Value, revision: &str, inputs: &ValidationInputs) -> String {
    digest(
        json!([INPUT_MODEL_VERSION, revision, command, inputs.fingerprint()])
            .to_string()
            .as_bytes(),
    )
}

fn probe(
    program: &str,
    args: &[&str],
    cwd: &Path,
    cap: usize,
    deadline: Instant,
    cancelled: &dyn Fn() -> bool,
) -> VResult<crate::process::CapturedOutput> {
    active(cancelled)?;
    let output = crate::process::SystemCaptureRunner.run_with_cancellation(
        program,
        &args.iter().map(|arg| arg.to_string()).collect::<Vec<_>>(),
        cwd,
        cap,
        &|| cancelled() || Instant::now() >= deadline,
    )?;
    active(cancelled)?;
    if Instant::now() >= deadline || output.truncated() {
        return Err("input probe timed out or exceeded bounds".into());
    }
    Ok(output)
}
fn probe_bytes(
    program: &str,
    args: &[&str],
    cwd: &Path,
    cap: usize,
    deadline: Instant,
    cancelled: &dyn Fn() -> bool,
) -> VResult<Vec<u8>> {
    let output = probe(program, args, cwd, cap, deadline, cancelled)?;
    if !output.success {
        return Err("input probe unsuccessful".into());
    }
    Ok(output.stdout)
}
fn executable_identity(program: &str, cancelled: &dyn Fn() -> bool) -> VResult<String> {
    let executable = SystemProcessHost
        .find_in_path(program)
        .ok_or("executable unavailable")?
        .canonicalize()?;
    let mut remaining = MAX_BYTES;
    let content = file_hash(&executable, cancelled, &mut remaining)?;
    Ok(digest(
        json!([executable.to_string_lossy(), content])
            .to_string()
            .as_bytes(),
    ))
}
fn toolchain(
    command: &Value,
    cwd: &Path,
    deadline: Instant,
    cancelled: &dyn Fn() -> bool,
) -> VResult<(String, Option<String>)> {
    let program = command["program"].as_str().ok_or("program missing")?;
    let executable = executable_identity(program, cancelled)?;
    let version = probe_bytes(program, &["--version"], cwd, 4096, deadline, cancelled)?;
    if program == "git" {
        return Ok((
            digest(json!([executable, digest(&version)]).to_string().as_bytes()),
            None,
        ));
    }
    let compiler = std::env::var_os("RUSTC")
        .or_else(|| std::env::var_os("CARGO_BUILD_RUSTC"))
        .unwrap_or_else(|| "rustc".into());
    let compiler = PathBuf::from(compiler);
    let compiler = if compiler.is_relative() && compiler.components().count() > 1 {
        cwd.join(compiler)
    } else {
        compiler
    };
    let compiler = compiler.to_str().ok_or("non UTF-8 compiler path")?;
    let rustc = executable_identity(compiler, cancelled)?;
    let rustc_version = probe_bytes(
        compiler,
        &["--version", "--verbose"],
        cwd,
        4096,
        deadline,
        cancelled,
    )?;
    let text = std::str::from_utf8(&rustc_version)?;
    let host = text
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .ok_or("rustc host unavailable")?;
    if !host
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "-_".contains(c))
    {
        return Err("invalid rustc host".into());
    }
    let target = command["args"].as_array().and_then(|args| {
        args.windows(2)
            .find(|pair| pair[0] == "--target")
            .map(|pair| pair[1].clone())
    });
    Ok((
        digest(
            json!([
                executable,
                digest(&version),
                rustc,
                digest(&rustc_version),
                host,
                target
            ])
            .to_string()
            .as_bytes(),
        ),
        Some(host.to_string()),
    ))
}

fn environment(command: &Value, host: Option<&str>) -> (String, Vec<String>) {
    let cargo = command["program"] == "cargo";
    let mut facts = BTreeMap::new();
    let mut unsupported = Vec::new();
    // Never enumerate environment entries: even unused values may contain credentials.
    for name in if cargo {
        CARGO_ENVIRONMENT
    } else {
        GIT_ENVIRONMENT
    } {
        facts.insert(
            name.to_string(),
            std::env::var_os(name).map(|value| digest(value.to_string_lossy().as_bytes())),
        );
    }
    if cargo {
        let mut targets = Vec::new();
        if let Some(host) = host {
            targets.push(host.to_string());
        }
        if let Some(target) = std::env::var_os("CARGO_BUILD_TARGET") {
            targets.push(target.to_string_lossy().into_owned());
        }
        if let Some(args) = command["args"].as_array() {
            for pair in args.windows(2) {
                if pair[0] == "--target" {
                    if let Some(target) = pair[1].as_str() {
                        targets.push(target.to_string());
                    }
                }
            }
        }
        targets.sort();
        targets.dedup();
        for target in targets {
            if target.len() > 128
                || !target
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
            {
                unsupported.push("opaque_build_inputs".to_string());
                continue;
            }
            let target = target.to_ascii_uppercase().replace('-', "_");
            for suffix in ["LINKER", "RUSTFLAGS"] {
                let name = format!("CARGO_TARGET_{target}_{suffix}");
                // A target supplied by argv/environment must never turn into a credential variable name.
                if name.split('_').any(|part| {
                    matches!(
                        part,
                        "TOKEN" | "KEY" | "SECRET" | "PASSWORD" | "CREDENTIAL" | "APIKEY"
                    )
                }) {
                    unsupported.push("opaque_build_inputs".to_string());
                    continue;
                }
                facts.insert(
                    name.clone(),
                    std::env::var_os(name).map(|value| digest(value.to_string_lossy().as_bytes())),
                );
            }
        }
    } else if [
        "GIT_EXTERNAL_DIFF",
        "GIT_EXEC_PATH",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_NAMESPACE",
    ]
    .iter()
    .any(|name| facts.get(*name).is_some_and(Option::is_some))
    {
        unsupported.push("opaque_git_inputs".to_string());
    }
    (digest(json!(facts).to_string().as_bytes()), unsupported)
}

fn cargo_boundary(
    root: &ProjectRoot,
    command: &Value,
    cwd: &Path,
    source: &Snapshot,
    deadline: Instant,
    cancelled: &dyn Fn() -> bool,
) -> VResult<(String, Vec<String>)> {
    // --no-deps avoids resolving/downloading or traversing external dependency contents.
    let mut args = vec![
        "metadata",
        "--format-version",
        "1",
        "--no-deps",
        "--locked",
        "--offline",
    ];
    if let Some(argv) = command["args"].as_array() {
        let mut position = 1;
        while position < argv.len() {
            match argv[position].as_str() {
                Some("--all-features" | "--no-default-features") => {
                    args.push(argv[position].as_str().ok_or("invalid feature flag")?)
                }
                Some("--features") => {
                    args.push("--features");
                    position += 1;
                    args.push(
                        argv.get(position)
                            .and_then(Value::as_str)
                            .ok_or("missing features")?,
                    );
                }
                _ => {}
            }
            position += 1;
        }
    }
    let bytes = probe_bytes("cargo", &args, cwd, 1024 * 1024, deadline, cancelled)?;
    let metadata: Value = serde_json::from_slice(&bytes)?;
    if metadata["version"] != 1 {
        return Err("unsupported Cargo metadata version".into());
    }
    let packages = metadata["packages"]
        .as_array()
        .ok_or("Cargo packages unavailable")?;
    if packages.is_empty() || packages.len() > 512 {
        return Err("Cargo metadata package limit".into());
    }
    let mut risks = Vec::new();
    let mut facts = Vec::new();
    for package in packages {
        active(cancelled)?;
        let manifest = Path::new(
            package["manifest_path"]
                .as_str()
                .ok_or("manifest path unavailable")?,
        );
        if !source.stamps.contains_key(manifest) {
            risks.push("external_path_dependency".to_string());
        }
        let targets = package["targets"].as_array().ok_or("targets unavailable")?;
        let opaque = targets.iter().any(|target| {
            target["kind"]
                .as_array()
                .is_some_and(|kinds| kinds.iter().any(|kind| kind == "custom-build"))
        });
        if opaque {
            risks.push("opaque_build_inputs".to_string());
        }
        facts.push(json!([
            digest(manifest.to_string_lossy().as_bytes()),
            opaque
        ]));
        let dependencies = package["dependencies"]
            .as_array()
            .ok_or("dependencies unavailable")?;
        if dependencies.len() > 2048 {
            return Err("Cargo dependency count limit".into());
        }
        for dependency in dependencies {
            if let Some(path) = dependency["path"].as_str() {
                let path = Path::new(path);
                let inside = path
                    .strip_prefix(root.path())
                    .ok()
                    .and_then(|relative| relative.to_str())
                    .and_then(|relative| root.resolve_existing(relative).ok());
                if let Some(inside) = inside {
                    if !source.stamps.contains_key(&inside.join("Cargo.toml")) {
                        risks.push("dependency_inputs_untracked".to_string());
                    }
                } else {
                    risks.push("external_path_dependency".to_string());
                }
                facts.push(json!(["path", digest(path.to_string_lossy().as_bytes())]));
            } else {
                // Registry/Git sources and their transitive build steps are outside the Project snapshot.
                risks.push("dependency_inputs_untracked".to_string());
                facts.push(json!(["external_dependency"]));
            }
        }
    }
    facts.sort_by_key(Value::to_string);
    risks.sort();
    risks.dedup();
    Ok((digest(json!(facts).to_string().as_bytes()), risks))
}

fn cargo_config(
    root: &ProjectRoot,
    cwd: &Path,
    source: &Snapshot,
) -> VResult<(String, Vec<String>)> {
    let mut unsupported = vec!["compiler_input_closure_unproven".to_string()];
    if source
        .stamps
        .keys()
        .any(|path| path.file_name().is_some_and(|name| name == "build.rs"))
    {
        unsupported.push("opaque_build_inputs".to_string());
    }
    let mut external = Vec::new();
    // Inside-project manifests, lockfiles and Cargo configuration are already content-hashed by source.
    for ancestor in cwd.ancestors() {
        if ancestor.starts_with(root.path()) {
            continue;
        }
        for name in [".cargo/config", ".cargo/config.toml", "Cargo.toml"] {
            let path = ancestor.join(name);
            match fs::symlink_metadata(&path) {
                Ok(_) => {
                    external.push(digest(path.to_string_lossy().as_bytes()));
                    unsupported.push("external_cargo_config".to_string());
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    let home = std::env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cargo")))
        .ok_or("Cargo home unknown")?;
    for name in ["config", "config.toml"] {
        match fs::symlink_metadata(home.join(name)) {
            Ok(_) => {
                external.push(name.to_string());
                unsupported.push("external_cargo_config".to_string());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    unsupported.sort();
    unsupported.dedup();
    // Global config may contain credentials; its values are neither copied nor hashed.
    Ok((digest(json!(external).to_string().as_bytes()), unsupported))
}

fn git_state(
    root: &ProjectRoot,
    cwd: &Path,
    deadline: Instant,
    cancelled: &dyn Fn() -> bool,
) -> VResult<(String, String, Vec<String>, String)> {
    let top = probe_bytes(
        "git",
        &["rev-parse", "--show-toplevel"],
        cwd,
        4096,
        deadline,
        cancelled,
    )?;
    let top = PathBuf::from(std::str::from_utf8(&top)?.trim()).canonicalize()?;
    if top != root.path() {
        return Err("Git repository root differs from Project".into());
    }
    let repository = probe_bytes(
        "git",
        &["rev-parse", "--absolute-git-dir", "--show-object-format"],
        cwd,
        4096,
        deadline,
        cancelled,
    )?;
    let index_path = probe_bytes(
        "git",
        &["rev-parse", "--git-path", "index"],
        cwd,
        4096,
        deadline,
        cancelled,
    )?;
    let index_path = PathBuf::from(std::str::from_utf8(&index_path)?.trim());
    let index_path = if index_path.is_absolute() {
        index_path
    } else {
        cwd.join(index_path)
    };
    let index_stamp = crate::context::fulltext::metadata_stamp(&fs::metadata(&index_path)?);
    let head = probe_bytes(
        "git",
        &["rev-parse", "--verify", "HEAD"],
        cwd,
        4096,
        deadline,
        cancelled,
    )?;
    // Discover unknown configuration names without copying potentially credential-bearing values.
    let names = probe(
        "git",
        &[
            "config",
            "--null",
            "--name-only",
            "--get-regexp",
            "^(core\\.|diff\\.|filter\\.|extensions\\.)",
        ],
        cwd,
        64 * 1024,
        deadline,
        cancelled,
    )?;
    if !names.success && names.exit != crate::process::ProcessExit::Code(1) {
        return Err("Git config names unavailable".into());
    }
    let safe = [
        "core.whitespace",
        "core.autocrlf",
        "core.safecrlf",
        "core.eol",
        "core.filemode",
        "core.symlinks",
        "core.ignorecase",
        "core.precomposeunicode",
        "diff.algorithm",
        "diff.renames",
        "diff.relative",
        "diff.ignoresubmodules",
        "diff.mnemonicprefix",
        "diff.noprefix",
        "diff.context",
        "core.repositoryformatversion",
        "extensions.worktreeconfig",
        "extensions.objectformat",
        "extensions.refstorage",
    ];
    let ignored = [
        "core.bare",
        "core.logallrefupdates",
        "core.pager",
        "core.editor",
        "core.quotepath",
        "core.attributesfile",
        "core.excludesfile",
    ];
    let mut unsupported = Vec::new();
    for name in names
        .stdout
        .split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
    {
        let name = std::str::from_utf8(name)?;
        if !safe.contains(&name) && !ignored.contains(&name) {
            unsupported.push("opaque_git_inputs".to_string());
        }
    }
    let pattern = format!(
        "^({})$",
        safe.iter()
            .map(|name| name.replace('.', "\\."))
            .collect::<Vec<_>>()
            .join("|")
    );
    let config = probe(
        "git",
        &["config", "--null", "--get-regexp", &pattern],
        cwd,
        64 * 1024,
        deadline,
        cancelled,
    )?;
    if !config.success && config.exit != crate::process::ProcessExit::Code(1) {
        return Err("Git config probe failed".into());
    }
    let mut relevant = BTreeMap::new();
    for entry in config
        .stdout
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
    {
        let (name, value) = std::str::from_utf8(entry)?
            .split_once('\n')
            .ok_or("invalid Git config record")?;
        if !safe.contains(&name) {
            return Err("unexpected Git config input".into());
        }
        relevant.insert(name, digest(value.as_bytes()));
    }
    let index = probe_bytes(
        "git",
        &[
            "-c",
            "core.fsmonitor=false",
            "ls-files",
            "--stage",
            "-v",
            "-z",
        ],
        root.path(),
        4 * 1024 * 1024,
        deadline,
        cancelled,
    )?;
    let paths = probe_bytes(
        "git",
        &["-c", "core.fsmonitor=false", "ls-files", "--cached", "-z"],
        root.path(),
        1024 * 1024,
        deadline,
        cancelled,
    )?;
    let paths: Vec<&str> = paths
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(std::str::from_utf8)
        .collect::<Result<_, _>>()?;
    if paths.len() > MAX_FILES {
        return Err("Git path count exceeds bounds".into());
    }
    let mut attributes = Vec::new();
    for chunk in paths.chunks(128) {
        let mut args = vec!["check-attr", "--all", "-z", "--"];
        args.extend_from_slice(chunk);
        let output = probe_bytes("git", &args, root.path(), 1024 * 1024, deadline, cancelled)?;
        let parts: Vec<&[u8]> = output
            .split(|byte| *byte == 0)
            .filter(|part| !part.is_empty())
            .collect();
        for entry in parts.chunks_exact(3) {
            if matches!(entry[1], b"filter" | b"diff" | b"working-tree-encoding")
                && !matches!(entry[2], b"unset" | b"unspecified")
            {
                unsupported.push("opaque_git_inputs".to_string());
            }
        }
        attributes.push(digest(&output));
    }
    unsupported.sort();
    unsupported.dedup();
    let config = digest(json!([relevant, attributes]).to_string().as_bytes());
    // Never invoke configured filters/textconv while merely checking applicability.
    let worktree = if unsupported.is_empty() {
        Some(digest(&probe_bytes(
            "git",
            &[
                "-c",
                "core.fsmonitor=false",
                "--no-pager",
                "diff",
                "--no-ext-diff",
                "--no-textconv",
                "--binary",
                "--full-index",
                "--no-color",
                "--no-renames",
                "--no-relative",
                "--ignore-submodules=none",
            ],
            root.path(),
            4 * 1024 * 1024,
            deadline,
            cancelled,
        )?))
    } else {
        None
    };
    let staged = if unsupported.is_empty() {
        Some(digest(&probe_bytes(
            "git",
            &[
                "-c",
                "core.fsmonitor=false",
                "--no-pager",
                "diff",
                "--cached",
                "--no-ext-diff",
                "--no-textconv",
                "--binary",
                "--full-index",
                "--no-color",
                "--no-renames",
                "--no-relative",
                "--ignore-submodules=none",
            ],
            root.path(),
            4 * 1024 * 1024,
            deadline,
            cancelled,
        )?))
    } else {
        None
    };
    let head_after = probe_bytes(
        "git",
        &["rev-parse", "--verify", "HEAD"],
        cwd,
        4096,
        deadline,
        cancelled,
    )?;
    let index_after = probe_bytes(
        "git",
        &[
            "-c",
            "core.fsmonitor=false",
            "ls-files",
            "--stage",
            "-v",
            "-z",
        ],
        root.path(),
        4 * 1024 * 1024,
        deadline,
        cancelled,
    )?;
    let index_stamp_after = crate::context::fulltext::metadata_stamp(&fs::metadata(&index_path)?);
    if head != head_after || index != index_after || index_stamp != index_stamp_after {
        return Err("Git state changed during fingerprint".into());
    }
    Ok((
        digest(
            json!([repository, head, index, worktree, staged])
                .to_string()
                .as_bytes(),
        ),
        config,
        unsupported,
        index_stamp_after,
    ))
}

fn inputs(
    root: &ProjectRoot,
    command: &Value,
    source: &Snapshot,
    cancelled: &dyn Fn() -> bool,
    deadline: Instant,
) -> ValidationInputs {
    let cwd = root.path().join(command["cwd"].as_str().unwrap_or("."));
    let (environment_fingerprint, unsupported_inputs) = environment(command, None);
    let mut result = ValidationInputs {
        version: INPUT_MODEL_VERSION,
        toolchain: None,
        environment: environment_fingerprint,
        config: None,
        git_state: None,
        dependency_boundary: None,
        unsupported_inputs,
        unknown: false,
        toolchain_ms: 0,
        git_state_ms: 0,
        dependency_boundary_ms: 0,
        git_index_stamp: None,
    };
    if Instant::now() >= deadline {
        result.unknown = true;
        return result;
    }
    let started = Instant::now();
    match toolchain(command, &cwd, deadline, cancelled) {
        Ok((value, host)) => {
            result.toolchain = Some(value);
            let (environment, risks) = environment(command, host.as_deref());
            result.environment = environment;
            result.unsupported_inputs.extend(risks);
        }
        Err(_) => result.unknown = true,
    }
    result.toolchain_ms = started.elapsed().as_millis();
    if command["program"] == "cargo" {
        let started = Instant::now();
        match cargo_boundary(root, command, &cwd, source, deadline, cancelled) {
            Ok((boundary, risks)) => {
                result.dependency_boundary = Some(boundary);
                result.unsupported_inputs.extend(risks);
            }
            Err(_) => result.unknown = true,
        }
        result.dependency_boundary_ms = started.elapsed().as_millis();
        match cargo_config(root, &cwd, source) {
            Ok((config, risks)) => {
                result.config = Some(config);
                result.unsupported_inputs.extend(risks);
            }
            Err(_) => result.unknown = true,
        }
    } else {
        let started = Instant::now();
        if result.unsupported_inputs.is_empty() {
            match git_state(root, &cwd, deadline, cancelled) {
                Ok((state, config, risks, index_stamp)) => {
                    result.git_index_stamp = Some(index_stamp);
                    result.git_state = Some(state);
                    result.config = Some(config);
                    result.unsupported_inputs.extend(risks);
                }
                Err(_) => result.unknown = true,
            }
        }
        result.git_state_ms = started.elapsed().as_millis();
    }
    if command["program"] == "cargo" && command["args"][0] == "build" {
        result
            .unsupported_inputs
            .push("opaque_build_inputs".to_string());
    }
    result.unsupported_inputs.sort();
    result.unsupported_inputs.dedup();
    result
}

fn applicability(
    evidence: &Value,
    current: &Snapshot,
    observed: &ValidationInputs,
    current_command: Option<&Value>,
) -> (&'static str, Vec<String>) {
    let unavailable = || {
        (
            "unverifiable",
            vec!["applicability_unavailable".to_string()],
        )
    };
    let Some(revision) = evidence["project_revision"].as_str() else {
        return unavailable();
    };
    if revision != current.revision {
        return ("stale", vec!["source_changed".to_string()]);
    }
    let stored =
        match serde_json::from_value::<ValidationInputs>(evidence["validation_inputs"].clone()) {
            Ok(inputs)
                if evidence["input_model_version"] == INPUT_MODEL_VERSION
                    && inputs.version == INPUT_MODEL_VERSION =>
            {
                inputs
            }
            _ => return unavailable(),
        };
    if evidence["validation_input_fingerprint"]
        != validation_fingerprint(&evidence["command"], revision, &stored)
    {
        return unavailable();
    }
    let mut changed = Vec::new();
    if current_command.is_some_and(|command| evidence["command"] != *command) {
        changed.push("command_changed".to_string());
    }
    if stored.toolchain != observed.toolchain
        && stored.toolchain.is_some()
        && observed.toolchain.is_some()
    {
        changed.push("toolchain_changed".to_string());
    }
    if !observed.environment.is_empty() && stored.environment != observed.environment {
        changed.push("environment_changed".to_string());
    }
    if evidence["command"]["program"] == "git"
        && ((stored.git_state != observed.git_state
            && stored.git_state.is_some()
            && observed.git_state.is_some())
            || (stored.config != observed.config
                && stored.config.is_some()
                && observed.config.is_some()))
    {
        changed.push("git_state_changed".to_string());
    } else if evidence["command"]["program"] == "cargo"
        && stored.config != observed.config
        && stored.config.is_some()
        && observed.config.is_some()
    {
        changed.push("configuration_changed".to_string());
    }
    if stored.dependency_boundary != observed.dependency_boundary
        && stored.dependency_boundary.is_some()
        && observed.dependency_boundary.is_some()
    {
        changed.push("dependency_boundary_changed".to_string());
    }
    if !changed.is_empty() {
        return ("stale", changed);
    }
    let mut reasons = stored
        .unsupported_inputs
        .iter()
        .chain(&observed.unsupported_inputs)
        .cloned()
        .collect::<Vec<_>>();
    if stored.unknown
        || observed.unknown
        || evidence["stable_during_execution"] != true
        || current_command.is_none()
        || stored.toolchain.is_none()
        || observed.toolchain.is_none()
        || stored.config.is_none()
        || observed.config.is_none()
        || (evidence["command"]["program"] == "git"
            && (stored.git_state.is_none() || observed.git_state.is_none()))
        || (evidence["command"]["program"] == "cargo"
            && (stored.dependency_boundary.is_none() || observed.dependency_boundary.is_none()))
    {
        reasons.push("applicability_unavailable".to_string());
    }
    reasons.sort();
    reasons.dedup();
    reasons.truncate(16);
    if reasons.is_empty() {
        ("applicable", reasons)
    } else {
        ("unverifiable", reasons)
    }
}

pub(super) struct Observation {
    command: Value,
    snapshot: Snapshot,
    inputs: ValidationInputs,
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
            let snapshot = snapshot(root, cancelled)?;
            let inputs = inputs(
                root,
                &command,
                &snapshot,
                cancelled,
                Instant::now() + Duration::from_secs(3),
            );
            Ok(Self {
                command,
                snapshot,
                inputs,
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
            let after_inputs = inputs(
                root,
                &self.command,
                &after,
                cancelled,
                Instant::now() + Duration::from_secs(3),
            );
            let stable = self.snapshot.revision == after.revision
                && self.inputs.fingerprint() == after_inputs.fingerprint()
                && self.inputs.git_index_stamp == after_inputs.git_index_stamp
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
                self.inputs.fingerprint()
            ]))?);
            let mut evidence = json!({"identity":identity,"project_id":job.project_id,"project_revision":self.snapshot.revision,
                "command":self.command,"environment_fingerprint":self.inputs.environment,"stable_during_execution":stable,
                "input_model_version":INPUT_MODEL_VERSION,"validation_inputs":self.inputs,"validation_input_fingerprint":validation_fingerprint(&self.command,&self.snapshot.revision,&self.inputs),
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

pub(super) fn query_current(
    root: &ProjectRoot,
    arguments: &Value,
    project_id: &str,
    current: &Snapshot,
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
        let mut observations = BTreeMap::new();
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
            if evidence["project_id"] != project_id
                || filter
                    .as_ref()
                    .is_some_and(|filter| evidence["command"] != *filter)
            {
                continue;
            }
            if entries.len() == limit {
                truncated = true;
                break;
            }
            let normalized = command(root, &evidence["command"]).ok();
            let command = normalized.as_ref().unwrap_or(&evidence["command"]).clone();
            let key = command.to_string();
            let unavailable = ValidationInputs::unavailable();
            // A source mismatch already proves staleness. Legacy or malformed
            // records cannot become reusable through additional probes either.
            let observed = if evidence["project_revision"] == current.revision
                && evidence["input_model_version"] == INPUT_MODEL_VERSION
                && normalized.is_some()
            {
                observations.entry(key).or_insert_with(|| {
                    inputs(
                        root,
                        &command,
                        current,
                        cancelled,
                        started + Duration::from_secs(3),
                    )
                })
            } else {
                &unavailable
            };
            let (state, reasons) = applicability(&evidence, current, observed, normalized.as_ref());
            let applies = state == "applicable";
            evidence["applicable"] = json!(applies);
            evidence["reusable"] = json!(applies);
            evidence["stale"] = json!(state == "stale");
            evidence["applicability"] = json!(state);
            evidence["reason"] = json!(reasons.first());
            evidence["reasons"] = json!(reasons);
            evidence["current_validation_input_fingerprint"] = if observed.unknown {
                Value::Null
            } else {
                json!(validation_fingerprint(
                    &command,
                    &current.revision,
                    observed
                ))
            };
            evidence["unsupported_inputs"] = json!(observed.unsupported_inputs);
            budget += evidence.to_string().len();
            if entries.len() == limit || budget > super::TOOL_OUTPUT_CAP / 2 {
                truncated = true;
                break;
            }
            entries.push(evidence);
        }
        // Directory and file stamps protect the query from returning a known raced snapshot.
        current.verify(cancelled)?;
        Ok(ToolResult {
            success: true,
            output: json!({"project_id":project_id,"project_revision":current.revision,"evidence":entries,
            "scope":{"excluded_directories":EXCLUDED,"environment":"explicit per-command allowlist; unproven Cargo compiler/dependency closure is never reusable","reuse":"discovery_only; never automatically skip execution"},"rebuilt":rebuilt}),
            truncated,
            metadata: json!({"elapsed_ms":started.elapsed().as_millis(),"dependency_boundary_ms":observations.values().map(|inputs| inputs.dependency_boundary_ms).sum::<u128>(),"toolchain_fingerprint_ms":observations.values().map(|inputs| inputs.toolchain_ms).sum::<u128>(),"git_state_fingerprint_ms":observations.values().map(|inputs| inputs.git_state_ms).sum::<u128>()}),
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

pub(super) fn query(
    root: &ProjectRoot,
    arguments: &Value,
    cancelled: &dyn Fn() -> bool,
) -> ToolResult {
    let started = Instant::now();
    let query = || -> VResult<ToolResult> {
        let project = DomainRepository::open_existing(root.path())?
            .project_at_root(root.path())?
            .ok_or("Project missing")?;
        let source_started = Instant::now();
        let current = snapshot(root, cancelled)?;
        let source_revision_ms = source_started.elapsed().as_millis();
        let mut result = query_current(root, arguments, &project.id, &current, cancelled);
        result.metadata["source_revision_ms"] = json!(source_revision_ms);
        result.metadata["elapsed_ms"] = json!(started.elapsed().as_millis());
        Ok(result)
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
