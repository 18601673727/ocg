use crate::error::{OcgError, Result};
use crate::native_tools::{NativeToolExecutor, NativeToolRegistry, PermissionPolicy, ToolResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

#[derive(Debug, Clone)]
pub(crate) struct RemoteExecution {
    pub operator_id: String,
    workspace: PathBuf,
    helper: PathBuf,
    runtime: Option<PathBuf>,
}

fn required(name: &str) -> Result<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| OcgError::config(format!("{name} is required")))
}

impl RemoteExecution {
    pub fn from_env() -> Result<Option<Self>> {
        match std::env::var("OCG_REMOTE_EXECUTION").as_deref() {
            Err(std::env::VarError::NotPresent) | Ok("disabled") => return Ok(None),
            Ok("single-operator") => {}
            _ => return Err(OcgError::config("unsupported remote execution mode")),
        }
        if std::env::var("OCG_AUTH_MODE").as_deref() != Ok("cloudflare-access") {
            return Err(OcgError::config(
                "remote execution requires Access authentication",
            ));
        }
        let subject = required("OCG_REMOTE_OPERATOR_SUBJECT")?;
        if subject.trim() != subject || subject.len() > 256 {
            return Err(OcgError::config("invalid remote operator subject"));
        }
        let workspace = PathBuf::from(required("OCG_REMOTE_WORKSPACE")?)
            .canonicalize()
            .map_err(|error| OcgError::io("resolve authorized workspace", error))?;
        let helper = PathBuf::from(required("OCG_NATIVE_TOOL_HELPER")?)
            .canonicalize()
            .map_err(|error| OcgError::io("resolve native tool helper", error))?;
        validate_host(&workspace, &helper)?;
        let runtime = std::env::var_os("OCG_NATIVE_RUNTIME")
            .map(PathBuf::from)
            .map(|path| {
                path.canonicalize()
                    .map_err(|error| OcgError::io("resolve native runtime", error))
            })
            .transpose()?;
        if let Some(path) = &runtime {
            if !path.is_dir() || path.starts_with(&workspace) {
                return Err(OcgError::config(
                    "native runtime must be an administrator-owned directory outside the workspace",
                ));
            }
            validate_host(path, &helper)?;
        }
        Ok(Some(Self {
            operator_id: crate::control_security::user_id(
                &required("OCG_ACCESS_ISSUER")?,
                &subject,
            ),
            workspace,
            helper,
            runtime,
        }))
    }

    pub fn accepts_root(&self, root: &Path) -> Result<()> {
        let resolved = root
            .canonicalize()
            .map_err(|error| OcgError::io("resolve remote Project", error))?;
        // Only direct children of an administrator-owned directory are eligible.
        // Native tools cannot replace the root path that the next bind will use.
        if resolved.parent() != Some(self.workspace.as_path())
            || resolved != root
            || !resolved.is_dir()
        {
            return Err(OcgError::config(
                "Project is outside the authorized workspace",
            ));
        }
        let state = root.join(".ocg");
        if state.canonicalize().ok().as_ref() != Some(&state) || !state.is_dir() {
            return Err(OcgError::config(
                "remote Project needs an ordinary .ocg directory",
            ));
        }
        let locks = state.join("edit-locks");
        std::fs::create_dir_all(&locks)
            .map_err(|error| OcgError::io("prepare confined edit locks", error))?;
        if locks.canonicalize().ok().as_ref() != Some(&locks) || !locks.is_dir() {
            return Err(OcgError::config(
                "remote edit locks must be an ordinary directory",
            ));
        }
        Ok(())
    }

    pub fn accepts_storage(&self, path: &Path) -> Result<()> {
        let parent_in_workspace = path
            .ancestors()
            .find(|ancestor| ancestor.exists())
            .is_some_and(|ancestor| {
                crate::project::canonicalize(ancestor).starts_with(&self.workspace)
            });
        let resolved = crate::project::canonicalize(path);
        let in_runtime = self.runtime.as_ref().is_some_and(|runtime| {
            resolved.starts_with(runtime)
                || path
                    .ancestors()
                    .find(|ancestor| ancestor.exists())
                    .is_some_and(|ancestor| {
                        crate::project::canonicalize(ancestor).starts_with(runtime)
                    })
        });
        if resolved.starts_with(&self.workspace) || parent_in_workspace || in_runtime {
            return Err(OcgError::config(
                "service credentials and control state must be outside the authorized workspace",
            ));
        }
        Ok(())
    }

    fn arguments(&self, root: &Path) -> Vec<String> {
        let mut args: Vec<String> = [
            "--unshare-all",
            "--die-with-parent",
            "--new-session",
            "--cap-drop",
            "ALL",
            "--clearenv",
            "--setenv",
            "PATH",
            "/runtime/bin:/usr/bin:/bin",
            "--setenv",
            "TMPDIR",
            "/tmp",
            "--setenv",
            "LANG",
            "C.UTF-8",
            "--proc",
            "/proc",
            "--dev",
            "/dev",
            "--tmpfs",
            "/tmp",
            "--ro-bind",
            "/usr/bin",
            "/usr/bin",
            "--ro-bind",
            "/usr/lib",
            "/usr/lib",
            "--symlink",
            "usr/bin",
            "/bin",
            "--symlink",
            "usr/lib",
            "/lib",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        if Path::new("/usr/lib64").is_dir() {
            args.extend(
                [
                    "--ro-bind",
                    "/usr/lib64",
                    "/usr/lib64",
                    "--symlink",
                    "usr/lib64",
                    "/lib64",
                ]
                .map(str::to_string),
            );
        }
        if let Some(runtime) = &self.runtime {
            args.extend([
                "--ro-bind".into(),
                runtime.to_string_lossy().into_owned(),
                "/runtime".into(),
                "--setenv".into(),
                "RUSTUP_HOME".into(),
                "/runtime/rustup".into(),
                "--setenv".into(),
                "CARGO_HOME".into(),
                root.join(".cache/cargo").to_string_lossy().into_owned(),
                "--setenv".into(),
                "CARGO_NET_OFFLINE".into(),
                "true".into(),
            ]);
        }
        args.extend([
            "--setenv".into(),
            "OCG_NATIVE_CONFINED".into(),
            "1".into(),
            "--setenv".into(),
            "HOME".into(),
            root.to_string_lossy().into_owned(),
            "--bind".into(),
            root.to_string_lossy().into_owned(),
            root.to_string_lossy().into_owned(),
            "--ro-bind".into(),
            root.join(".ocg").to_string_lossy().into_owned(),
            root.join(".ocg").to_string_lossy().into_owned(),
            "--bind".into(),
            root.join(".ocg/edit-locks").to_string_lossy().into_owned(),
            root.join(".ocg/edit-locks").to_string_lossy().into_owned(),
            "--ro-bind".into(),
            self.helper.to_string_lossy().into_owned(),
            "/ocg-native-tool".into(),
            "--chdir".into(),
            root.to_string_lossy().into_owned(),
            "--".into(),
            "/ocg-native-tool".into(),
            root.to_string_lossy().into_owned(),
        ]);
        args
    }

    pub fn execute(
        &self,
        root: &Path,
        name: &str,
        arguments: &Value,
        policy: PermissionPolicy,
        cancelled: &dyn Fn() -> bool,
    ) -> Result<ToolResult> {
        self.accepts_root(root)?;
        let input = serde_json::to_vec(&ToolInput {
            name: name.into(),
            arguments: arguments.clone(),
            policy: [
                policy.read_only,
                policy.filesystem_write,
                policy.process_exec,
                policy.filesystem_capability,
                policy.process_capability,
            ],
        })
        .map_err(|error| OcgError::config(format!("encode confined tool: {error}")))?;
        if input.len() > 256 * 1024 {
            return Err(OcgError::config("confined tool input exceeds limit"));
        }
        let snapshot = if matches!(name, "context.snapshot" | "context.validation") {
            let directory = tempfile::tempdir()
                .map_err(|error| OcgError::io("prepare confined context snapshot", error))?;
            crate::orchestration::domain::DomainRepository::write_read_snapshot(
                root,
                &directory.path().join("domain.sqlite3"),
            )?;
            Some(directory)
        } else {
            None
        };
        let mut arguments = self.arguments(root);
        if let Some(directory) = &snapshot {
            let at = arguments
                .iter()
                .position(|argument| argument == "--")
                .ok_or_else(|| OcgError::config("invalid confinement arguments"))?;
            arguments.splice(
                at..at,
                [
                    "--ro-bind".into(),
                    directory
                        .path()
                        .join("domain.sqlite3")
                        .to_string_lossy()
                        .into_owned(),
                    "/tmp/domain.sqlite3".into(),
                    "--setenv".into(),
                    "OCG_NATIVE_DOMAIN_SNAPSHOT".into(),
                    "/tmp/domain.sqlite3".into(),
                ],
            );
        }
        let output = crate::process::run_owned_input(
            "/usr/bin/bwrap",
            &arguments,
            root,
            crate::native_tools::TOOL_OUTPUT_CAP,
            cancelled,
            Some(&input),
        )?;
        if cancelled() || output.termination != crate::process::CommandTermination::Completed {
            return Err(OcgError::config(
                "confined tool cancelled or exceeded deadline",
            ));
        }
        if !output.success || output.truncated() {
            return Err(OcgError::config("confined native tool helper failed"));
        }
        serde_json::from_slice(&output.stdout)
            .map_err(|_| OcgError::config("invalid confined tool result"))
    }

    pub fn verify(&self, root: &Path) -> Result<()> {
        let result = self.execute(
            root,
            "process.exec",
            &serde_json::json!({"program":"/usr/bin/true","args":[]}),
            PermissionPolicy::allow_all(),
            &|| false,
        )?;
        if !result.success {
            return Err(OcgError::config("native confinement probe failed"));
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn validate_host(workspace: &Path, helper: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    if rustix::process::geteuid().is_root() || rustix::process::getuid().is_root() {
        return Err(OcgError::config("remote execution refuses root"));
    }
    let status = std::fs::read_to_string("/proc/self/status")
        .map_err(|error| OcgError::io("inspect service restrictions", error))?;
    if !status.lines().any(|line| line == "NoNewPrivs:\t1")
        || !status
            .lines()
            .any(|line| line == "CapEff:\t0000000000000000")
    {
        return Err(OcgError::config(
            "remote execution requires no new privileges and empty capabilities",
        ));
    }
    for path in [
        helper,
        Path::new("/usr/bin/bwrap"),
        Path::new("/usr/bin"),
        Path::new("/usr/lib"),
        workspace,
    ] {
        for ancestor in path.ancestors() {
            let metadata = std::fs::metadata(ancestor)
                .map_err(|error| OcgError::io("inspect confinement path", error))?;
            if metadata.uid() != 0 || metadata.mode() & 0o022 != 0 {
                return Err(OcgError::config(
                    "confinement paths must be administrator-owned and immutable to the service",
                ));
            }
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn validate_host(_workspace: &Path, _helper: &Path) -> Result<()> {
    Err(OcgError::config("remote native confinement requires Linux"))
}

#[derive(Serialize, Deserialize)]
struct ToolInput {
    name: String,
    arguments: Value,
    policy: [bool; 5],
}

pub(crate) fn native_confined() -> bool {
    std::env::var("OCG_NATIVE_CONFINED").as_deref() == Ok("1")
}

pub fn run_native_tool_helper() -> Result<()> {
    let mut bytes = Vec::new();
    std::io::stdin()
        .take(256 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| OcgError::io("read confined tool", error))?;
    if bytes.len() > 256 * 1024 {
        return Err(OcgError::config("tool input exceeds limit"));
    }
    let input: ToolInput =
        serde_json::from_slice(&bytes).map_err(|_| OcgError::config("invalid tool input"))?;
    let definition = NativeToolRegistry::get(&input.name)
        .ok_or_else(|| OcgError::config("unknown native tool"))?;
    let [read_only, filesystem_write, process_exec, filesystem_capability, process_capability] =
        input.policy;
    let root = std::env::args_os()
        .nth(1)
        .ok_or_else(|| OcgError::config("confined Project root is required"))?;
    let executor = NativeToolExecutor::new(Path::new(&root))?;
    let result = executor.execute(
        &input.name,
        &input.arguments,
        definition.permission,
        PermissionPolicy {
            read_only,
            filesystem_write,
            process_exec,
            filesystem_capability,
            process_capability,
        },
        &AtomicBool::new(false),
    );
    let output = serde_json::to_vec(&result)
        .map_err(|error| OcgError::config(format!("encode tool result: {error}")))?;
    std::io::stdout()
        .write_all(&output)
        .map_err(|error| OcgError::io("write tool result", error))
}
