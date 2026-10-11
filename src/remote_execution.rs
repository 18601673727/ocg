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
    /// The confinement program that launches the helper.
    program: PathBuf,
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
            program: PathBuf::from("/usr/bin/bwrap"),
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
        // A confinement program that is absent was never launched, so no
        // helper ran and the failure is definite.
        if !self.program.is_file() {
            return Err(OcgError::config("confinement program is unavailable"));
        }
        // From here the helper may have started and acted. A failure it does
        // not report as a tool result leaves the tool's effect unknown, which
        // is what it is reported as: never a failure that proves no effect.
        let indeterminate = |message: &str| {
            Ok(ToolResult::failure(crate::native_tools::ToolError::new(
                crate::native_tools::ToolErrorKind::Indeterminate,
                message,
            )))
        };
        let output = match crate::process::run_owned_input(
            &self.program.to_string_lossy(),
            &arguments,
            root,
            crate::native_tools::TOOL_OUTPUT_CAP,
            cancelled,
            Some(&input),
        ) {
            Ok(output) => output,
            Err(error) => return indeterminate(&format!("confined helper failed: {error}")),
        };
        if cancelled() || output.termination != crate::process::CommandTermination::Completed {
            return indeterminate("confined tool cancelled or exceeded deadline");
        }
        if !output.success || output.truncated() {
            return indeterminate("confined native tool helper failed");
        }
        match serde_json::from_slice(&output.stdout) {
            Ok(result) => Ok(result),
            Err(_) => indeterminate("invalid confined tool result"),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_tools::{PermissionPolicy, ToolError, ToolErrorKind, ToolResult};
    use serde_json::json;

    struct Helper {
        _dir: tempfile::TempDir,
        workspace: PathBuf,
        root: PathBuf,
    }

    impl Helper {
        fn new() -> Self {
            let dir = tempfile::tempdir().expect("workspace");
            let workspace = dir.path().canonicalize().expect("canonical workspace");
            let root = workspace.join("project");
            std::fs::create_dir_all(root.join(".ocg")).expect("Project state");
            Self {
                _dir: dir,
                workspace,
                root,
            }
        }

        /// A stand-in for the confinement program: it consumes the tool input
        /// it is given, then runs `script`.
        fn execution(&self, script: &str) -> RemoteExecution {
            let program = self._dir.path().join("stub-bwrap");
            std::fs::write(&program, format!("#!/bin/sh\ncat > /dev/null\n{script}\n"))
                .expect("stub program");
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755))
                    .expect("executable stub");
            }
            self.with_program(program)
        }

        fn with_program(&self, program: PathBuf) -> RemoteExecution {
            RemoteExecution {
                operator_id: "operator".into(),
                workspace: self.workspace.clone(),
                helper: self.workspace.join("ocg-native-tool"),
                runtime: None,
                program,
            }
        }

        fn run(&self, remote: &RemoteExecution) -> Result<ToolResult> {
            remote.execute(
                &self.root,
                "filesystem.read",
                &json!({"path": "notes.txt"}),
                PermissionPolicy::allow_all(),
                &|| false,
            )
        }
    }

    fn reported(result: &ToolResult) -> String {
        serde_json::to_string(result).expect("tool result JSON")
    }

    #[test]
    fn helper_that_exits_after_starting_leaves_its_effect_unknown() {
        let helper = Helper::new();
        let remote = helper.execution("exit 7");

        let result = helper.run(&remote).expect("settled outcome");

        assert!(!result.success);
        let error = result.error.expect("unknown outcome");
        assert_eq!(error.kind, ToolErrorKind::Indeterminate);
        assert_eq!(error.message, "confined native tool helper failed");
    }

    #[test]
    fn helper_output_that_is_not_a_tool_result_leaves_its_effect_unknown() {
        let helper = Helper::new();
        let remote = helper.execution("printf 'not a result'");

        let error = helper
            .run(&remote)
            .expect("settled outcome")
            .error
            .expect("unknown outcome");

        assert_eq!(error.kind, ToolErrorKind::Indeterminate);
        assert_eq!(error.message, "invalid confined tool result");
    }

    #[test]
    fn helper_reported_failure_is_a_confirmed_definite_failure() {
        let helper = Helper::new();
        let failure = ToolResult::failure(ToolError::new(
            ToolErrorKind::ExecutionFailure,
            "file is missing",
        ));
        let remote = helper.execution(&format!("printf '%s' '{}'", reported(&failure)));

        let result = helper.run(&remote).expect("reported outcome");

        assert!(!result.success);
        let error = result.error.expect("confirmed failure");
        assert_eq!(error.kind, ToolErrorKind::ExecutionFailure);
        assert_eq!(error.message, "file is missing");
    }

    #[test]
    fn helper_reported_success_is_a_confirmed_success() {
        let helper = Helper::new();
        let success = ToolResult::success(json!({"content": "hello"}));
        let remote = helper.execution(&format!("printf '%s' '{}'", reported(&success)));

        let result = helper.run(&remote).expect("reported outcome");

        assert!(result.success);
        assert_eq!(result.output["content"], "hello");
    }

    #[test]
    fn helper_that_never_launches_is_not_reported_as_an_unknown_effect() {
        let helper = Helper::new();
        let remote = helper.with_program(helper._dir.path().join("missing-bwrap"));

        // Nothing ran, so the outcome is definite: an Err, or a failure that is
        // not Indeterminate.
        if let Ok(result) = helper.run(&remote) {
            assert!(!matches!(
                result.error.map(|error| error.kind),
                Some(ToolErrorKind::Indeterminate)
            ));
        }
    }
}
