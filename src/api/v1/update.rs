use super::contracts::envelope;
use super::*;

pub(super) const UPDATE_BODY_LIMIT: usize = 64;

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Update")]
pub(crate) struct UpdateView {
    running: String,
    latest: Option<String>,
    available: bool,
    latest_error: Option<String>,
    can_update: bool,
    reason: Option<String>,
    log_path: Option<String>,
    #[cfg(unix)]
    state: Option<crate::system::update::State>,
    #[cfg(not(unix))]
    state: Option<serde_json::Value>,
}

#[cfg(unix)]
fn inspect() -> (
    bool,
    Option<String>,
    Option<crate::system::update::State>,
    Option<String>,
) {
    use crate::system::update::{self, Phase};
    let Ok(install) = update::install() else {
        return (false, Some("not_installed".into()), None, None);
    };
    let service = std::env::var_os("HOME")
        .map(|home| update::service_file(std::path::Path::new(&home)))
        .is_some_and(|path| path.is_file());
    let path = install.prefix.join("update.log").display().to_string();
    let mut state = update::read_state(&install.prefix);
    if let Some(state) = state.as_mut()
        && matches!(state.phase, Phase::Waiting | Phase::Installing)
        && update::now_ms().saturating_sub(state.started_at) > 15_000
        && update::lock(&install.prefix).is_ok()
    {
        state.phase = Phase::Failed;
        state.message = Some(format!("the updater stopped; see {path}"));
    }
    (
        service,
        if service {
            None
        } else {
            Some("no_service".into())
        },
        state,
        Some(path),
    )
}

pub(super) async fn get(latest: bool) -> Result<Reply, Refusal> {
    #[cfg(unix)]
    let (can_update, reason, state, log_path) = ntex::rt::spawn_blocking(inspect)
        .await
        .map_err(|_| INTERNAL)?;
    #[cfg(not(unix))]
    let (can_update, reason, state, log_path) = (false, Some("not_installed".into()), None, None);
    #[cfg(unix)]
    let lookup = if latest {
        Some(
            crate::system::update::lookup(None)
                .await
                .map(|(_, _, _, version, _)| version)
                .map_err(|e| e.to_string()),
        )
    } else {
        None
    };
    #[cfg(not(unix))]
    let lookup: Option<Result<String, String>> = None;
    #[cfg(not(unix))]
    let _ = latest;
    let (latest, latest_error) = match lookup {
        Some(Ok(version)) => (Some(version), None),
        Some(Err(error)) => (None, Some(error)),
        None => (None, None),
    };
    #[cfg(unix)]
    let available = latest
        .as_deref()
        .and_then(crate::system::update::Version::parse)
        .zip(crate::system::update::Version::parse(env!(
            "CARGO_PKG_VERSION"
        )))
        .is_some_and(|(latest, running)| latest > running);
    #[cfg(not(unix))]
    let available = false;
    Ok(Reply::ok(envelope(serde_json::json!(UpdateView {
        running: env!("CARGO_PKG_VERSION").into(),
        latest,
        available,
        latest_error,
        can_update,
        reason,
        state,
        log_path
    }))))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    version: String,
}
const CONFLICT: Refusal = Refusal::new(
    StatusCode::CONFLICT,
    "update_unavailable",
    "an update cannot be started",
);

pub(super) async fn post(body: &[u8]) -> Result<Reply, Refusal> {
    let input: Input = serde_json::from_slice(body).map_err(|_| INVALID_REQUEST)?;
    #[cfg(not(unix))]
    {
        let _ = input;
        Err(CONFLICT)
    }
    #[cfg(unix)]
    {
        use crate::system::update::{self, Phase, State, Version};
        let version = Version::parse(&input.version).ok_or(INVALID_REQUEST)?;
        if Some(version) == Version::parse(env!("CARGO_PKG_VERSION")) {
            return Err(CONFLICT);
        }
        let state = State {
            to: input.version,
            phase: Phase::Waiting,
            running_jobs: None,
            message: None,
            started_at: update::now_ms(),
        };
        let result_state = state.clone();
        let child = ntex::rt::spawn_blocking(move || launch(state))
            .await
            .map_err(|_| INTERNAL)??;
        ntex::rt::spawn(async move {
            let _ = ntex::rt::spawn_blocking(move || {
                let mut child = child;
                child.wait()
            })
            .await;
        });
        Ok(Reply::new(
            StatusCode::ACCEPTED,
            envelope(serde_json::json!(result_state)),
        ))
    }
}

#[cfg(unix)]
fn launch(state: crate::system::update::State) -> Result<std::process::Child, Refusal> {
    use crate::system::update::{self, Phase};
    use std::os::unix::fs::OpenOptionsExt;
    use std::process::{Command, Stdio};
    let install = update::install().map_err(|_| CONFLICT)?;
    let home = std::env::var_os("HOME").ok_or(CONFLICT)?;
    if !update::service_file(std::path::Path::new(&home)).is_file() {
        return Err(CONFLICT);
    }
    let lock = update::lock(&install.prefix).map_err(|_| CONFLICT)?;
    if update::read_state(&install.prefix).is_some_and(|old| {
        matches!(old.phase, Phase::Waiting | Phase::Installing)
            && update::now_ms().saturating_sub(old.started_at) < 15_000
    }) {
        return Err(CONFLICT);
    }
    let log = install.prefix.join("update.log");
    let output = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(&log)
        .map_err(|_| INTERNAL)?;
    let error = output.try_clone().map_err(|_| INTERNAL)?;
    update::write_state(&install.prefix, &state).map_err(|_| INTERNAL)?;
    let exe = install.prefix.join("current/ocg");
    #[cfg(target_os = "linux")]
    let mut command = {
        let mut command = Command::new("systemd-run");
        command
            .args(["--user", "--collect", "--quiet", "--unit", "ocg-update"])
            .arg("-p")
            .arg(format!("StandardOutput=append:{}", log.display()))
            .arg("-p")
            .arg(format!("StandardError=append:{}", log.display()));
        // Mirror checks pass the host's release URL through to the transient unit.
        if let Some(url) = std::env::var_os("OCG_RELEASE_URL") {
            command
                .arg("--setenv")
                .arg(std::path::PathBuf::from(format!(
                    "OCG_RELEASE_URL={}",
                    url.to_string_lossy()
                )));
        }
        command.arg(exe);
        command
    };
    #[cfg(not(target_os = "linux"))]
    let mut command = {
        use std::os::unix::process::CommandExt;
        let mut command = Command::new(exe);
        // Mirror checks inherit OCG_RELEASE_URL from the host environment.
        unsafe {
            command.pre_exec(|| {
                rustix::process::setsid()
                    .map(|_| ())
                    .map_err(std::io::Error::from)
            });
        }
        command
    };
    let child = command
        .args(["update", "--version", &state.to])
        .stdin(Stdio::null())
        .stdout(Stdio::from(output))
        .stderr(Stdio::from(error))
        .spawn();
    drop(lock);
    child.map_err(|_| INTERNAL)
}
