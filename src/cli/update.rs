use std::cmp::Ordering;
use std::error::Error;
use std::fs::{self, DirBuilder};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::DirBuilderExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;
use std::time::Instant;

use ntex::client::{Client, Connector};
use ntex::http::header;
use ntex::time::Millis;

use crate::system::http;
use crate::system::update::{self, Install, Phase, State, Version, fetch, install};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
async fn write_progress(prefix: &Path, state: &State) -> Result<()> {
    let prefix = prefix.to_path_buf();
    let state = state.clone();
    ntex::rt::spawn_blocking(move || update::write_state(&prefix, &state))
        .await
        .map_err(|_| "progress writer stopped")??;
    Ok(())
}

async fn wait_jobs(install: &Install, state: &mut State) -> Result<()> {
    let client = Client::builder()
        .connector::<()>(Connector::default())
        .response_timeout(Millis(2_000))
        .response_payload_timeout(Millis(2_000))
        .build(ntex::SharedCfg::new("update-health"))
        .await;
    let Ok(client) = client else { return Ok(()) };
    let Ok(auth) = http::authorization(&install.token) else {
        return Ok(());
    };
    let url = format!("http://127.0.0.1:{}/api/v1/health", install.port);
    loop {
        let Ok(response) = client
            .get(url.as_str())
            .header(header::AUTHORIZATION, auth.clone())
            .send()
            .await
        else {
            break;
        };
        if !response.status().is_success() {
            break;
        }
        let Ok(body) = response.body().await else {
            break;
        };
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(&body) else {
            break;
        };
        let Some(running) = value
            .pointer("/queue/running")
            .and_then(serde_json::Value::as_u64)
        else {
            break;
        };
        if running == 0 {
            break;
        }
        state.running_jobs = Some(running);
        write_progress(&install.prefix, state).await?;
        println!("waiting for {running} running Jobs (use --yes to update now)");
        ntex::time::sleep(Duration::from_secs(5)).await;
    }
    Ok(())
}

struct Download {
    version: String,
    installer: Vec<u8>,
}
async fn prepare(
    install: &Install,
    requested: Option<&str>,
    check: bool,
    yes: bool,
    state: &mut Option<State>,
) -> Result<Option<Download>> {
    let (client, base, local, version, checksum) = update::lookup(requested).await?;
    let current = env!("CARGO_PKG_VERSION");
    let order = Version::parse(&version)
        .ok_or("invalid release version")?
        .cmp(&Version::parse(current).ok_or("invalid running version")?);
    if check {
        println!("running {current}, latest {version}");
        return Ok(None);
    }
    if order == Ordering::Equal || (requested.is_none() && order != Ordering::Greater) {
        println!("up to date");
        return Ok(None);
    }
    if order == Ordering::Less {
        println!("downgrading from {current} to {version}");
    }
    if state.is_none() {
        *state = Some(State {
            to: version.clone(),
            phase: Phase::Waiting,
            running_jobs: None,
            message: None,
            started_at: update::now_ms(),
        });
    }
    if let Some(progress) = state.as_mut() {
        write_progress(&install.prefix, progress).await?;
        if !yes {
            wait_jobs(install, progress).await?;
        }
    }
    let installer = fetch(&client, &format!("{base}/install.sh"), local).await?;
    let actual = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &installer)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    if actual != checksum {
        return Err("installer checksum mismatch".into());
    }
    Ok(Some(Download { version, installer }))
}

pub(super) fn run(args: Vec<String>) -> Result<()> {
    let mut check = false;
    let mut yes = false;
    let mut version = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--check" if !check => check = true,
            "--yes" if !yes => yes = true,
            "--version" if version.is_none() => {
                let value = args.next().ok_or("--version requires a version")?;
                Version::parse(&value).ok_or("invalid version")?;
                version = Some(value);
            }
            _ => return Err("usage: ocg update [--check] [--version X.Y.Z] [--yes]".into()),
        }
    }
    let install = install()?;
    let start = Instant::now();
    let _lock = loop {
        match update::lock(&install.prefix) {
            Ok(lock) => break lock,
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    && start.elapsed() < Duration::from_secs(2) =>
            {
                std::thread::sleep(Duration::from_millis(100))
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Err("an update is already running".into());
            }
            Err(error) => return Err(error.into()),
        }
    };
    let mut state = if !check
        && version
            .as_deref()
            .is_some_and(|v| v != env!("CARGO_PKG_VERSION"))
    {
        let to = version.as_deref().unwrap_or_default();
        Some(
            update::read_state(&install.prefix)
                .filter(|state| {
                    state.to == to
                        && matches!(state.phase, Phase::Waiting)
                        && update::now_ms().saturating_sub(state.started_at) < 15_000
                })
                .unwrap_or_else(|| State {
                    to: to.to_owned(),
                    phase: Phase::Waiting,
                    running_jobs: None,
                    message: None,
                    started_at: update::now_ms(),
                }),
        )
    } else {
        None
    };
    if let Some(progress) = &state {
        update::write_state(&install.prefix, progress)?;
    }
    let error_target = version.clone();
    let (prepared, returned_state) = ntex::rt::System::build()
        .name("ocg-update")
        .signals(false)
        .build(ntex::rt::DefaultRuntime)
        .block_on({
            let install = install.clone();
            async move {
                let mut state = state;
                let result = prepare(&install, version.as_deref(), check, yes, &mut state).await;
                (result, state)
            }
        });
    state = returned_state;
    if !check && prepared.is_err() && state.is_none() {
        state = Some(State {
            to: error_target.unwrap_or_default(),
            phase: Phase::Waiting,
            running_jobs: None,
            message: None,
            started_at: update::now_ms(),
        });
    }
    let result = (|| -> Result<Option<i32>> {
        let Some(download) = prepared? else {
            return Ok(None);
        };
        if let Some(progress) = state.as_mut() {
            progress.phase = Phase::Installing;
            progress.running_jobs = None;
            update::write_state(&install.prefix, progress)?;
        }
        let dir = std::env::temp_dir().join(format!("ocg-update-{}", uuid::Uuid::now_v7()));
        DirBuilder::new().mode(0o700).create(&dir)?;
        let result = run_installer(&dir, &install.prefix, &download);
        let cleanup = fs::remove_dir_all(&dir);
        let (status, message) = result?;
        cleanup?;
        if !status.success() {
            if let Some(progress) = state.as_mut() {
                progress.message = message;
            }
            return Ok(Some(status.code().unwrap_or(1)));
        }
        Ok(Some(0))
    })();
    if let Some(progress) = state.as_mut() {
        match &result {
            Ok(Some(0)) => progress.phase = Phase::Succeeded,
            Ok(Some(_)) => {
                progress.phase = Phase::Failed;
                if progress.message.is_none() {
                    progress.message = Some("installer failed".to_owned());
                }
            }
            Ok(None) => {}
            Err(error) => {
                progress.phase = Phase::Failed;
                progress.message = Some(error.to_string());
            }
        }
        update::write_state(&install.prefix, progress)?;
    }
    match result? {
        Some(code) if code != 0 => std::process::exit(code),
        Some(_) | None => Ok(()),
    }
}

fn run_installer(
    dir: &Path,
    prefix: &Path,
    download: &Download,
) -> Result<(std::process::ExitStatus, Option<String>)> {
    let file = dir.join("install.sh");
    fs::write(&file, &download.installer)?;
    let home = std::env::var_os("HOME").ok_or("HOME is not set")?;
    let home = Path::new(&home);
    let service = update::service_file(home);
    let mut command = Command::new("/bin/sh");
    command
        .arg(file)
        .arg("--version")
        .arg(&download.version)
        .arg("--prefix")
        .arg(prefix);
    if !service.is_file() {
        command.arg("--no-service");
    }
    command.stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let stderr = child.stderr.take().ok_or("installer stderr unavailable")?;
    let mut message = None;
    let mut fallback = None;
    for line in BufReader::new(stderr).lines() {
        let line = line?;
        writeln!(std::io::stderr().lock(), "{line}")?;
        if line.starts_with("Update to ") || line.starts_with("OCG did not") {
            message = Some(line);
        } else if line.starts_with("OCG failed:") || (fallback.is_none() && !line.trim().is_empty())
        {
            fallback = Some(line);
        }
    }
    Ok((child.wait()?, message.or(fallback)))
}
